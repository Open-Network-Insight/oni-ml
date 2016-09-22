package org.opennetworkinsight.dns

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.{DNSSchema => Schema}
import org.opennetworkinsight.utilities.{Entropy, Quantiles}

import scala.io.Source

  object DNSWordCreation {

    val countryCodes = Set("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au",
      "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bm", "bn", "bo", "bq", "br", "bs", "bt",
      "bv", "bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr", "cu", "cv",
      "cw", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "eu", "fi",
      "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr",
      "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "io", "iq", "ir",
      "is", "it", "je", "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "krd", "kw", "ky", "kz", "la",
      "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mk", "ml", "mm",
      "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni",
      "nl", "no", "np", "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "ps", "pt",
      "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "", "sk",
      "sl", "sm", "sn", "so", "sr", "ss", "st", "su", "sv", "sx", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk",
      "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve",
      "vg", "vi", "vn", "vu", "wf", "ws", "ye", "yt", "za", "zm", "zw")

    def dnsWordCreation(totalDataDF: DataFrame,
                        sc: SparkContext, logger: Logger, sqlContext: SQLContext): DataFrame ={

      var timeCuts = new Array[Double](10)
      var frameLengthCuts = new Array[Double](10)
      var subdomainLengthCuts = new Array[Double](5)
      var numberPeriodsCuts = new Array[Double](5)
      var entropyCuts = new Array[Double](5)

      val topDomains = sc.broadcast(Source.fromFile("top-1m.csv").getLines.map(line => {
        val parts = line.split(",")
        parts(1).split("[.]")(0)
      }).toSet)

      val countryCodesBC = sc.broadcast(countryCodes)

      logger.info("Computing subdomain info")

      val queryNameIndex = totalDataDF.schema.fieldNames.indexOf(Schema.QueryName)

      val dataWithSubdomainsRDD: RDD[Row] = totalDataDF.rdd.map(row =>
        Row.fromSeq {
          row.toSeq ++
            extractSubdomain(countryCodesBC, row.getString(queryNameIndex))
        })

      // Update data frame schema with newly added columns. This happens b/c we are adding more than one column at once.
      val schemaWithSubdomain = {
        StructType(totalDataDF.schema.fields ++
          refArrayOps(Array(StructField(Schema.Domain, StringType),
            StructField(Schema.Subdomain, StringType),
            StructField(Schema.SubdomainLength, DoubleType),
            StructField(Schema.NumPeriods, DoubleType))))
      }

      val dataWithSubDomainsDF = sqlContext.createDataFrame(dataWithSubdomainsRDD, schemaWithSubdomain)

      logger.info("Computing subdomain entropy")

      val udfStringEntropy = DNSWordCreation.udfStringEntropy()

      val dataWithSubdomainEntropyDF = dataWithSubDomainsDF.withColumn(Schema.SubdomainEntropy,
        udfStringEntropy(col(Schema.Subdomain)))

      logger.info("Calculating time cuts ...")

      timeCuts = Quantiles.computeDeciles(dataWithSubdomainEntropyDF
        .select(Schema.UnixTimestamp)
        .rdd
        .map({ case Row(unixTimeStamp: Long) => unixTimeStamp.toDouble }))

      logger.info(timeCuts.mkString(","))

      logger.info("Calculating frame length cuts ...")

      frameLengthCuts = Quantiles.computeDeciles(dataWithSubdomainEntropyDF
        .select(Schema.FrameLength)
        .rdd
        .map({ case Row(frameLen: Int) => frameLen.toDouble }))

      logger.info(frameLengthCuts.mkString(","))

      logger.info("Calculating subdomain length cuts ...")

      subdomainLengthCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
        .filter(Schema.SubdomainLength +  " > 0")
        .select(Schema.SubdomainLength)
        .rdd
        .map({ case Row(subdomainLength: Double) => subdomainLength }))

      logger.info(subdomainLengthCuts.mkString(","))

      logger.info("Calculating entropy cuts")

      entropyCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
        .filter(Schema.SubdomainEntropy + " > 0")
        .select(Schema.SubdomainEntropy)
        .rdd
        .map({ case Row(subdomainEntropy: Double) => subdomainEntropy }))

      logger.info(entropyCuts.mkString(","))

      logger.info("Calculating num periods cuts ...")

      numberPeriodsCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
        .filter(Schema.NumPeriods + " > 0")
        .select(Schema.NumPeriods)
        .rdd
        .map({ case Row(numberPeriods: Double) => numberPeriods }))

      logger.info(numberPeriodsCuts.mkString(","))

      val udfGetTopDomain = DNSWordCreation.udfGetTopDomain(topDomains)

      val dataWithTopDomainDF = dataWithSubdomainEntropyDF.withColumn(Schema.TopDomain,
        udfGetTopDomain(dataWithSubdomainEntropyDF(Schema.Domain)))

      logger.info("Adding words")

      val udfWordCreation = DNSWordCreation.udfWordCreation(frameLengthCuts, timeCuts,
        subdomainLengthCuts, entropyCuts, numberPeriodsCuts)

      val dataWithWordDF = dataWithTopDomainDF.withColumn(Schema.Word, udfWordCreation(
        dataWithTopDomainDF(Schema.TopDomain),
        dataWithTopDomainDF(Schema.FrameLength),
        dataWithTopDomainDF(Schema.UnixTimestamp),
        dataWithTopDomainDF(Schema.SubdomainLength),
        dataWithTopDomainDF(Schema.SubdomainEntropy),
        dataWithTopDomainDF(Schema.NumPeriods),
        dataWithTopDomainDF(Schema.QueryType),
        dataWithTopDomainDF(Schema.QueryResponseCode)))

      dataWithWordDF
    }

    def udfGetTopDomain(topDomains: Broadcast[Set[String]]) = udf((domain: String) => getTopDomain(topDomains, domain))

    def getTopDomain(topDomains: Broadcast[Set[String]], domain: String) = {
      if (domain == "intel") {
        "2"
      } else if (topDomains.value contains domain) {
        "1"
      } else "0"
    }

    def extractSubdomain(country_codes: Broadcast[Set[String]], url: String): Array[Any] = {

      val spliturl = url.split("[.]")
      val numparts = spliturl.length
      var domain = "None"
      var subdomain = "None"

      //first check if query is an Ip address e.g.: 123.103.104.10.in-addr.arpa or a name
      val is_ip = {
        if (numparts > 2) {
          if (spliturl(numparts - 1) == "arpa" & spliturl(numparts - 2) == "in-addr") {
            "IP"
          } else "Name"
        } else "Unknown"
      }

      if (numparts > 2 && is_ip != "IP") {
        //This might try to parse things with only 1 or 2 numparts
        //test if last element is a country code or tld
        //use: Array(spliturl(numparts-1)).exists(country_codes contains _)
        // don't use: country_codes.exists(spliturl(numparts-1).contains) this doesn't test exact match, might just match substring
        if (Array(spliturl(numparts - 1)).exists(country_codes.value contains _)) {
          domain = spliturl(numparts - 3)
          if (1 <= numparts - 3) {
            subdomain = spliturl.slice(0, numparts - 3).mkString(".")
          }
        }
        else {
          domain = spliturl(numparts - 2)
          subdomain = spliturl.slice(0, numparts - 2).mkString(".")
        }
      }
      Array(domain, subdomain, {
        if (subdomain != "None") {
          subdomain.length.toDouble
        } else {
          0.0
        }
      }, numparts.toDouble)
    }

    def udfStringEntropy() = udf((subdomain: String) => Entropy.stringEntropy(subdomain))

    def udfWordCreation(frameLengthCuts: Array[Double],
                        timeCuts: Array[Double],
                        subdomainLengthCuts: Array[Double],
                        entropyCuts: Array[Double],
                        numberPeriodsCuts: Array[Double]) =
      udf((topDomain: String,
           frameLength: Int,
           unixTimeStamp: Long,
           subdomainLength: Double,
           subdomainEntropy: Double,
           numberPeriods: Double,
           dnsQueryType: Int,
           dnsQueryRcode: Int) => dnsWord(topDomain, frameLength, unixTimeStamp, subdomainLength, subdomainEntropy,
        numberPeriods, dnsQueryType, dnsQueryRcode, frameLengthCuts, timeCuts, subdomainLengthCuts, entropyCuts, numberPeriodsCuts))

    def dnsWord(topDomain: String,
                frameLength: Int,
                unixTimeStamp: Long,
                subdomainLength: Double,
                subdomainEntropy: Double,
                numberPeriods: Double,
                dnsQueryType: Int,
                dnsQueryRcode: Int,
                frameLengthCuts: Array[Double],
                timeCuts: Array[Double],
                subdomainLengthCuts: Array[Double],
                entropyCuts: Array[Double],
                numberPeriodsCuts: Array[Double]): String = {
      Seq(topDomain ,
        Quantiles.bin(frameLength.toDouble, frameLengthCuts) ,
        Quantiles.bin(unixTimeStamp.toDouble, timeCuts) ,
        Quantiles.bin(subdomainLength, subdomainLengthCuts) ,
        Quantiles.bin(subdomainEntropy, entropyCuts) ,
        Quantiles.bin(numberPeriods, numberPeriodsCuts) ,
        dnsQueryType ,
        dnsQueryRcode).mkString("_")
    }
  }


