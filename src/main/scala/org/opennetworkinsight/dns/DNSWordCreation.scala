package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.{DNSSchema => Schema}
import org.opennetworkinsight.utilities.{CountryCodes, Entropy, Quantiles}
import org.slf4j.Logger

import scala.io.Source

  object DNSWordCreation {



    def dnsWordCreation(inDF: DataFrame,
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



      logger.info("Computing subdomain entropy")

      val udfStringEntropy = DNSWordCreation.udfStringEntropy()

      val dataWithSubdomainEntropyDF = inDF.withColumn(Schema.SubdomainEntropy,
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


