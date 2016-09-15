package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.DNSSchema._
import org.opennetworkinsight.utilities.{CountryCodes, Entropy, Quantiles, TopDomains}
import org.slf4j.Logger

import scala.io.Source

  object DNSWordCreation {



    def addDerivedFields(sparkContext: SparkContext,
                         sqlContext: SQLContext,
                         countryCodesBC: Broadcast[Set[String]],
                         topDomainsBC: Broadcast[Set[String]],
                         inDF: DataFrame) : DataFrame = {


      val queryNameIndex = inDF.schema.fieldNames.indexOf(QueryName)

      val schemaWithAddedFields = StructType(inDF.schema.fields ++
        refArrayOps(Array(StructField(TopDomain, IntegerType),
          StructField(SubdomainLength, DoubleType),
          StructField(SubdomainEntropy, DoubleType),
          StructField(NumPeriods, DoubleType))))

      val dataWithSubdomainsRDD: RDD[Row] = inDF.rdd.map(row =>
        Row.fromSeq {row.toSeq ++
          derivedFieldsToArray(createDerivedFields(countryCodesBC, topDomainsBC, row.getString(queryNameIndex)))})

      // Update data frame schema with newly added columns. This happens b/c we are adding more than one column at once.

      sqlContext.createDataFrame(dataWithSubdomainsRDD, schemaWithAddedFields)
    }


    case class DerivedFields(topDomainClass: Int, subdomainLength: Double, subdomainEntropy: Double, numPeriods: Double)

    def derivedFieldsToArray(df: DerivedFields) =
      Array(df.topDomainClass, df.subdomainLength, df.subdomainEntropy, df.numPeriods)

    def createDerivedFields(countryCodesBC: Broadcast[Set[String]],
                            topDomains: Broadcast[Set[String]],
                            url: String) : DerivedFields = {

      val SubdomainInfo(domain, subdomain, subdomainLength, numPeriods) = extractSubomain(countryCodesBC, url)

      val topDomainClass = getTopDomain(topDomains: Broadcast[Set[String]], domain: String)
      val subdomainEntropy = if (subdomain != "None") Entropy.stringEntropy(subdomain) else 0d

      DerivedFields(topDomainClass= topDomainClass,
        subdomainLength= subdomainLength,
        subdomainEntropy= subdomainEntropy,
        numPeriods= numPeriods)
    }


    def getTopDomain(topDomains: Broadcast[Set[String]], domain: String) : Int = {
      if (domain == "intel") {
        2
      } else if (topDomains.value contains domain) {
        1
      } else {
        0
      }
    }

    def udfWordCreation(frameLengthCuts: Array[Double],
                        timeCuts: Array[Double],
                        subdomainLengthCuts: Array[Double],
                        entropyCuts: Array[Double],
                        numberPeriodsCuts: Array[Double],
                        countryCodesBC: Broadcast[Set[String]],
                        topDomainsBC: Broadcast[Set[String]]) =
      udf((timeStamp: String,
           unixTimeStamp: Long,
           frameLength: Int,
           clientIP: String,
           queryName: String,
           queryClass: String,
           dnsQueryType: Int,
           dnsQueryRcode: Int) => dnsWord(timeStamp,
        unixTimeStamp,
        frameLength,
        clientIP,
        queryName,
        queryClass,
        dnsQueryType,
        dnsQueryRcode,
        frameLengthCuts,
        timeCuts,
        subdomainLengthCuts,
        entropyCuts,
        numberPeriodsCuts,
        countryCodesBC,
        topDomainsBC))


    def dnsWord(timeStamp: String,
                 unixTimeStamp: Long,
                 frameLength: Int,
                 clientIP: String,
                 queryName: String,
                 queryClass: String,
                 dnsQueryType: Int,
                 dnsQueryRcode: Int,
                frameLengthCuts: Array[Double],
                timeCuts: Array[Double],
                subdomainLengthCuts: Array[Double],
                entropyCuts: Array[Double],
                numberPeriodsCuts: Array[Double],
               countryCodesBC: Broadcast[Set[String]],
                topDomainsBC: Broadcast[Set[String]]): String = {



      val DerivedFields(topDomain, subdomainLength, subdomainEntropy, numPeriods) =
        createDerivedFields(countryCodesBC, topDomainsBC, queryName)



      Seq(topDomain,
        Quantiles.bin(frameLength.toDouble, frameLengthCuts) ,
        Quantiles.bin(unixTimeStamp.toDouble, timeCuts) ,
        Quantiles.bin(subdomainLength, subdomainLengthCuts) ,
        Quantiles.bin(subdomainEntropy, entropyCuts) ,
        Quantiles.bin(numPeriods, numberPeriodsCuts) ,
        dnsQueryType ,
        dnsQueryRcode).mkString("_")
    }




    case class SubdomainInfo(domain: String, subdomain: String, subdomainLength: Double, numPeriods: Double)
    def extractSubomain(countryCodesBC: Broadcast[Set[String]], url: String): SubdomainInfo = {

      val None = "None"
      val splitURL = url.split("[.]")
      val numParts = splitURL.length
      var domain = None
      var subdomain = None

      //first check if query is an Ip address e.g.: 123.103.104.10.in-addr.arpa or a name
      val isIP = {
        if (numParts > 2) {
          if (splitURL(numParts - 1) == "arpa" & splitURL(numParts - 2) == "in-addr") {
            "IP"
          } else "Name"
        } else "Unknown"
      }

      if (numParts > 2 && isIP != "IP") {
        //This might try to parse things with only 1 or 2 numParts
        //test if last element is a country code or tld
        //use: Array(splitURL(numParts-1)).exists(country_codes contains _)
        // don't use: country_codes.exists(splitURL(numParts-1).contains) this doesn't test exact match, might just match substring
        if (Array(splitURL(numParts - 1)).exists(countryCodesBC.value contains _)) {
          domain = splitURL(numParts - 3)
          if (1 <= numParts - 3) {
            subdomain = splitURL.slice(0, numParts - 3).mkString(".")
          }
        }
        else {
          domain = splitURL(numParts - 2)
          subdomain = splitURL.slice(0, numParts - 2).mkString(".")
        }
      }


      val subdomainLength = if (subdomain != None) {
        subdomain.length.toDouble
      } else {
        0.0
      }
      SubdomainInfo(domain, subdomain, subdomainLength, numParts.toDouble)
    }



  }


