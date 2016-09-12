package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper.OniLDACOutput
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.opennetworkinsight.OniLDACWrapper
import org.opennetworkinsight.OniLDACWrapper.OniLDACOutput
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.opennetworkinsight.dns.DNSSchema._
import org.opennetworkinsight.utilities.CountryCodes
import org.slf4j.Logger

/**
  * Run suspicious connections analysis on DNS log data.
  */

object DNSSuspiciousConnectsAnalysis {

  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("Starting DNS suspicious connects analysis.")

    logger.info("Loading data")


    val topicCount = 20

    val rawDataDF = sqlContext.read.parquet(config.inputPath)
      .filter(Timestamp + " is not null and " + UnixTimestamp + " is not null")
      .select(Timestamp,
        UnixTimestamp,
        FrameLength,
        ClientIP,
        QueryName,
        QueryClass,
        QueryType,
        QueryResponseCode)

    // add the derived fields

    logger.info("Computing subdomain info")
    val countryCodesBC = sparkContext.broadcast(CountryCodes.CountryCodes)

    val queryNameIndex = rawDataDF.schema.fieldNames.indexOf(QueryName)

    val dataWithSubdomainsRDD: RDD[Row] = rawDataDF.rdd.map(row =>
      Row.fromSeq {row.toSeq ++ extractSubdomain(countryCodesBC, row.getString(queryNameIndex))})

    // Update data frame schema with newly added columns. This happens b/c we are adding more than one column at once.
    val schemaWithSubdomain = StructType(rawDataDF.schema.fields ++
      refArrayOps(Array(StructField(Domain, StringType),
          StructField(Subdomain, StringType),
          StructField(SubdomainLength, DoubleType),
          StructField(NumPeriods, DoubleType))))


    val dataWithSubDomainsDF = sqlContext.createDataFrame(dataWithSubdomainsRDD, schemaWithSubdomain)

    val docWordCount = DNSPreLDA.dnsPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext, sqlContext, logger, dataWithSubDomainsDF)

    val OniLDACOutput(documentResults, wordResults) = OniLDACWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser, config.analysis, config.nodes)

    DNSPostLDA.dnsPostLDA(config.inputPath, config.hdfsScoredConnect, config.outputDelimiter, config.threshold, config.maxResults, topicCount, documentResults,
      wordResults, sparkContext, sqlContext, logger)
    logger.info("DNS  suspcicious connects analysis completed.")
  }


  def extractSubdomain(countryCodesBC: Broadcast[Set[String]], url: String): Array[Any] = {

    val splitURL = url.split("[.]")
    val numParts = splitURL.length
    var domain = "None"
    var subdomain = "None"

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

    Array(domain, subdomain, {
      if (subdomain != "None") {
        subdomain.length.toDouble
      } else {
        0.0
      }
    }, numParts.toDouble)
  }
}



