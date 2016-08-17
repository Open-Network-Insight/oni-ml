package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.opennetworkinsight.utilities.Quantiles
import org.slf4j.Logger

import scala.io.Source

/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming DNS records.
  */
object DNSPreLDA {

  case class Feedback(frameTime: String,
                      unixTimeStamp: String,
                      frameLen: Int,
                      ipDst: String,
                      dnsQryName: String,
                      dnsQryClass: String,
                      dnsQryType: String,
                      dnsQryRcode: String,
                      dnsSev: Int) extends Serializable

  def dnsPreLDA(inputPath: String, scoresFile: String, duplicationFactor: Int,
                sc: SparkContext, sqlContext: SQLContext, logger: Logger): RDD[String]
  = {

    logger.info("DNS pre LDA starts")

    import sqlContext.implicits._

    val feedbackFile = scoresFile
    var timeCuts = new Array[Double](10)
    var frameLengthCuts = new Array[Double](10)
    var subdomainLengthCuts = new Array[Double](5)
    var numberPeriodsCuts = new Array[Double](5)
    var entropyCuts = new Array[Double](5)
    var rawDataDFColumns = new Array[String](0)

    val topDomains: Broadcast[Set[String]] = sc.broadcast(Source.fromFile("top-1m.csv").getLines
      .map(line => {
        val parts = line.split(",")
        parts(1).split("[.]")(0)
      }).toSet)

    val scoredFileExists = new java.io.File(feedbackFile).exists

    val scoredData = if (scoredFileExists) {

      /* dns_scores.csv - feedback file structure

      0   frame_time             object
      1   frame_len              object
      2   ip_dst                 object
      3   dns_qry_name           object
      4   dns_qry_class           int64
      5   dns_qry_type            int64
      6   dns_qry_rcode           int64
      7   domain                 object
      8   subdomain              object
      9   subdomain_length        int64
      10  num_periods             int64
      11  subdomain_entropy     float64
      12  top_domain              int64
      13  word                   object
      14  score                 float64
      15  query_rep              object
      16  hh                      int64
      17  ip_sev                  int64
      18  dns_sev                 int64
      19  dns_qry_class_name     object
      20  dns_qry_type_name      object
      21  dns_qry_rcode_name     object
      22  network_context       float64
      23  unix_tstamp
      */
      val FrameTimeIndex = 0
      val UnixTimeStampIndex = 23
      val FrameLenIndex = 1
      val IpDstIndex = 2
      val DnsQryNameIndex = 3
      val DnsQryClassIndex = 4
      val DnsQryTypeIndex = 5
      val DnsQryRcodeIndex = 6
      val DnsSevIndex = 18

      /**
        * Calling drop(1) to remove file header.
        */
      val lines = Source.fromFile(feedbackFile).getLines().toArray.drop(1)
      val feedback: RDD[String] = sc.parallelize(lines)
      val feedbackDataFrame = feedback.map(_.split(",")).map(row => Feedback(row(FrameTimeIndex),
        row(UnixTimeStampIndex),
        row(FrameLenIndex).trim.toInt,
        row(IpDstIndex),
        row(DnsQryNameIndex),
        row(DnsQryClassIndex),
        row(DnsQryTypeIndex),
        row(DnsQryRcodeIndex),
        row(DnsSevIndex).trim.toInt)).toDF()
      val result = feedbackDataFrame.filter("dnsSev = 3").select("frameTime", "unixTimeStamp", "frameLen", "ipDst",
        "dnsQryName", "dnsQryClass", "dnsQryType", "dnsQryRcode")
      val toDuplicate = result.rdd.flatMap({ case Row(row: String) => List.fill(duplicationFactor)(row) })
      toDuplicate
    } else {
      null
    }


    val rawDataRDD = {
      val df = sqlContext.parquetFile(inputPath.split(",")(0))
        .filter("frame_len is not null and unix_tstamp is not null")
        .select("frame_time",
          "unix_tstamp",
          "frame_len",
          "ip_dst",
          "dns_qry_name",
          "dns_qry_class",
          "dns_qry_type",
          "dns_qry_rcode")
      // Need to extract raw data columns to reference index in future lines. rawDataDFColumns will be zipped with index.
      rawDataDFColumns = df.columns
      df.map(_.mkString(","))
    }

    print("Read source data")
    val totalDataDF = {
      if (!scoredFileExists) {
        rawDataRDD.toDF
      } else {
        rawDataRDD.union(scoredData).toDF
      }
    }

    val rawDataColumnWithIndex = DNSWordCreation.getColumnNames(rawDataDFColumns)

    val countryCodes = sc.broadcast(DNSWordCreation.countryCodes)

    logger.info("Computing subdomain info")

    val dataWithSubdomainsRDD: RDD[Row] = totalDataDF.rdd.map(row =>
      Row.fromSeq {
        row.toSeq ++
          DNSWordCreation.extractSubdomain(countryCodes, row.getAs[String](rawDataColumnWithIndex("dns_qry_name")))
      })

    // Update data frame schema with newly added columns. This happens b/c we are adding more than one column at once.
    val schemaWithSubdomain = {
      StructType(totalDataDF.schema.fields ++
        refArrayOps(Array(StructField("domain", StringType),
          StructField("subdomain", StringType),
          StructField("subdomain.length", DoubleType),
          StructField("num.periods", DoubleType))))
    }

    val dataWithSubDomainsDF = sqlContext.createDataFrame(dataWithSubdomainsRDD, schemaWithSubdomain)

    logger.info("Computing subdomain entropy")

    val udfStringEntropy = DNSWordCreation.udfStringEntropy()

    val dataWithSubdomainEntropyDF = dataWithSubDomainsDF.withColumn("subdomain.entropy",
      udfStringEntropy(dataWithSubDomainsDF("subdomain")))

    logger.info("Calculating time cuts ...")

    timeCuts = Quantiles.computeDeciles(dataWithSubdomainEntropyDF
      .select("unix_tstamp")
      .rdd
      .map({ case Row(unixTimeStamp: Int) => unixTimeStamp.toDouble }))

    logger.info(timeCuts.mkString(","))

    logger.info("Calculating frame length cuts ...")

    frameLengthCuts = Quantiles.computeDeciles(dataWithSubdomainEntropyDF
      .select("frame_len")
      .rdd
      .map({ case Row(frameLen: Int) => frameLen.toDouble }))

    logger.info(frameLengthCuts.mkString(","))

    logger.info("Calculating subdomain length cuts ...")

    subdomainLengthCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
      .filter("subdomain.length > 0")
      .select("subdomain.length")
      .rdd
      .map({ case Row(subdomainLength: Double) => subdomainLength }))

    logger.info(subdomainLengthCuts.mkString(","))

    logger.info("Calculating entropy cuts")

    entropyCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
      .filter("subdomain.entropy > 0")
      .select("subdomain.entropy")
      .rdd
      .map({ case Row(subdomainEntropy: Double) => subdomainEntropy }))

    logger.info(entropyCuts.mkString(","))

    logger.info("Calculating num periods cuts ...")

    numberPeriodsCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
      .filter("num.periods > 0")
      .select("num.periods")
      .rdd
      .map({ case Row(numberPeriods: Double) => numberPeriods }))

    logger.info(numberPeriodsCuts.mkString(","))

    val udfGetTopDomain = DNSWordCreation.udfGetTopDomain(topDomains)

    val dataWithTopDomainDF = dataWithSubdomainEntropyDF.withColumn("top_domain", udfGetTopDomain(dataWithSubdomainEntropyDF("domain")))

    logger.info("Adding words")

    val udfWordCreation = DNSWordCreation.udfWordCreation(frameLengthCuts, timeCuts, subdomainLengthCuts, entropyCuts, numberPeriodsCuts)

    val dataWithWordDF = dataWithTopDomainDF.withColumn("word", udfWordCreation(
      dataWithTopDomainDF("top_domain"),
      dataWithTopDomainDF("frame_len"),
      dataWithTopDomainDF("unix_tstamp"),
      dataWithTopDomainDF("subdomain.length"),
      dataWithTopDomainDF("subdomain.entropy"),
      dataWithTopDomainDF("num.periods"),
      dataWithTopDomainDF("dns_qry_type"),
      dataWithTopDomainDF("dns_qry_rcode"))).select("ip_dst, word")

    val docWordCount = dataWithWordDF
      .map({
        case Row(destIP: String, word: String) =>
          (destIP, word) -> 1
      })
      .reduceByKey(_ + _)
      .map({
        case ((destIp, word), count) =>
          Seq(destIp, word, count).mkString(",")
      })

    docWordCount
  }

}

