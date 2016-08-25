package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper.OniLDACInput
import org.opennetworkinsight.dns.DNSSchema._
import org.slf4j.Logger

import scala.io.Source

/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming DNS records.
  */
object DNSPreLDA {

  def dnsPreLDA(inputPath: String, scoresFile: String, duplicationFactor: Int,
                sc: SparkContext, sqlContext: SQLContext, logger: Logger): RDD[OniLDACInput]
  = {

    logger.info("DNS pre LDA starts")

    import sqlContext.implicits._
    val feedbackFile = scoresFile
    val scoredFileExists = new java.io.File(feedbackFile).exists

    val falsePositives: DataFrame = if (scoredFileExists) {

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
      feedback.map(_.split(","))
        .filter(row => row(DnsSevIndex).trim.toInt == 3)
        .map(row => Feedback(row(FrameTimeIndex),
        row(UnixTimeStampIndex),
        row(FrameLenIndex).trim.toInt,
        row(IpDstIndex),
        row(DnsQryNameIndex),
        row(DnsQryClassIndex),
        row(DnsQryTypeIndex),
        row(DnsQryRcodeIndex),
        row(DnsSevIndex).trim.toInt))
        .flatMap(row => List.fill(duplicationFactor)(row))
        .toDF()
        .select("frameTime", "unixTimeStamp", "frameLen", "ipDst",
          "dnsQryName", "dnsQryClass", "dnsQryType", "dnsQryRcode")
    } else {
      null
    }

    val rawData = {
      sqlContext.read.parquet(inputPath.split(",")(0))
        .filter(Timestamp + " is not null and " + UnixTimestamp + " is not null")
        .select(Timestamp,
          UnixTimestamp,
          FrameLength,
          ClientIP,
          QueryName,
          QueryClass,
          QueryType,
          QueryResponseCode)
    }

    print("Read source data")
    val totalDataDF = {
      if (!scoredFileExists) {
        rawData
      } else {
        rawData.unionAll(falsePositives)
      }
    }

    val dataWithWordDF = DNSWordCreation.dnsWordCreation(totalDataDF, sc, logger, sqlContext)

    val ipDstWordCounts = dataWithWordDF
      .select(ClientIP, Word)
      .map({
        case Row(destIP: String, word: String) =>
          (destIP, word) -> 1
      })
      .reduceByKey(_ + _)

    ipDstWordCounts.map({case ((ipDst, word), count) => OniLDACInput(ipDst, word, count)})
  }

  case class Feedback(frameTime: String,
                      unixTimeStamp: String,
                      frameLen: Int,
                      ipDst: String,
                      dnsQryName: String,
                      dnsQryClass: String,
                      dnsQryType: String,
                      dnsQryRcode: String,
                      dnsSev: Int) extends Serializable

}

