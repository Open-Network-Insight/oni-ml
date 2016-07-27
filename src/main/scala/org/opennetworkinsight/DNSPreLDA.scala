package org.opennetworkinsight

import org.slf4j.Logger

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import org.opennetworkinsight.SuspiciousConnectsArgumentParser.Config

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
    var time_cuts = new Array[Double](10)
    var frame_length_cuts = new Array[Double](10)
    var subdomain_length_cuts = new Array[Double](5)
    var numperiods_cuts = new Array[Double](5)
    var entropy_cuts = new Array[Double](5)
    var df_cols = new Array[String](0)

    val l_top_domains = Source.fromFile("top-1m.csv").getLines.map(line => {
      val parts = line.split(",")
      val l = parts.length
      parts(1).split("[.]")(0)
    }).toSet
    val top_domains = sc.broadcast(l_top_domains)


    val scoredFileExists = new java.io.File(feedbackFile).exists

    val falsePositives: org.apache.spark.rdd.RDD[String] = if (scoredFileExists) {

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
        "dnsQryName", "dnsQryClass", "dnsQryType", "dnsQryRcode").map(_.mkString(","))
      val toDuplicate: org.apache.spark.rdd.RDD[String] = result.flatMap(x => List.fill(duplicationFactor)(x))
      toDuplicate
    } else {
      null
    }


    val multidata = {
      var df = sqlContext.parquetFile(inputPath.split(",")(0)).filter("frame_len is not null and unix_tstamp is not null")
      val files = inputPath.split(",")
      for ((file, index) <- files.zipWithIndex) {
        if (index > 1) {
          df = df.unionAll(sqlContext.parquetFile(file).filter("frame_len is not null and unix_tstamp is not null"))
        }
      }
      df = df.select("frame_time", "unix_tstamp", "frame_len", "ip_dst", "dns_qry_name",
        "dns_qry_class", "dns_qry_type", "dns_qry_rcode")
      df_cols = df.columns
      val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
      tempRDD
    }

    print("Read source data")
    val rawdata: org.apache.spark.rdd.RDD[String] = {
      if (!scoredFileExists) {
        multidata
      } else {
        multidata.union(falsePositives)
      }
    }

    val col = DNSWordCreation.getColumnNames(df_cols)

    def addcol(colname: String) = if (!col.keySet.exists(_ == colname)) {
      col(colname) = col.values.max + 1
    }
    if (feedbackFile != "None") {
      addcol("feedback")
    }

    val datagood = rawdata.map(line => line.split(",")).filter(line => (line.length == df_cols.length)).map(line => {
      if (feedbackFile != "None") {
        line :+ "None"
      } else {
        line
      }
    })

    val country_codes = sc.broadcast(DNSWordCreation.l_country_codes)

    logger.info("Computing subdomain info")

    var data_with_subdomains = datagood.map(row => row ++ DNSWordCreation.extractSubdomain(country_codes, row(col("dns_qry_name"))))
    addcol("domain")
    addcol("subdomain")
    addcol("subdomain.length")
    addcol("num.periods")

    data_with_subdomains = data_with_subdomains.map(data => data :+ DNSWordCreation.entropy(data(col("subdomain"))).toString)
    addcol("subdomain.entropy")

    logger.info("Calculating time cuts ...")
    time_cuts = Quantiles.computeDeciles(data_with_subdomains.map(r => r(col("unix_tstamp")).toDouble))
    logger.info(time_cuts.mkString(","))

    logger.info("Calculating frame length cuts ...")
    frame_length_cuts = Quantiles.computeDeciles(data_with_subdomains.map(r => r(col("frame_len")).toDouble))
    logger.info(frame_length_cuts.mkString(","))
    logger.info("Calculating subdomain length cuts ...")
    subdomain_length_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col("subdomain.length")).toDouble > 0).map(r => r(col("subdomain.length")).toDouble))
    logger.info(subdomain_length_cuts.mkString(","))
    logger.info("Calculating entropy cuts")
    entropy_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col("subdomain.entropy")).toDouble > 0).map(r => r(col("subdomain.entropy")).toDouble))
    logger.info(entropy_cuts.mkString(","))
    logger.info("Calculating num periods cuts ...")
    numperiods_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col("num.periods")).toDouble > 0).map(r => r(col("num.periods")).toDouble))
    logger.info(numperiods_cuts.mkString(","))

    var data = data_with_subdomains.map(line => line :+ {
      if (line(col("domain")) == "intel") {
        "2"
      } else if (top_domains.value contains line(col("domain"))) {
        "1"
      } else "0"
    })
    addcol("top_domain")

    logger.info("Adding words")
    data = data.map(row => {
      val word = row(col("top_domain")) + "_" + DNSWordCreation.binColumn(row(col("frame_len")), frame_length_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("unix_tstamp")), time_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("subdomain.length")), subdomain_length_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("subdomain.entropy")), entropy_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("num.periods")), numperiods_cuts) + "_" + row(col("dns_qry_type")) + "_" + row(col("dns_qry_rcode"))
      row :+ word
    })
    addcol("word")

    val wc = data.map(row => (row(col("ip_dst")) + " " + row(col("word")), 1)).reduceByKey(_ + _).map(row => (row._1.split(" ")(0) + "," + row._1.split(" ")(1).toString + "," + row._2).mkString)
    logger.info("DNS pre LDA completed")

    wc
  }

}

