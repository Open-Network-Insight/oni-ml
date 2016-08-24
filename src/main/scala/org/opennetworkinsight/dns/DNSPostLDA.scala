
package org.opennetworkinsight.dns

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.{DNSSchema => Schema}
import org.slf4j.Logger

/**
  * Contains routines for scoring incoming netflow records from a DNS suspicious connections model.
  */
object DNSPostLDA {

  def dnsPostLDA(inputPath: String,
                 resultsFilePath: String,
                 outputDelimiter: String,
                 threshold: Double,
                 topK: Int,
                 ipToTopicMixes: Map[String, Array[Double]],
                 wordToProbPerTopic : Map[String, Array[Double]],
                 sc: SparkContext,
                 sqlContext: SQLContext,
                 logger: Logger) = {


    logger.info("DNS post LDA starts")

    var time_cuts = new Array[Double](10)
    var frame_length_cuts = new Array[Double](10)
    var subdomain_length_cuts = new Array[Double](5)
    var numperiods_cuts = new Array[Double](5)
    var entropy_cuts = new Array[Double](5)
    var df_cols = new Array[String](0)

    val l_top_domains = Source.fromFile("top-1m.csv").getLines.map(line => {
      val parts = line.split(",")
      parts(1).split("[.]")(0)
    }).toSet
    val top_domains = sc.broadcast(l_top_domains)


    val topics = sc.broadcast(ipToTopicMixes)
    val words = sc.broadcast(wordToProbPerTopic)

    val multidata = {
      var df = sqlContext.parquetFile(inputPath.split(",")(0)).filter("frame_len is not null and unix_tstamp is not null")
      val files = inputPath.split(",")
      for ((file, index) <- files.zipWithIndex) {
        if (index > 1) {
          df = df.unionAll(sqlContext.parquetFile(file).filter("frame_len is not null and unix_tstamp is not null"))
        }
      }
      df = df.select("frame_time", "unix_tstamp", "frame_len", "ip_dst", "dns_qry_name", "dns_qry_class", "dns_qry_type", "dns_qry_rcode")
      df_cols = df.columns
      val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
      tempRDD
    }
    val rawdata: org.apache.spark.rdd.RDD[String] = {
      multidata
    val topicLines = documentResults.map(line => {
      val ip = line.split(",")(0)
      val text = line.split(",")(1)
      val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
      (ip, text_no_quote)
    }).map(elem => elem._1 -> elem._2).toMap

    val topics = sc.broadcast(topicLines)

    val wordLines = wordResults.map(line => {
      val word = line.split(",")(0)
      val text = line.split(",")(1)
      val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
      (word, text_no_quote)
    }).map(elem => elem._1 -> elem._2).toMap

    val words = sc.broadcast(wordLines)

    val totalDataDF = {
      sqlContext.parquetFile(inputPath.split(",")(0))
        .filter(Schema.Timestamp + " is not null and " + Schema.UnixTimestamp + " is not null")
        .select(Schema.Timestamp,
          Schema.UnixTimestamp,
          Schema.FrameLength,
          Schema.ClientIP,
          Schema.QueryName,
          Schema.QueryClass,
          Schema.QueryType,
          Schema.QueryResponseCode)
    }

    val dataWithWordDF = DNSWordCreation.dnsWordCreation(totalDataDF, sc, logger, sqlContext)

    logger.info("Computing conditional probability")

    val dataScored: DataFrame = score(dataWithWordDF, topics, words)

    logger.info("Persisting data")

    val filteredDF = dataScored.filter(Schema.Score + " < " + threshold)

    val count = filteredDF.count

    val takeCount  = if (topK == -1 || count < topK) {
      count.toInt
    } else {
      topK
    }

    val scoreIndex = filteredDF.schema.fieldNames.indexOf(Schema.Score)

    class DataOrdering() extends Ordering[Row] {
      def compare(row1: Row, row2: Row) = row1.getDouble(scoreIndex).compare(row2.getDouble(scoreIndex))
    }

    implicit val ordering = new DataOrdering()

    val top : Array[Row] = filteredDF.rdd.takeOrdered(takeCount)

    val outputRDD = sc.parallelize(top).sortBy(row => row.getDouble(scoreIndex))
    outputRDD.map(_.mkString(outputDelimiter)).saveAsTextFile(resultsFilePath)

    logger.info("DNS Post LDA completed")
  }

  def score(dataWithWordDF: DataFrame, topics: Broadcast[Map[String, Array[Double]]], words: Broadcast[Map[String, Array[Double]]]) = {
    def scoreFunction(ip: String, word: String) : Double = {

      val uniformProb = Array.fill(20){0.05d}

      val topicGivenDocProbs  = topics.value.getOrElse(ip, uniformProb)
      val wordGivenTopicProbs = words.value.getOrElse(word, uniformProb)

      topicGivenDocProbs.zip(wordGivenTopicProbs)
        .map({case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic*pTopicGivenDoc })
        .sum
    }

    def udfScoreFunction = udf((ip: String, word: String) => scoreFunction(ip,word))

    dataWithWordDF.withColumn(Schema.Score, udfScoreFunction(dataWithWordDF(Schema.ClientIP), dataWithWordDF(Schema.Word)))
  }

}
