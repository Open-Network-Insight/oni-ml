
package org.opennetworkinsight.dns

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.{DNSSchema => Schema}
import org.slf4j.Logger

import scala.io.Source

/**
  * Contains routines for scoring incoming netflow records from a DNS suspicious connections model.
  */
object DNSPostLDA {

  def dnsPostLDA(inputPath: String, resultsFilePath: String,  outputDelimiter: String, threshold: Double, topK: Int, documentResults: Array[String],
                 wordResults: Array[String], sc: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("DNS post LDA starts")

    import sqlContext.implicits._
    var rawDataDFColumns = new Array[String](0)

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

    val rawDataRDD = {
      val df = sqlContext.parquetFile(inputPath.split(",")(0))
        .filter(Schema.Timestamp + " is not null and " + Schema.UnixTimestamp + " is not null")
        .select(Schema.Timestamp,
          Schema.UnixTimestamp,
          Schema.FrameLength,
          Schema.ClientIP,
          Schema.QueryName,
          Schema.QueryClass,
          Schema.QueryClass,
          Schema.QueryResponseCode)
      // Need to extract raw data columns to reference index in future lines. rawDataDFColumns will be zipped with index.
      rawDataDFColumns = df.columns
      df.map(_.mkString(","))
    }

    val totalDataDF = rawDataRDD.toDF

    val dataWithWordDF = DNSWordCreation.dnsWordCreation(totalDataDF, rawDataDFColumns, sc, logger, sqlContext)

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
