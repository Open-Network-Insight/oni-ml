
package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.DNSSchema._
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

    val totalDataDF = {
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

    val dataWithWordDF = DNSWordCreation.dnsWordCreation(totalDataDF, sc, logger, sqlContext)

    logger.info("Computing conditional probability")

    val dataScored: DataFrame = score(sc, dataWithWordDF, ipToTopicMixes, wordToProbPerTopic)

    logger.info("Persisting data")

    val filteredDF = dataScored.filter(Score + " < " + threshold)

    val count = filteredDF.count

    val takeCount  = if (topK == -1 || count < topK) {
      count.toInt
    } else {
      topK
    }

    val scoreIndex = filteredDF.schema.fieldNames.indexOf(Score)

    class DataOrdering() extends Ordering[Row] {
      def compare(row1: Row, row2: Row) = row1.getDouble(scoreIndex).compare(row2.getDouble(scoreIndex))
    }

    implicit val ordering = new DataOrdering()

    val top : Array[Row] = filteredDF.rdd.takeOrdered(takeCount)

    val outputRDD = sc.parallelize(top).sortBy(row => row.getDouble(scoreIndex))
    outputRDD.map(_.mkString(outputDelimiter)).saveAsTextFile(resultsFilePath)

    logger.info("DNS Post LDA completed")
  }

  def score(sc: SparkContext,
            dataWithWordDF: DataFrame,
            ipToTopicMixes: Map[String, Array[Double]],
            wordToProbPerTopic: Map[String, Array[Double]]) = {
    def scoreFunction(ip: String, word: String) : Double = {

      val topics = sc.broadcast(ipToTopicMixes)
      val words = sc.broadcast(wordToProbPerTopic)

      val uniformProb = Array.fill(20){0.05d}

      val topicGivenDocProbs  = topics.value.getOrElse(ip, uniformProb)
      val wordGivenTopicProbs = words.value.getOrElse(word, uniformProb)

      topicGivenDocProbs.zip(wordGivenTopicProbs)
        .map({case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic*pTopicGivenDoc })
        .sum
    }

    def udfScoreFunction = udf((ip: String, word: String) => scoreFunction(ip,word))

    dataWithWordDF.withColumn(Score, udfScoreFunction(dataWithWordDF(ClientIP), dataWithWordDF(Word)))
  }

}
