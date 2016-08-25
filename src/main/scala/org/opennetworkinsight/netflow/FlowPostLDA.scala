package org.opennetworkinsight.netflow

import org.apache.log4j.{Logger => apacheLogger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.netflow.FlowSchema._
import org.slf4j.Logger

/**
  * Contains routines for scoring incoming netflow records from a netflow suspicious connections model.
  */
object FlowPostLDA {

  def flowPostLDA(inputPath: String,
                  resultsFilePath: String,
                  outputDelimiter: String,
                  threshold: Double, topK: Int,
                  ipToTopicMixes: Map[String, Array[Double]],
                  wordToProbPerTopic: Map[String, Array[Double]],
                  sc: SparkContext,
                  sqlContext: SQLContext,
                  logger: Logger) = {

    logger.info("loading machine learning results")

    logger.info("loading data")
    val totalDataDF: DataFrame = {
      sqlContext.read.parquet(inputPath)
        .filter("trhour BETWEEN 0 AND 23 AND  " +
          "trminute BETWEEN 0 AND 59 AND  " +
          "trsec BETWEEN 0 AND 59")
        .select(TimeReceived,
          Year,
          Month,
          Day,
          Hour,
          Minute,
          Second,
          Duration,
          SourceIP,
          DestinationIP,
          SourcePort,
          DestinationPort,
          proto,
          Flag,
          fwd,
          stos,
          ipkt,
          ibyt,
          opkt,
          obyt,
          input,
          output,
          sas,
          das,
          dtos,
          dir,
          rip)
    }

    val dataWithWord = FlowWordCreation.flowWordCreation(totalDataDF, sc, logger, sqlContext)

    logger.info("Computing conditional probability")

    val dataWithSrcScore = score(sc, dataWithWord, ipToTopicMixes, wordToProbPerTopic, SourceScore, SourceIP, SourceWord)
    val dataWithDestScore = score(sc, dataWithSrcScore, ipToTopicMixes, wordToProbPerTopic, DestinationScore, DestinationIP, DestinationWord)
    val dataScored = minimumScore(dataWithDestScore)

    logger.info("Persisting data")
    val filteredDF = dataScored.filter(MinimumScore + " <" + threshold)

    val count = filteredDF.count

    val takeCount  = if (topK == -1 || count < topK) {
      count.toInt
    } else {
      topK
    }

    val minimumScoreIndex = filteredDF.schema.fieldNames.indexOf(MinimumScore)

    class DataOrdering() extends Ordering[Row] {
      def compare(row1: Row, row2: Row) = row1.getDouble(minimumScoreIndex).compare(row2.getDouble(minimumScoreIndex))
    }

    implicit val ordering = new DataOrdering()

    val top: Array[Row] = filteredDF.rdd.takeOrdered(takeCount)

    val outputRDD = sc.parallelize(top).sortBy(row => row.getDouble(minimumScoreIndex))

    // Using dropRight as we don't need last column MinimumScore, only SourceScore and DestinationScore
    outputRDD.map(row => Row.fromSeq(row.toSeq.dropRight(1))).map(_.mkString(outputDelimiter)).saveAsTextFile(resultsFilePath)

    logger.info("Flow post LDA completed")

  }

  def score(sc: SparkContext,
            dataFrame: DataFrame,
            ipToTopicMixes: Map[String, Array[Double]],
            wordToProbPerTopic: Map[String, Array[Double]],
            newColumnName: String,
            ipColumnName: String,
            wordColumnName: String): DataFrame = {

    val topics = sc.broadcast(ipToTopicMixes)
    val words = sc.broadcast(wordToProbPerTopic)

    def scoreFunction(ip: String, word: String): Double = {
      val uniformProb = Array.fill(20) {
        0.05d
      }

      val topicGivenDocProbs = topics.value.getOrElse(ip, uniformProb)
      val wordGivenTopicProbs = words.value.getOrElse(word, uniformProb)

      topicGivenDocProbs.zip(wordGivenTopicProbs)
        .map({ case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic * pTopicGivenDoc })
        .sum
    }

    def udfScoreFunction = udf((ip: String, word: String) => scoreFunction(ip, word))

    dataFrame.withColumn(newColumnName, udfScoreFunction(dataFrame(ipColumnName), dataFrame(wordColumnName)))
  }

  def minimumScore(dataWithDestScore: DataFrame): DataFrame = {

    def minimumScoreFunction(sourceScore: Double, destinationScore: Double): Double = {
      scala.math.min(sourceScore, destinationScore)
    }

    def udfMinimumScoreFunction = udf((sourceScore: Double, destinationScore: Double) =>
      minimumScoreFunction(sourceScore, destinationScore))

    dataWithDestScore.withColumn(MinimumScore,
      udfMinimumScoreFunction(dataWithDestScore(SourceScore), dataWithDestScore(DestinationScore)))
  }
}