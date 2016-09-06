package org.opennetworkinsight.netflow

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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
                  docToTopicMix: Map[String, Array[Double]],
                  wordToProbPerTopic: Map[String, Array[Double]],
                  sc: SparkContext,
                  sqlContext: SQLContext,
                  logger: Logger) = {

    logger.info("loading machine learning results")

    import sqlContext.implicits._
    logger.info("loading data")
    val totalDataDF: DataFrame = {
      sqlContext.read.parquet(inputPath)
        .filter(Hour + " BETWEEN 0 AND 23 AND  " +
          Minute + " BETWEEN 0 AND 59 AND  " +
          Second + " BETWEEN 0 AND 59")
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

    val docToTopicMixesRDD: RDD[(String, Array[Double])] = sc.parallelize(docToTopicMix.toSeq)

    val ipToTopicMixesDF = docToTopicMixesRDD.map({ case (doc, probabilities) => DocTopicMixes(doc, probabilities) }).toDF

    val words = sc.broadcast(wordToProbPerTopic)

    val dataWithSrcScore = score(sc, dataWithWord, ipToTopicMixesDF, words, SourceScore, SourceIP, SourceProbabilities, SourceWord)
    val dataWithDestScore = score(sc, dataWithSrcScore, ipToTopicMixesDF, words, DestinationScore, DestinationIP, DestinationProbabilities, DestinationWord)
    val dataScored = minimumScore(dataWithDestScore)

    logger.info("Persisting data")
    val filteredDF = dataScored.filter(MinimumScore + " < " + threshold)
    filteredDF.orderBy(MinimumScore).limit(topK).rdd.map(row => Row.fromSeq(row.toSeq.dropRight(1))).map(_.mkString(outputDelimiter)).saveAsTextFile(resultsFilePath)

    logger.info("Flow post LDA completed")
  }

  def score(sc: SparkContext,
            dataFrame: DataFrame,
            ipToTopicMixesDF: DataFrame,
            words: Broadcast[Map[String, Array[Double]]],
            newColumnName: String,
            ipColumnName: String,
            ipProbabilitiesColumnName: String,
            wordColumnName: String): DataFrame = {

    val dataWithIpProbJoin = dataFrame.join(ipToTopicMixesDF, dataFrame(ipColumnName) === ipToTopicMixesDF(Doc))

    var newSchemaColumns = dataFrame.schema.fieldNames :+ Probabilities + " as " + ipProbabilitiesColumnName
    val dataWithIpProb = dataWithIpProbJoin.selectExpr(newSchemaColumns: _*)

    def scoreFunction(word: String, ipProbabilities: Seq[Double]): Double = {
      val uniformProb = Array.fill(20)(0.05d)
      val wordGivenTopicProb = words.value.getOrElse(word, uniformProb)

      ipProbabilities.zip(wordGivenTopicProb)
        .map({ case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic * pTopicGivenDoc })
        .sum
    }

    def udfScoreFunction = udf((word: String, ipProbabilities: Seq[Double]) => scoreFunction(word, ipProbabilities))

    val result: DataFrame = dataWithIpProb.withColumn(newColumnName, udfScoreFunction(dataWithIpProb(wordColumnName), dataWithIpProb(ipProbabilitiesColumnName)))
    newSchemaColumns = dataFrame.schema.fieldNames :+ newColumnName
    result.select(newSchemaColumns.map(col): _*)
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

  case class DocTopicMixes(doc: String, probabilities: Array[Double]) extends Serializable
}