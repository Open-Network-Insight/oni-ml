package org.apache.spot.netflow.model

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spot.SpotLDACWrapper
import org.apache.spot.SpotLDACWrapper.{SpotLDACInput, SpotLDACOutput}
import org.apache.spot.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.apache.spot.netflow.FlowSchema._
import org.apache.spot.netflow.FlowWordCreator
import org.apache.spot.utilities.Quantiles

/**
  * A probabilistic model of the netflow traffic observed in a network.
  *
  * The model uses a topic-modelling approach that:
  * 1. Simplifies netflow records into words, one word at the source IP and another (possibly different) at the
  *    destination IP.
  * 2. The netflow words about each IP are treated as collections of thes words.
  * 3. A topic modelling approach is used to infer a collection of "topics" that represent common profiles
  *    of network traffic. These "topics" are probability distributions on words.
  * 4. Each IP has a mix of topics corresponding to its behavior.
  * 5. The probability of a word appearing in the traffic about an IP is estimated by simplifying its netflow record
  *    into a word, and then combining the word probabilities per topic using the topic mix of the particular IP.
  *
  * Create these models using the  factory in the companion object.
  *
  * @param topicCount Number of topics (profiles of common traffic patterns) used in the topic modelling routine.
  * @param ipToTopicMix Map assigning a distribution on topics to each IP.
  * @param wordToPerTopicProb Map assigning to each word it's per-topic probabilities.
  *                           Ie. Prob [word | t ] for t = 0 to topicCount -1
  * @param timeCuts Quantile cut-offs for binning time-of-day values when forming words from netflow records.
  * @param ibytCuts Quantile cut-offs for binning ibyt values when forming words from netflow records.
  * @param ipktCuts Quantile cut-offs for binning ipkt values when forming words from netflow records.
  */

class FlowSuspiciousConnectsModel(topicCount: Int,
                                  ipToTopicMix: Map[String, Array[Double]],
                                  wordToPerTopicProb: Map[String, Array[Double]],
                                  timeCuts: Array[Double],
                                  ibytCuts: Array[Double],
                                  ipktCuts: Array[Double]) {

  def score(sc: SparkContext, sqlContext: SQLContext, inDF: DataFrame): DataFrame = {


    val ipToTopicMixBC = sc.broadcast(ipToTopicMix)
    val wordToPerTopicProbBC = sc.broadcast(wordToPerTopicProb)


    val scoreFunction =
      new FlowScoreFunction(timeCuts,
        ibytCuts,
        ipktCuts,
        topicCount,
        ipToTopicMixBC,
        wordToPerTopicProbBC)


    val scoringUDF = udf((hour: Int,
                          minute: Int,
                          second: Int,
                          srcIP: String,
                          dstIP: String,
                          srcPort: Int,
                          dstPort: Int,
                          ipkt: Long,
                          ibyt: Long) =>
      scoreFunction.score(hour,
        minute,
        second,
        srcIP,
        dstIP,
        srcPort,
        dstPort,
        ipkt,
        ibyt))

    inDF.withColumn(Score, scoringUDF(FlowSuspiciousConnectsModel.ModelColumns: _*))
  }
}

/**
  * Contains dataframe schema information as well as the train-from-dataframe routine
  * (which is a kind of factory routine) for [[FlowSuspiciousConnectsModel]] instances.
  *
  */
object FlowSuspiciousConnectsModel {

  val ModelSchema = StructType(List(HourField,
    MinuteField,
    SecondField,
    SourceIPField,
    DestinationIPField,
    SourcePortField,
    DestinationPortField,
    IpktField,
    IbytField))

  val ModelColumns = ModelSchema.fieldNames.toList.map(col)

  def trainNewModel(sparkContext: SparkContext,
                    sqlContext: SQLContext,
                    logger: Logger,
                    config: SuspiciousConnectsConfig,
                    inDF: DataFrame,
                    topicCount: Int): FlowSuspiciousConnectsModel = {

    logger.info("Training netflow suspicious connects model from " + config.inputPath)

    val selectedDF = inDF.select(ModelColumns: _*)


    val totalDataDF = selectedDF.unionAll(FlowFeedback.loadFeedbackDF(sparkContext,
      sqlContext,
      config.scoresFile,
      config.duplicationFactor))



    // create quantile cut-offs

    val timeCuts = Quantiles.computeDeciles(totalDataDF
      .select(Hour, Minute, Second)
      .rdd
      .map({ case Row(hours: Int, minutes: Int, seconds: Int) => 3600 * hours + 60 * minutes + seconds }))

    logger.info(timeCuts.mkString(","))

    logger.info("calculating byte cuts ...")

    val ibytCuts = Quantiles.computeDeciles(totalDataDF
      .select(Ibyt)
      .rdd
      .map({ case Row(ibyt: Long) => ibyt.toDouble }))

    logger.info(ibytCuts.mkString(","))

    logger.info("calculating pkt cuts")

    val ipktCuts = Quantiles.computeQuintiles(totalDataDF
      .select(Ipkt)
      .rdd
      .map({ case Row(ipkt: Long) => ipkt.toDouble }))


    logger.info(ipktCuts.mkString(","))

    // simplify DNS log entries into "words"

    val flowWordCreator = new FlowWordCreator(ibytCuts, ipktCuts, timeCuts)

    val srcWordUDF = flowWordCreator.srcWordUDF
    val dstWordUDF = flowWordCreator.dstWordUDF

    val dataWithWordsDF = totalDataDF.withColumn(SourceWord, flowWordCreator.srcWordUDF(ModelColumns: _*))
      .withColumn(DestinationWord, flowWordCreator.dstWordUDF(ModelColumns: _*))
    // aggregate per-word counts at each IP

    val srcWordCounts = dataWithWordsDF.select(SourceIP, SourceWord)
      .map({ case Row(sourceIp: String, sourceWord: String) => (sourceIp, sourceWord) -> 1 })
      .reduceByKey(_ + _)

    val dstWordCounts = dataWithWordsDF.select(DestinationIP, DestinationWord)
      .map({ case Row(destinationIp: String, destinationWord: String) => (destinationIp, destinationWord) -> 1 })
      .reduceByKey(_ + _)

    val ipWordCounts =
      sparkContext.union(srcWordCounts, dstWordCounts).map({ case ((ip, word), count) => SpotLDACInput(ip, word, count) })


    val SpotLDACOutput(ipToTopicMix, wordToPerTopicProb) = SpotLDACWrapper.runLDA(ipWordCounts,
      config.modelFile,
      config.topicDocumentFile,
      config.topicWordFile,
      config.mpiPreparationCmd,
      config.mpiCmd,
      config.mpiProcessCount,
      config.topicCount,
      config.localPath,
      config.ldaPath,
      config.localUser,
      config.analysis,
      config.nodes,
      config.ldaPRGSeed)


    new FlowSuspiciousConnectsModel(topicCount,
      ipToTopicMix,
      wordToPerTopicProb,
      timeCuts,
      ibytCuts,
      ipktCuts)
  }

}