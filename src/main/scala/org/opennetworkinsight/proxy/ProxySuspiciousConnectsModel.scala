package org.opennetworkinsight.proxy

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper
import org.opennetworkinsight.OniLDACWrapper.{OniLDACInput, OniLDACOutput}
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.opennetworkinsight.utilities._
import org.slf4j.Logger
import org.opennetworkinsight.proxy.ProxySchema._

/**
  * Encapsulation of a proxy suspicious connections model.
  *
  * @param topicCount         Number of "topics" used to cluster IPs and proxy "words" in the topic modelling analysis.
  * @param ipToTopicMIx       Maps each IP to a vector measuring Prob[ topic | this IP] for each topic.
  * @param wordToPerTopicProb Maps each word to a vector measuring Prob[word | topic] for each topic.
  * @param timeCuts           Decile cutoffs for time-of-day in seconds.
  * @param entropyCuts        Quintile cutoffs for measurement of full URI string entropy.
  * @param agentCuts          Quintiile cutoffs for frequency of user agent.
  */
class ProxySuspiciousConnectsModel(topicCount: Int,
                                   ipToTopicMIx: Map[String, Array[Double]],
                                   wordToPerTopicProb: Map[String, Array[Double]],
                                   timeCuts: Array[Double],
                                   entropyCuts: Array[Double],
                                   agentCuts: Array[Double]) {


  /**
    * Calculate suspicious connection scores for an incoming dataframe using this proxy suspicious connects model.
    *
    * @param sc        Spark context.
    * @param dataFrame Dataframe with columns Host, Time, ReqMethod, FullURI, ResponseContentType, UserAgent, RespCode
    *                  (as defined in ProxySchema object).
    * @return Dataframe with Score column added.
    */
  def score(sc: SparkContext, dataFrame: DataFrame): DataFrame = {

    val topDomains: Broadcast[Set[String]] = sc.broadcast(TopDomains.TOP_DOMAINS)

    val agentToCount: Map[String, Long] =
      dataFrame.select(UserAgent).rdd.map({ case Row(ua: String) => (ua, 1L) }).reduceByKey(_ + _).collect().toMap

    val agentToCountBC = sc.broadcast(agentToCount)

    val udfWordCreation =
      ProxyWordCreation.udfWordCreation(topDomains, agentToCountBC, timeCuts, entropyCuts, agentCuts)

    val wordedDataFrame = dataFrame.withColumn(Word,
      udfWordCreation(dataFrame(Host),
        dataFrame(Time),
        dataFrame(ReqMethod),
        dataFrame(FullURI),
        dataFrame(ResponseContentType),
        dataFrame(UserAgent),
        dataFrame(RespCode)))

    val ipToTopicMixBC = sc.broadcast(ipToTopicMIx)
    val wordToPerTopicProbBC = sc.broadcast(wordToPerTopicProb)

    def scoreFunction(ip: String, word: String): Double = {

      val uniformProb = Array.fill(topicCount) {
        1.0d / topicCount
      }

      val topicGivenDocProbs = ipToTopicMixBC.value.getOrElse(ip, uniformProb)
      val wordGivenTopicProbs = wordToPerTopicProbBC.value.getOrElse(word, uniformProb)

      topicGivenDocProbs.zip(wordGivenTopicProbs)
        .map({ case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic * pTopicGivenDoc })
        .sum
    }

    def udfScoreFunction = udf((ip: String, word: String) => scoreFunction(ip, word))
    wordedDataFrame.withColumn(Score, udfScoreFunction(wordedDataFrame(ClientIP), wordedDataFrame(Word)))
  }
}

/**
  * Contains model creation and training routines.
  */
object ProxySuspiciousConnectsModel {

  /**
    * Factory for ProxySuspiciousConnectsModel.
    * Trains the model from the incoming DataFrame using the specified number of topics
    * for clustering in the topic model.
    *
    * @param sparkContext Spark context.
    * @param sqlContext   SQL context.
    * @param logger       Logge object.
    * @param config       SuspiciousConnetsArgumnetParser.Config object containg CLI arguments.
    * @param df           Dataframe for training data, with columns Host, Time, ReqMethod, FullURI, ResponseContentType,
    *                     UserAgent, RespCode (as defined in ProxySchema object).
    * @param topicCount   Number of topics used for topic modelling during training.
    * @return ProxySuspiciousConnectsModel
    */
  def trainNewModel(sparkContext: SparkContext,
                    sqlContext: SQLContext,
                    logger: Logger,
                    config: SuspiciousConnectsConfig,
                    df: DataFrame,
                    topicCount: Int): ProxySuspiciousConnectsModel = {

    logger.info("training new proxy suspcious connects model")

    val timeCuts =
      Quantiles.computeDeciles(df.select(Time).rdd.map({ case Row(t: String) => TimeUtilities.getTimeAsDouble(t) }))

    val entropyCuts = Quantiles.computeQuintiles(df.select(FullURI).
      rdd.map({ case Row(uri: String) => Entropy.stringEntropy(uri) }))

    val agentToCount: Map[String, Long] =
      df.select(UserAgent).rdd.map({ case Row(agent: String) => (agent, 1L) }).reduceByKey(_ + _).collect().toMap

    val agentToCountBC = sparkContext.broadcast(agentToCount)

    val agentCuts =
      Quantiles.computeQuintiles(df.select(UserAgent).rdd.map({ case Row(agent: String) => agentToCountBC.value(agent) }))

    val docWordCount: RDD[OniLDACInput] = getIPWordCounts(sparkContext, sqlContext, logger, df, config.scoresFile, config.duplicationFactor, agentToCount, timeCuts, entropyCuts, agentCuts)


    val OniLDACOutput(documentResults, wordResults) = OniLDACWrapper.runLDA(docWordCount,
      config.modelFile,
      config.topicDocumentFile,
      config.topicWordFile,
      config.mpiPreparationCmd,
      config.mpiCmd,
      config.mpiProcessCount,
      config.mpiTopicCount,
      config.localPath,
      config.ldaPath,
      config.localUser,
      config.analysis,
      config.nodes)

    new ProxySuspiciousConnectsModel(topicCount, documentResults, wordResults, timeCuts, entropyCuts, agentCuts)
  }

  /**
    * Transform proxy log events into summarized words and aggregate into IP-word counts.
    * Returned as OniLDACInput objects.
    *
    * @return RDD of OniLDACInput objects containing the aggregated IP-word counts.
    */
  def getIPWordCounts(sc: SparkContext, sqlContext: SQLContext, logger: Logger, rawDataDF: DataFrame, feedbackFile: String, duplicationFactor: Int, agentToCount: Map[String, Long], timeCuts: Array[Double], entropyCuts: Array[Double], agentCuts: Array[Double]): RDD[OniLDACInput] = {


    val scoredFileExists = new java.io.File(feedbackFile).exists

    logger.info("Read source data")

    val totalDataDF = rawDataDF.unionAll(ProxyFeedback.loadFeedbackDF(sc, sqlContext, feedbackFile, duplicationFactor))

    val wc = ipWordCountFromDF(sc, rawDataDF, agentToCount, timeCuts, entropyCuts, agentCuts)
    logger.info("proxy pre LDA completed")

    wc
  }

  def ipWordCountFromDF(sc: SparkContext,
                        dataFrame: DataFrame,
                        agentToCount: Map[String, Long],
                        timeCuts: Array[Double],
                        entropyCuts: Array[Double],
                        agentCuts: Array[Double]): RDD[OniLDACInput] = {

    val topDomains: Broadcast[Set[String]] = sc.broadcast(TopDomains.TOP_DOMAINS)

    val agentToCountBC = sc.broadcast(agentToCount)
    val udfWordCreation = ProxyWordCreation.udfWordCreation(topDomains, agentToCountBC, timeCuts, entropyCuts, agentCuts)

    val ipWordDF = dataFrame.withColumn(Word,
      udfWordCreation(dataFrame(Host),
        dataFrame(Time),
        dataFrame(ReqMethod),
        dataFrame(FullURI),
        dataFrame(ResponseContentType),
        dataFrame(UserAgent),
        dataFrame(RespCode))).
      select(ClientIP, Word)

    ipWordDF.rdd.map({ case Row(ip, word) => ((ip.asInstanceOf[String], word.asInstanceOf[String]), 1) })
      .reduceByKey(_ + _).map({ case ((ip, word), count) => OniLDACInput(ip, word, count) })
  }
}