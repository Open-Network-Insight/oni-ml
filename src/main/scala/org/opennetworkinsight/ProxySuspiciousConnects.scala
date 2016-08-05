package org.opennetworkinsight

import org.opennetworkinsight.SuspiciousConnectsArgumentParser.Config

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.slf4j.Logger

/**
  * Run suspicious connections analysis on proxy data.
  */
object ProxySuspiciousConnects {

  def run(config: Config, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("Proxy suspicious connects starts")

    val docWordCount = ProxyPreLDA.getIPWordCounts(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val (documentResults, wordResults) = LDAWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser,  config.dataSource, config.nodes)

    val topicCount  = 20
    ProxyPostLDA.getResults(config.inputPath, config.hdfsScoredConnect, topicCount,  config.threshold, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("Proxy suspcicious connects completed")
  }

}