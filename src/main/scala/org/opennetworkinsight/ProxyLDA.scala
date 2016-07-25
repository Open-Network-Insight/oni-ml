package org.opennetworkinsight

import org.opennetworkinsight.LDAArgumentParser.Config

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.slf4j.Logger

/**
  * Run suspicious connections anlaysis on proxy data.
  */
object ProxyLDA {

  def run(config: Config, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("Proxy LDA starts")

    val docWordCount = ProxyPreLDA.proxyPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val ldaResult = LDAWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser,  config.dataSource, config.nodes)

    ProxyPostLDA.proxyPostLDA(config.inputPath, config.hdfsScoredConnect, config.threshold, ldaResult("document_results"),
      ldaResult("word_results"), sparkContext, sqlContext, logger)

    logger.info("Proxy LDA completed")
  }

}