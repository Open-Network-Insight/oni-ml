package org.opennetworkinsight.netflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.opennetworkinsight.OniLDACWrapper
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.Config
import org.slf4j.Logger

object FlowSuspiciousConnects {

  def run(config: Config, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("Flow LDA starts")

    val docWordCount = FlowPreLDA.flowPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val (documentResults, wordResults) = OniLDACWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser,  config.analysis, config.nodes)

    FlowPostLDA.flowPostLDA(config.inputPath, config.hdfsScoredConnect, config.threshold, config.maxResults, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("Flow LDA completed")
  }

}