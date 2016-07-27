package org.opennetworkinsight

import org.opennetworkinsight.LDAArgumentParser.Config

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.slf4j.Logger

object FlowLDA {

  def run(config: Config, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("Flow LDA starts")

    val docWordCount = FlowPreLDA.flowPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)
    // TODO persist docWordCount in MEMORY and DISK
    val (documentResults, wordResults) = LDAWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser,  config.dataSource, config.nodes)

    FlowPostLDA.flowPostLDA(config.inputPath, config.hdfsScoredConnect, config.threshold, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("Flow LDA completed")
  }

}