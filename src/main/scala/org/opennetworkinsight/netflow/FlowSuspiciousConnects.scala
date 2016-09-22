package org.opennetworkinsight.netflow

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.opennetworkinsight.OniLDACWrapper
import org.opennetworkinsight.OniLDACWrapper.OniLDACOutput
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig

object FlowSuspiciousConnects {

  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger)(implicit outputDelimiter: String) = {
    
    logger.info("Flow LDA starts")

    val docWordCount = FlowPreLDA.flowPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val OniLDACOutput(documentResults, wordResults) = OniLDACWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser,  config.analysis, config.nodes)

    FlowPostLDA.flowPostLDA(config.inputPath, config.hdfsScoredConnect, config.outputDelimiter, config.threshold, config.maxResults, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("Flow LDA completed")
  }

}