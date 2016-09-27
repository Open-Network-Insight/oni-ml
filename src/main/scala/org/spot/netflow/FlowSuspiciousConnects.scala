package org.spot.netflow

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.spot.SpotLDACWrapper
import org.spot.SpotLDACWrapper.SpotLDACOutput
import org.spot.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig

object FlowSuspiciousConnects {

  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger)(implicit outputDelimiter: String) = {
    
    logger.info("Flow LDA starts")

    val docWordCount = FlowPreLDA.flowPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val SpotLDACOutput(documentResults, wordResults) = SpotLDACWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.topicCount, config.localPath,
      config.ldaPath, config.localUser,  config.analysis, config.nodes, config.ldaPRGSeed)

    FlowPostLDA.flowPostLDA(config.inputPath, config.hdfsScoredConnect, config.outputDelimiter, config.threshold, config.maxResults, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("Flow LDA completed")
  }

}