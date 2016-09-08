package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.opennetworkinsight.OniLDACWrapper
import org.opennetworkinsight.OniLDACWrapper.OniLDACOutput
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.slf4j.Logger

object DNSSuspiciousConnects {

  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("DNS LDA starts")

    val docWordCount = DNSPreLDA.dnsPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val OniLDACOutput(documentResults, wordResults) = OniLDACWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser, config.analysis, config.nodes, config.ldaPRGSeed)

    DNSPostLDA.dnsPostLDA(config.inputPath, config.hdfsScoredConnect, config.outputDelimiter, config.threshold, config.maxResults, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("DNS LDA completed")
  }

}