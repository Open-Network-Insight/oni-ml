package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.opennetworkinsight.OniLDACWrapper
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.Config
import org.slf4j.Logger

object DNSSuspiciousConnects {

  def run(config: Config, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("DNS LDA starts")

    val docWordCount = DNSPreLDA.dnsPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val (documentResults, wordResults) = OniLDACWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser, config.analysis, config.nodes)

    DNSPostLDA.dnsPostLDA(config.inputPath, config.hdfsScoredConnect, config.threshold, config.maxResults, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("DNS LDA completed")
  }

}