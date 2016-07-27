package org.opennetworkinsight

import org.opennetworkinsight.SuspiciousConnectsArgumentParser.Config

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.slf4j.Logger

object DNSLDA {

  def run(config: Config, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("DNS LDA starts")

    val docWordCount = DNSPreLDA.dnsPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sparkContext,
      sqlContext, logger)

    val (documentResults, wordResults) = LDAWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
      config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.localPath,
      config.ldaPath, config.localUser, config.dataSource, config.nodes)

    DNSPostLDA.dnsPostLDA(config.inputPath, config.hdfsScoredConnect, config.threshold, documentResults,
      wordResults, sparkContext, sqlContext, logger)

    logger.info("DNS LDA completed")
  }

}