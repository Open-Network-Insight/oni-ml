package org.opennetworkinsight.proxy

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.opennetworkinsight.proxy.ProxySchema._
import org.opennetworkinsight.utilities.DataFrameUtils
import org.slf4j.Logger

/**
  * Run suspicious connections analysis on proxy data.
  */
object ProxySuspiciousConnectsAnalysis {

  /**
    * Run suspicious connections analysis on proxy data.
    *
    * @param config       SuspicionConnectsConfig objet, contains runtime parameters from CLI.
    * @param sparkContext Apache Spark context.
    * @param sqlContext   Spark SQL context.
    * @param logger       Logs execution progress, information and errors for user.
    */
  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    val topicCount = 20

    logger.info("Starting proxy suspicious connects analysis.")

    logger.info("Loading data")

    val rawDataDF = sqlContext.read.parquet(config.inputPath).
      filter(Date + " is not null and " + Time + " is not null and " + ClientIP + " is not null").
      select(Date, Time, ClientIP, Host, ReqMethod, UserAgent, ResponseContentType, Duration, UserName,
        WebCat, Referer, RespCode, URIPort, URIPath, URIQuery, ServerIP, SCBytes, CSBytes, FullURI)

    logger.info("Training the model")
    val model =
      ProxySuspiciousConnectsModel.trainNewModel(sparkContext, sqlContext, logger, config, rawDataDF, topicCount)

    logger.info("Scoring")
    val scoredDF = model.score(sparkContext, rawDataDF)

    // take the maxResults least probable events of probability below the threshold and sort

    val filteredDF = scoredDF.filter(Score +  " <= " + config.threshold)
    val topRows = DataFrameUtils.dfTakeOrdered(filteredDF, "score", config.maxResults)
    val scoreIndex = scoredDF.schema.fieldNames.indexOf("score")
    val outputRDD = sparkContext.parallelize(topRows).sortBy(row => row.getDouble(scoreIndex))

    logger.info("Persisting data")
    outputRDD.map(_.mkString(config.outputDelimiter)).saveAsTextFile(config.hdfsScoredConnect)

    logger.info("Proxy suspcicious connects completed")
  }
}