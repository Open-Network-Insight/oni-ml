package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper.OniLDACOutput
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.opennetworkinsight.OniLDACWrapper
import org.opennetworkinsight.OniLDACWrapper.OniLDACOutput
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.opennetworkinsight.dns.DNSSchema._
import org.opennetworkinsight.proxy.ProxySchema.{ClientIP => _, Score => _, _}
import org.opennetworkinsight.utilities.{CountryCodes, DataFrameUtils}
import org.slf4j.Logger

/**
  * Run suspicious connections analysis on DNS log data.
  */

object DNSSuspiciousConnectsAnalysis {

  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {
    import sqlContext.implicits._

    logger.info("Starting DNS suspicious connects analysis.")

    logger.info("Loading data")


    val topicCount = 20

    val rawDataDF = sqlContext.read.parquet(config.inputPath)
      .filter(Timestamp + " is not null and " + UnixTimestamp + " is not null")
      .select(ModelColumns:_*)

    logger.info("Training the model")
    val model =
      DNSSuspiciousConnectsModel.trainNewModel(sparkContext, sqlContext, logger, config, rawDataDF, topicCount)

    logger.info("Scoring")
    val scoredDF = model.score(sparkContext, sqlContext, rawDataDF)

    // take the maxResults least probable events of probability below the threshold and sort

    val filteredDF = scoredDF.filter(Score +  " <= " + config.threshold)
    val topRows = DataFrameUtils.dfTakeOrdered(filteredDF, Score, config.maxResults)
    val topRowsDF : DataFrame = sqlContext.createDataFrame(sparkContext.parallelize(topRows),filteredDF.schema)

    // add the OA required columns  here
    val sideInformationGenerator = new DNSSideInformation(model)
    val dfWithSideInfo = sideInformationGenerator.addSideInformationForOA(sparkContext, sqlContext, topRowsDF)

    val outputDF = dfWithSideInfo.sort(Score)

    logger.info("DNS  suspcicious connects analysis completed.")
    logger.info("Saving results to : " + config.hdfsScoredConnect)
    outputDF.map(_.mkString(config.outputDelimiter)).saveAsTextFile(config.hdfsScoredConnect)
  }
}