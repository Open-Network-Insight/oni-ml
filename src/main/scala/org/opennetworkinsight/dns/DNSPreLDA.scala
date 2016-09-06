package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper.OniLDACInput
import org.opennetworkinsight.dns.DNSSchema._
import org.opennetworkinsight.proxy.ProxyFeedback
import org.opennetworkinsight.proxy.ProxySchema.{ClientIP => _, Word => _, _}
import org.slf4j.Logger

import scala.io.Source

/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming DNS records.
  */
object DNSPreLDA {

  def dnsPreLDA(inputPath: String,
                scoresFile: String,
                duplicationFactor: Int,
                sc: SparkContext,
                sqlContext: SQLContext,
                logger: Logger,
                inDF: DataFrame ): RDD[OniLDACInput] =  {

    logger.info("DNS pre LDA starts")

    import sqlContext.implicits._


    print("Read source data")
    logger.info("Read source data")
    val df = inDF.select(Timestamp,
      UnixTimestamp,
      FrameLength,
      ClientIP,
      QueryName,
      QueryClass,
      QueryType,
      QueryResponseCode)
    val totalDataDF = df.unionAll(ProxyFeedback.loadFeedbackDF(sc, sqlContext, scoresFile, duplicationFactor))

    val dataWithWordDF = DNSWordCreation.dnsWordCreation(totalDataDF, sc, logger, sqlContext)

    val ipDstWordCounts = dataWithWordDF
      .select(ClientIP, Word)
      .map({
        case Row(destIP: String, word: String) =>
          (destIP, word) -> 1
      })
      .reduceByKey(_ + _)

    ipDstWordCounts.map({case ((ipDst, word), count) => OniLDACInput(ipDst, word, count)})
  }

}

