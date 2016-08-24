package org.opennetworkinsight.proxy

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper.OniLDACInput
import org.opennetworkinsight.utilities._
import org.slf4j.Logger
import org.opennetworkinsight.proxy.ProxySchema._
/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming proxy records.
  */

object ProxyPreLDA {


  def getIPWordCounts(inputPath: String, feedbackFile: String, duplicationFactor: Int,
                      sc: SparkContext, sqlContext: SQLContext, logger: Logger): RDD[OniLDACInput] = {

    logger.info("Proxy pre LDA starts")



    val rawDataDF = sqlContext.read.parquet(inputPath).
      filter(Date + " is not null and " + Time + " is not null and " + ClientIP + " is not null").
      select(Date, Time, ClientIP, Host, ReqMethod, UserAgent, ResponseContentType, RespCode, FullURI)

    val scoredFileExists = new java.io.File(feedbackFile).exists

    logger.info("Read source data")

    val totalDataDF = rawDataDF.unionAll(ProxyFeedback.loadFeedbackDF(feedbackFile, duplicationFactor, sc, sqlContext))

    val topDomains : Broadcast[Set[String]] = sc.broadcast(TopDomains.TOP_DOMAINS)

    def getTimeAsDouble(timeStr: String) = {
      val s = timeStr.split(":")
      val hours = s(0).toInt
      val minutes = s(1).toInt
      val seconds = s(2).toInt

      (3600*hours + 60*minutes + seconds).toDouble
    }



    val timeCuts =
      Quantiles.computeDeciles(rawDataDF.select(Time).rdd.map({case Row(t: String) => getTimeAsDouble(t)}))

    val entropyCuts = Quantiles.computeQuintiles(rawDataDF.select(FullURI).
      rdd.map({case Row(uri: String) => Entropy.stringEntropy(uri)}))

    val agentToCount: Map[String, Long] =
      rawDataDF.select(UserAgent).rdd.map({case Row(ua: String) => (ua,1L)}).reduceByKey(_+_).collect().toMap

    val agentToCountBC = sc.broadcast(agentToCount)

    val agentCuts = Quantiles.computeQuintiles(rawDataDF.select(UserAgent).rdd.map({case Row(ua: String) => agentToCountBC.value(ua)}))

    val wc = ipWordCountFromDF(rawDataDF, topDomains, agentToCountBC, timeCuts, entropyCuts, agentCuts)
    logger.info("proxy pre LDA completed")

    wc
  }

  def ipWordCountFromDF(dataFrame: DataFrame,
                        topDomains: Broadcast[Set[String]],
                        agentToCountBC: Broadcast[Map[String, Long]],
                        timeCuts: Array[Double],
                        entropyCuts: Array[Double],
                        agentCuts: Array[Double]) : RDD[OniLDACInput] = {

    val udfWordCreation = ProxyWordCreation.udfWordCreation(topDomains, agentToCountBC,  timeCuts, entropyCuts, agentCuts)

    val ipWordDF = dataFrame.withColumn(Word,
      udfWordCreation(dataFrame(Host),
        dataFrame(Time),
        dataFrame(ReqMethod),
        dataFrame(FullURI),
        dataFrame(ResponseContentType),
        dataFrame(UserAgent),
        dataFrame(RespCode))).
      select(ClientIP, Word)

    ipWordDF.rdd.map({case Row(ip, word) => ((ip.asInstanceOf[String], word.asInstanceOf[String]), 1)}).reduceByKey(_ + _).map({case ((ip, word), count) => OniLDACInput(ip, word, count) })
  }
}

