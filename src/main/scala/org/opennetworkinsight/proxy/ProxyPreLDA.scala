package org.opennetworkinsight.proxy

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper.OniLDACInput
import org.opennetworkinsight.utilities._
import org.slf4j.Logger
import org.opennetworkinsight.proxy.{ProxySchema => Schema}
/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming proxy records.
  */

object ProxyPreLDA {


  def getIPWordCounts(inputPath: String, feedbackFile: String, duplicationFactor: Int,
                      sc: SparkContext, sqlContext: SQLContext, logger: Logger): RDD[OniLDACInput] = {

    logger.info("Proxy pre LDA starts")

    val feedbackFileExists = new java.io.File(feedbackFile).exists

    val rawDataDF = sqlContext.parquetFile(inputPath).
      filter(Schema.Date + " is not null and " + Schema.Time + " is not null and " + Schema.ClientIP + " is not null").
      select(Schema.Date,
        Schema.Time,
        Schema.ClientIP,
        Schema.Host,
        Schema.ReqMethod,
        Schema.UserAgent,
        Schema.ResponseContentType,
        Schema.RespCode,
        Schema.FullURI)


    // TBD: incorporate feedback data


    val topDomains : Broadcast[Set[String]] = sc.broadcast(TopDomains.TOP_DOMAINS)

    def getTimeAsDouble(timeStr: String) = {
      val s = timeStr.split(":")
      val hours = s(0).toInt
      val minutes = s(1).toInt
      val seconds = s(2).toInt

      (3600*hours + 60*minutes + seconds).toDouble
    }



    val timeCuts =
      Quantiles.computeDeciles(rawDataDF.select(Schema.Time).rdd.map({case Row(t: String) => getTimeAsDouble(t)}))

    val entropyCuts = Quantiles.computeQuintiles(rawDataDF.select(Schema.FullURI).
      rdd.map({case Row(uri: String) => Entropy.stringEntropy(uri)}))

    val agentToCount: Map[String, Long] =
      rawDataDF.select(Schema.UserAgent).rdd.map({case Row(ua: String) => (ua,1L)}).reduceByKey(_+_).collect().toMap

    val agentToCountBC = sc.broadcast(agentToCount)

    val agentCuts = Quantiles.computeQuintiles(rawDataDF.select(Schema.UserAgent).rdd.map({case Row(ua: String) => agentToCountBC.value(ua)}))

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

    val ipWordDF = dataFrame.withColumn(Schema.Word,
      udfWordCreation(dataFrame(Schema.Host),
        dataFrame(Schema.Time),
        dataFrame(Schema.ReqMethod),
        dataFrame(Schema.FullURI),
        dataFrame(Schema.ResponseContentType),
        dataFrame(Schema.UserAgent),
        dataFrame(Schema.RespCode))).
      select(Schema.ClientIP, Schema.Word)

    ipWordDF.rdd.map({case Row(ip, word) => ((ip.asInstanceOf[String], word.asInstanceOf[String]), 1)}).reduceByKey(_ + _).map({case ((ip, word), count) => OniLDACInput(ip, word, count) })
  }
}

