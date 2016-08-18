package org.opennetworkinsight.proxy

import org.apache.log4j.{Logger => apacheLogger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.OniLDACWrapper.OniLDACInput
import org.opennetworkinsight.utilities._
import org.slf4j.Logger

/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming proxy records.
  */

object ProxyPreLDA {


  def getIPWordCounts(inputPath: String, feedbackFile: String, duplicationFactor: Int,
                      sc: SparkContext, sqlContext: SQLContext, logger: Logger): RDD[OniLDACInput] = {

    logger.info("Proxy pre LDA starts")

    val feedbackFileExists = new java.io.File(feedbackFile).exists

    val rawDataDF = sqlContext.parquetFile(inputPath).
      filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null").
      select("proxy_date",
        "proxy_time",
        "proxy_clientip",
        "proxy_host",
        "proxy_reqmethod",
        "proxy_useragent",
        "proxy_resconttype",
        "proxy_respcode",
        "proxy_fulluri")


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
      Quantiles.computeDeciles(rawDataDF.select("proxy_time").rdd.map({case Row(t: String) => getTimeAsDouble(t)}))

    val entropyCuts = Quantiles.computeQuintiles(rawDataDF.select("proxy_fulluri").
      rdd.map({case Row(uri: String) => Entropy.stringEntropy(uri)}))

    val agentToCount: Map[String, Long] =
      rawDataDF.select("proxy_useragent").rdd.map({case Row(ua: String) => (ua,1L)}).reduceByKey(_+_).collect().toMap

    val agentToCountBC = sc.broadcast(agentToCount)

    val agentCuts = Quantiles.computeQuintiles(rawDataDF.select("proxy_useragent").rdd.map({case Row(ua: String) => agentToCountBC.value(ua)}))

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

    val ipWordDF = dataFrame.withColumn("word",
      udfWordCreation(dataFrame("proxy_host"),
        dataFrame("proxy_time"),
        dataFrame("proxy_reqmethod"),
        dataFrame("proxy_fulluri"),
        dataFrame("proxy_resconttype"),
        dataFrame("proxy_useragent"),
        dataFrame("proxy_respcode"))).
      select("proxy_clientip", "word")

    ipWordDF.rdd.map({case Row(ip, word) => ((ip.asInstanceOf[String], word.asInstanceOf[String]), 1)}).reduceByKey(_ + _).map({case ((ip, word), count) => OniLDACInput(ip, word, count) })
  }
}

