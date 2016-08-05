package org.opennetworkinsight

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._

/**
  * Created by nlsegerl on 7/19/16.
  */
object ProxyWordCreation {

  def udfWordCreation(topDomains : Broadcast[Set[String]],
                      agentCounts : Broadcast[Map[String, Long]],
                      timeCuts: Array[Double],
                      entropyCuts: Array[Double],
                      agentCuts: Array[Double]) =
    udf((host: String, time: String, reqMethod: String, uri: String, contentType: String, userAgent: String, responseCode: String) =>
      ProxyWordCreation.proxyWord(host, time, reqMethod, uri, contentType, userAgent, responseCode, topDomains, agentCounts, timeCuts, entropyCuts, agentCuts))


  def proxyWord(proxyHost: String,
                time: String,
                reqMethod: String,
                uri: String,
                contentType: String,
                userAgent: String,
                responseCode: String,
                topDomains: Broadcast[Set[String]],
                agentCounts: Broadcast[Map[String, Long]],
                timeCuts: Array[Double],
                entropyCuts: Array[Double],
                agentCuts: Array[Double]): String = {

    List(topDomain(proxyHost, topDomains.value).toString(),
      Quantiles.bin(getTimeAsDouble(time), timeCuts).toString(),
      reqMethod,
      Quantiles.bin(Utilities.stringEntropy(uri), entropyCuts),
      // contentType FOR SOME REASON INCLUDING RAW CONTENT TYPE IN THE WORD CHOKES THE LDA RUN
      Quantiles.bin(agentCounts.value(userAgent), agentCuts),
      responseCode(0)).mkString("_")

  }


  def topDomain(proxyHost: String, topDomains: Set[String]): Int = {

    val domain = proxyHost.split("[.]")(0)

    if (domainBelongsToCustomer(domain)) {
      2
    } else if (topDomains.contains(domain)) {
      1
    } else {
      0
    }
  }

  def domainBelongsToCustomer(proxyHost: String) = proxyHost.contains("intel") // TBD paramterize this!

  def getTimeAsDouble(timeStr: String) = {
    val s = timeStr.split(":")
    val hours = s(0).toInt
    val minutes = s(1).toInt
    val seconds = s(2).toInt

    (3600 * hours + 60 * minutes + seconds).toDouble
  }
}
