package org.opennetworkinsight.proxy

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.io.Source

import org.opennetworkinsight.proxy.ProxySchema._

object ProxyFeedback {


  case class Feedback(date: String,
                      timeStamp: String,
                      clientIP: String,
                      host: String,
                      reqMethod: String,
                      userAgent: String,
                      responseContentType: String,
                      respCode: String,
                      fullURI: String) extends Serializable


  def loadFeedbackDF(feedbackFile: String, duplicationFactor: Int, sc: SparkContext, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    val feedbackFileExists = new java.io.File(feedbackFile).exists
    if (feedbackFileExists) {

      val dateIndex = 0
      val timeIndex = 1
      val clientIpIndex = 2
      val hostIndex = 3
      val reqMethodIndex = 4
      val userAgentIndex = 5
      val resContTypeIndex = 6
      val respCodeIndex = 11
      val fullURIIndex = 18

      val fullURISeverityIndex = 22

      val lines = Source.fromFile(feedbackFile).getLines().toArray.drop(1)
      val feedback: RDD[String] = sc.parallelize(lines)
      feedback.map(_.split(","))
        .filter(row => row(fullURISeverityIndex).trim.toInt == 3)
        .map(row => Feedback(row(dateIndex),
          row(timeIndex),
          row(clientIpIndex),
          row(hostIndex),
          row(reqMethodIndex),
          row(userAgentIndex),
          row(resContTypeIndex),
          row(respCodeIndex),
          row(fullURIIndex)))
        .flatMap(row => List.fill(duplicationFactor)(row))
        .toDF()
        .select(Date, Time, ClientIP, Host, ReqMethod, UserAgent, ResponseContentType, RespCode)
  } else {
    null
    }
  }
}
