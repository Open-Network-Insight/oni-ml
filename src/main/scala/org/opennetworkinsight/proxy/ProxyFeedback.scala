package org.opennetworkinsight.proxy

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import scala.io.Source

import org.opennetworkinsight.proxy.ProxySchema._

object ProxyFeedback {

  def loadFeedbackDF(feedbackFile: String,
                     duplicationFactor: Int,
                     sc: SparkContext,
                     sqlContext: SQLContext): DataFrame = {


    val feedbackSchema = StructType(
      List(StructField(Date, StringType, true),
        StructField(Time, StringType, true),
        StructField(ClientIP, StringType, true),
        StructField(Host, StringType, true),
        StructField(ReqMethod, StringType, true),
        StructField(UserAgent, StringType, true),
        StructField(ResponseContentType, StringType, true),
        StructField(RespCode, StringType, true),
        StructField(FullURI, StringType, true)))

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

      sqlContext.createDataFrame(feedback.map(_.split("\t"))
        .filter(row => row(fullURISeverityIndex).trim.toInt == 3)
        .map(row => Row.fromSeq(List(row(dateIndex),
          row(timeIndex),
          row(clientIpIndex),
          row(hostIndex),
          row(reqMethodIndex),
          row(userAgentIndex),
          row(resContTypeIndex),
          row(respCodeIndex),
          row(fullURIIndex))))
        .flatMap(row => List.fill(duplicationFactor)(row)), feedbackSchema)
        .select(Date, Time, ClientIP, Host, ReqMethod, UserAgent, ResponseContentType, RespCode, FullURI)
    } else {
      sqlContext.createDataFrame(sc.emptyRDD[Row], feedbackSchema)
    }
  }
}
