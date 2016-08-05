package org.opennetworkinsight

import org.apache.spark.rdd.RDD
import org.opennetworkinsight.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


class ProxyPreLDATest extends TestingSparkContextFlatSpec with Matchers{

  val schema =  List()

  val sampleData = List(
   List("1973-12-31", "12:33:02", "127.0.0.1", "youneedacar.com", "GET",
     "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "image/gif", "229",
     "www.youneedacar.com/2015/03/01/todaysdeals.html?_r=1"),
   List("1973-12-31", "02:33:22", "127.0.0.1", "youneedacar.com", "GET",
     "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "image/gif", "229",
     "www.youneedacar.com/2015/03/01/todaysdeals.html?_r=1"),
    List("1973-12-31", "22:33:22", "127.0.0.1", "youneedacar.com", "GET",
      "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "text/html", "229",
      "www.youneedacar.com/home.html"),
    List("1973-12-31", "22:33:22", "192.168.1.1", "youneedacar.com", "GET",
      "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "text/html", "229",
      "www.youneedacar.com/home.html")
  )

  val expectedOne = "127.0.0.1,youneedacar.com|GET|229|5,2"
  val expectedTwo = "127.0.0.1,youneedacar.com|GET|229|2,1"
  val expectedThree = "192.168.1.1,youneedacar.com|GET|229|2,1"


  val topDomains  =  Set("google")
  val agentCounts = Map("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0"->4l)

  val timeCuts : Array[Double] = Array(8640, 17280, 25920, 34560, 43200, 51840, 60480, 69120, 77760, 86400)
  val entropyCuts : Array[Double] = Array(0.2, 0.4, 0.6, 0.8, 1.0)

  val agentCuts : Array[Double] = Array(0, 0, 0, 0, 4)
  ignore should "work" in {

    val schema = StructType(List(StructField("proxy_date", StringType), StructField("proxy_time", StringType),
      StructField("proxy_clientip", StringType), StructField("proxy_host", StringType), StructField("proxy_reqmethod", StringType),
      StructField("proxy_useragent", StringType), StructField("proxy_resconttype", StringType),
      StructField("proxy_respcode", StringType),  StructField("proxy_fulluri", StringType)))

    val sampleDF = sqlContext.createDataFrame(sparkContext.parallelize(sampleData.map(Row.fromSeq(_))), schema)

    val sampleTopDomains = sparkContext.broadcast(topDomains)
    val sampleAgentCounts = sparkContext.broadcast(agentCounts)
    val rddOut = ProxyPreLDA.ipWordCountFromDF(sampleDF, sampleTopDomains, sampleAgentCounts, timeCuts, entropyCuts, agentCuts)

    val out = rddOut.collect()

    out.length shouldBe 3
    out should contain (expectedOne)
    out should contain (expectedTwo)
    out should contain (expectedThree)

  }
}
