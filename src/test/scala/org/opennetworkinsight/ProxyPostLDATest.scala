package org.opennetworkinsight

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.testutils.TestingSparkContextFlatSpec
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.rdd.RDD
import org.opennetworkinsight.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.Logger


class ProxyPostLDATest extends TestingSparkContextFlatSpec with Matchers{

  val schema = StructType(List(StructField("proxy_date", StringType), StructField("proxy_time", StringType),
    StructField("proxy_clientip", StringType), StructField("proxy_host", StringType), StructField("proxy_reqmethod", StringType),
    StructField("proxy_useragent", StringType), StructField("proxy_resconttype", StringType),
    StructField("proxy_respcode", StringType),  StructField("proxy_fulluri", StringType)))

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

  val ipWordCount1 = "127.0.0.1,youneedacar.com|GET|229|5,2"
  val ipWordCount2 = "127.0.0.1,youneedacar.com|GET|229|2,1"
  val ipWordCount3 = "192.168.1.1,youneedacar.com|GET|229|2,1"

  val ip1 = "127.0.0.1"
  val ip2 = "192.168.1.1"

  val word1 = "youneedacar.com|GET|229|5"
  val word2  = "youneedacar.com|GET|229|2"

  val ipToTopicMix = Map(ip1->Array(0.75d,0.25d, 0.0d), ip2->Array(0.0d, 0.25d, 0.75d))
  val topicCount = 3
  val wordToPerTopicProb = Map(word1->Array(0d, 1d, 0d), word2->Array(1/3d, 0, 2/3d))

  val word1ScoreAtIp1 = 0.25d
  val word2ScoreAtIp1 = 0.25d
  val word2ScoreAtIp2 = 0.5d

  val expectedScored1 =
    ("1973-12-31", "12:33:02", "127.0.0.1", "youneedacar.com", "GET",
      "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "image/gif", "229",
      "www.youneedacar.com/2015/03/01/todaysdeals.html?_r=1", word1, word1ScoreAtIp1)

  val expectedScored2 =
    ("1973-12-31", "02:33:22", "127.0.0.1", "youneedacar.com", "GET",
      "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "image/gif", "229",
      "www.youneedacar.com/2015/03/01/todaysdeals.html?_r=1" , word1, word1ScoreAtIp1)

  val expectedScored3 =
    ("1973-12-31", "22:33:22", "127.0.0.1", "youneedacar.com", "GET",
      "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "text/html", "229",
      "www.youneedacar.com/home.html", word2, word2ScoreAtIp1)

  val expectedScored4 =
    ("1973-12-31", "22:33:22", "192.168.1.1", "youneedacar.com", "GET",
      "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0", "text/html", "229",
      "www.youneedacar.com/home.html", word2, word2ScoreAtIp2)


  ignore  should "work" in {



    val sampleDF = sqlContext.createDataFrame(sparkContext.parallelize(sampleData.map(Row.fromSeq(_))), schema)

    val outRows = ProxyPostLDA.score(sparkContext, sampleDF, topicCount, ipToTopicMix, wordToPerTopicProb).rdd.collect()
        .map({case Row(date: String, time: String, ip: String, url: String, method: String, agent: String, content: String, rc: String, uri: String, word: String, score: Double ) =>
          (date, time, ip, url, method, agent, content, rc, uri, word, score)})
    outRows.length shouldBe 4

    outRows should contain (expectedScored1)
    outRows should contain (expectedScored2)
    outRows should contain (expectedScored3)
    outRows should contain (expectedScored4)

  }
}

