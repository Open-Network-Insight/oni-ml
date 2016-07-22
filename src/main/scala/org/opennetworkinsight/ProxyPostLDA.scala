package org.opennetworkinsight

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.io.Source

object ProxyPostLDA {
  def getColumnNames(input: Array[String], sep: Char = ','): scala.collection.mutable.Map[String, Int] = {
    val columns = scala.collection.mutable.Map[String, Int]()
    val header = input.zipWithIndex
    header.foreach(tuple => columns(tuple._1) = tuple._2)
    columns
  }

  def run() = {

    val logger = LoggerFactory.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    logger.info("Proxy post LDA starts")

    val conf = new SparkConf().setAppName("ONI ML: proxy post lda")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val inputPaths = System.getenv("PROXY_PATH")
    val topicMixFile = System.getenv("HPATH") + "/doc_results.csv"
    val pwordFile = System.getenv("HPATH") + "/word_results.csv"
    val scoredOutputFile = System.getenv("HPATH") + "/scored"
    val threshold: Double = System.getenv("TOL").toDouble

    val topics_lines = sc.textFile(topicMixFile)
    val words_lines = sc.textFile(pwordFile)

    val l_topics = topics_lines.map(line => {
      val ip = line.split(",")(0)
      val topicProbs = line.split(",")(1).split(' ').map(_.toDouble)
      (ip, topicProbs)
    }).map({case (ip, topicProbs) => ip -> topicProbs }).collectAsMap()

    val topics = sc.broadcast(l_topics)

    val l_words  = words_lines.map(line => {
      val word = line.split(",")(0)
      val probPerTopic = line.split(",")(1).split(' ').map(_.toDouble)
      (word, probPerTopic)
    }).map({case (word, probPerTopic)  => word -> probPerTopic}).collectAsMap()

    val words = sc.broadcast(l_words)


    logger.info("Proxy post LDA starts")

    var df_cols = new Array[String](0)

    val multidata = {
      var df = sqlContext.parquetFile(inputPaths.split(",")(0)).filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null")
      val files = inputPaths.split(",")
      for ((file, index) <- files.zipWithIndex) {
        if (index > 1) {
          df = df.unionAll(sqlContext.parquetFile(file).filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null"))
        }
      }
      df = df.select("proxy_date",
        "proxy_time",
        "proxy_clientip",
        "proxy_host",
        "proxy_reqmethod",
        "proxy_useragent",
        "proxy_resconttype",
        "proxy_duration",
        "proxy_username",
        "proxy_webcat",
        "proxy_referer",
        "proxy_respcode",
        "proxy_uriport",
        "proxy_uripath",
        "proxy_uriquery",
        "proxy_serverip",
        "proxy_scbytes",
        "proxy_csbytes",
        "proxy_fulluri")

      df_cols = df.columns
      val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
      tempRDD
    }

    val col = getColumnNames(df_cols)

    def addcol(colname: String) = if (!col.keySet.exists(_ == colname)) {
      col(colname) = col.values.max + 1
    }

    val rawdata :org.apache.spark.rdd.RDD[String] =  multidata
    var data = rawdata.map(line => line.split(",")).filter(line => (line.length == df_cols.length))

    logger.info("Adding words")
    data = data.map(row => {
      row :+ ProxyWordCreation.proxyWord(row(col("proxy_host")), row(col("proxy_reqmethod")),
        row(col("proxy_respcode")) ,row(col("proxy_fulluri")))
    })
    addcol("word")

    logger.info("Computing conditional probability")

    val src_scored = data.map(row => {
      val topic_mix = topics.value.getOrElse(row(col("proxy_clientip")), Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05)).asInstanceOf[Array[Double]]
      val word_prob = words.value.getOrElse(row(col("word")), Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05)).asInstanceOf[Array[Double]]
      var src_score = 0.0
      for (i <- 0 to 19) {
        src_score += topic_mix(i) * word_prob(i)
      }
      (src_score, row :+ src_score)
    })

    addcol("score")

    logger.info("Persisting data")
    val scored = src_scored.sortByKey().map(row => row._2.mkString(","))
    scored.persist(StorageLevel.MEMORY_AND_DISK)
    scored.saveAsTextFile(scoredOutputFile)

    sc.stop()
    logger.info("proxy post LDA completed")
  }
}