package org.opennetworkinsight


import org.apache.log4j.{Level, Logger => apacheLogger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming proxy records.
  */

object ProxyPreLDA {

  def getColumnNames(input: Array[String], sep: Char = ','): scala.collection.mutable.Map[String, Int] = {
    val columns = scala.collection.mutable.Map[String, Int]()
    val header = input.zipWithIndex
    header.foreach(tuple => columns(tuple._1) = tuple._2)
    columns
  }

  def run() = {

    val logger = LoggerFactory.getLogger(this.getClass)
    apacheLogger.getLogger("org").setLevel(Level.OFF)
    apacheLogger.getLogger("akka").setLevel(Level.OFF)

    logger.info("Proxy pre LDA starts")

    val conf = new SparkConf().setAppName("ONI ML: proxy pre lda")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inputPaths = System.getenv("PROXY_PATH")
    val feedback_file = System.getenv("LPATH") + "/proxy_scores.csv"
    val duplication_factor = System.getenv("DUPFACTOR").toInt
    val outputfile = System.getenv("HPATH") + "/proxy_counts"


    var dataframeColumns = new Array[String](0)

    val scoredFileExists = new java.io.File(feedback_file).exists


    val multidata = {
      var df = sqlContext.parquetFile(inputPaths.split(",")(0)).filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null")
      val files = inputPaths.split(",")
      for ((file, index) <- files.zipWithIndex) {
        if (index > 1) {
          df = df.unionAll(sqlContext.parquetFile(file).filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null"))
        }
      }
      df = df.select("proxy_date", "proxy_time", "proxy_clientip", "proxy_host", "proxy_reqmethod",
        "proxy_useragent", "proxy_resconttype", "proxy_respcode", "proxy_fulluri")
      dataframeColumns = df.columns
      val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
      tempRDD
    }

    println("Read source data")


    val col = getColumnNames(dataframeColumns)

    def addcol(colname: String) = if (!col.keySet.exists(_ == colname)) {
      col(colname) = col.values.max + 1
    }
    if (feedback_file != "None") {
      addcol("feedback")
    }


    print("Read source data")
    val rawdata :org.apache.spark.rdd.RDD[String] = {
      if (!scoredFileExists) { multidata
      }else {
        multidata
      }
    }

    var data = rawdata.map(line => line.split(",")).filter(line => (line.length == dataframeColumns.length)).map(line => {
      if (feedback_file != "None") {
        line :+ "None"
      } else {
        line
      }
    })

    addcol("host")
    addcol("req_method")
    addcol("response_code")
    addcol("URI_length")

    logger.info("Adding words")
    data = data.map(row => {
      row :+ row(col("proxy_host")) + "_" + row(col("proxy_reqmethod")) + "_" + row(col("proxy_respcode")) + "_" +
        row(col("proxy_fulluri")).split('/').length.toString()
    })
    addcol("word")


    println("PROXY:  raw entry count: " + data.count())
    logger.info("Persisting data")
    val wc = data.map(row => (row(col("proxy_clientip")) + " " + row(col("word")), 1)).reduceByKey(_ + _).map(row => (row._1.split(" ")(0) + "," + row._1.split(" ")(1).toString + "," + row._2).mkString)
    wc.persist(StorageLevel.MEMORY_AND_DISK)
    wc.saveAsTextFile(outputfile)
    println("PROXY:  distinct ip  count: " + wc.count())

    sc.stop()
    logger.info("proxy pre LDA completed")
  }
}

