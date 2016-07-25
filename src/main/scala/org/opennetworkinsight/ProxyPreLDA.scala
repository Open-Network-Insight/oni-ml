package org.opennetworkinsight


import org.apache.log4j.{Level, Logger => apacheLogger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
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

  def proxyPreLDA(inputPath: String, feedbackFile: String, duplicationFactor: Int,
                  sc: SparkContext, sqlContext: SQLContext, logger: Logger): RDD[String] = {

    logger.info("Proxy pre LDA starts")

    var dataframeColumns = new Array[String](0)

    val feedbackFileExists = new java.io.File(feedbackFile).exists

    val multidata = {
      var df = sqlContext.parquetFile(inputPath.split(",")(0)).filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null")
      val files = inputPath.split(",")
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
        "proxy_respcode",
        "proxy_fulluri")
      dataframeColumns = df.columns
      val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
      tempRDD
    }

    val col = getColumnNames(dataframeColumns)

    def addcol(colname: String) = if (!col.keySet.contains(colname)) {
      col(colname) = col.values.max + 1
    }
    if (feedbackFile != "None") {
      addcol("feedback")
    }

    val rawdata :org.apache.spark.rdd.RDD[String] = {
      if (!feedbackFileExists) { multidata
      }else {
        multidata
      }
    }

    var data = rawdata.map(line => line.split(",")).filter(line => line.length == dataframeColumns.length).map(line => {
      if (feedbackFile != "None") {
        line :+ "None"
      } else {
        line
      }
    })

    logger.info("Adding words")
    data = data.map(row => {
      row :+ ProxyWordCreation.proxyWord(row(col("proxy_host")),
        row(col("proxy_reqmethod")),
        row(col("proxy_respcode")),
        row(col("proxy_fulluri")))
    })
    addcol("word")

    val wc = data.map(row => ((row(col("proxy_clientip")) , row(col("word"))), 1)).reduceByKey(_ + _).map({case ((ip, word), count) => List(ip, word, count).mkString(",")})

    logger.info("proxy pre LDA completed")

    wc
  }
}

