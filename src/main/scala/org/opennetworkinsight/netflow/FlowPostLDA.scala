package org.opennetworkinsight.netflow

import breeze.linalg._
import org.apache.log4j.{Logger => apacheLogger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.opennetworkinsight.utilities.Quantiles
import org.opennetworkinsight.netflow.{FlowColumnIndex => indexOf}
import org.slf4j.Logger

/**
  * Contains routines for scoring incoming netflow records from a netflow suspicious connections model.
  */
object FlowPostLDA {

  def flowPostLDA(inputPath: String, resultsFilePath: String, threshold: Double, topK: Int, documentResults: Array[String],
                  wordResults: Array[String], sc: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    var ibyt_cuts = new Array[Double](10)
    var ipkt_cuts = new Array[Double](5)
    var time_cuts = new Array[Double](10)

    logger.info("loading machine learning results")
    val topics_lines = documentResults
    val words_lines = wordResults

    val l_topics = topics_lines.map(line => {
      val ip = line.split(",")(0)
      val text = line.split(",")(1)
      val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
      (ip, text_no_quote)
    }).map(elem => elem._1 -> elem._2).toMap

    val topics = sc.broadcast(l_topics)

    val l_words = words_lines.map(line => {
      val word = line.split(",")(0)
      val text = line.split(",")(1)
      val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
      (word, text_no_quote)
    }).map(elem => elem._1 -> elem._2).toMap

    val words = sc.broadcast(l_words)

    logger.info("loading data")
    val rawdata: RDD[String] = {
      val flowDataFrame = sqlContext.parquetFile(inputPath)
        .filter("trhour BETWEEN 0 AND 23 AND  " +
          "trminute BETWEEN 0 AND 59 AND  " +
          "trsec BETWEEN 0 AND 59")
        .select("treceived",
          "tryear",
          "trmonth",
          "trday",
          "trhour",
          "trminute",
          "trsec",
          "tdur",
          "sip",
          "dip",
          "sport",
          "dport",
          "proto",
          "flag",
          "fwd",
          "stos",
          "ipkt",
          "ibyt",
          "opkt",
          "obyt",
          "input",
          "output",
          "sas",
          "das",
          "dtos",
          "dir",
          "rip")
      flowDataFrame.map(_.mkString(","))
    }

    val data_with_time = rawdata.map(_.trim.split(",")).map(FlowWordCreation.addTime)

    logger.info("calculating time cuts ...")
    time_cuts = Quantiles.computeDeciles(data_with_time.map(row => row(indexOf.NUMTIME).toDouble))
    logger.info(time_cuts.mkString(","))
    logger.info("calculating byte cuts ...")
    ibyt_cuts = Quantiles.computeDeciles(data_with_time.map(row => row(indexOf.IBYT).toDouble))
    logger.info(ibyt_cuts.mkString(","))
    logger.info("calculating pkt cuts")
    ipkt_cuts = Quantiles.computeQuintiles(data_with_time.map(row => row(indexOf.IPKT).toDouble))
    logger.info(ipkt_cuts.mkString(","))

    val binned_data = data_with_time.map(row => FlowWordCreation.binIbytIpktTime(row, ibyt_cuts, ipkt_cuts, time_cuts))

    val data_with_words = binned_data.map(row => FlowWordCreation.adjustPort(row))

    val src_scored = data_with_words.map(row => {
      val topic_mix_1 = topics.value.getOrElse(row(indexOf.SOURCEIP), Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05)).asInstanceOf[Array[Double]]
      val word_prob_1 = words.value.getOrElse(row(indexOf.SOURCEWORD), Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05)).asInstanceOf[Array[Double]]
      val topic_mix_2 = topics.value.getOrElse(row(indexOf.DESTIP), Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05)).asInstanceOf[Array[Double]]
      val word_prob_2 = words.value.getOrElse(row(indexOf.DESTWORD), Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05)).asInstanceOf[Array[Double]]
      var src_score = 0.0
      var dest_score = 0.0
      for (i <- 0 to 19) {
        src_score += topic_mix_1(i) * word_prob_1(i)
        dest_score += topic_mix_2(i) * word_prob_2(i)
      }
      (min(src_score, dest_score), row :+ src_score :+ dest_score)
    })



    val filtered = src_scored.filter(elem => elem._1 < threshold)

    val count = filtered.count

    val takeCount  = if (topK == -1 || count < topK) {
      count.toInt
    } else {
      topK
    }

    class DataOrdering() extends Ordering[(Double,Array[Any])] {
      def compare(p1: (Double, Array[Any]), p2: (Double, Array[Any]))    = p1._1.compare(p2._1)
    }

    implicit val ordering = new DataOrdering()

    val top : Array[(Double,Array[Any])] = filtered.takeOrdered(takeCount)

    val outputRDD = sc.parallelize(top).sortBy(_._1).map(_._2.mkString("\t"))

    outputRDD.saveAsTextFile(resultsFilePath)

    logger.info("Flow post LDA completed")

  }
}