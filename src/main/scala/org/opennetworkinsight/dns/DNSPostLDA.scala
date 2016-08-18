
package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.opennetworkinsight.utilities.{Entropy, Quantiles}
import org.slf4j.Logger

import scala.io.Source

/**
  * Contains routines for scoring incoming netflow records from a DNS suspicious connections model.
  */
object DNSPostLDA {

  def dnsPostLDA(inputPath: String, resultsFilePath: String, threshold: Double, topK: Int, documentResults: Array[String],
                 wordResults: Array[String], sc: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("DNS post LDA starts")

    var time_cuts = new Array[Double](10)
    var frame_length_cuts = new Array[Double](10)
    var subdomain_length_cuts = new Array[Double](5)
    var numperiods_cuts = new Array[Double](5)
    var entropy_cuts = new Array[Double](5)
    var df_cols = new Array[String](0)

    val l_top_domains = Source.fromFile("top-1m.csv").getLines.map(line => {
      val parts = line.split(",")
      parts(1).split("[.]")(0)
    }).toSet
    val top_domains = sc.broadcast(l_top_domains)

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

    val multidata = {
      var df = sqlContext.parquetFile(inputPath.split(",")(0)).filter("frame_len is not null and unix_tstamp is not null")
      val files = inputPath.split(",")
      for ((file, index) <- files.zipWithIndex) {
        if (index > 1) {
          df = df.unionAll(sqlContext.parquetFile(file).filter("frame_len is not null and unix_tstamp is not null"))
        }
      }
      df = df.select("frame_time", "unix_tstamp", "frame_len", "ip_dst", "dns_qry_name", "dns_qry_class", "dns_qry_type", "dns_qry_rcode")
      df_cols = df.columns
      val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
      tempRDD
    }
    val rawdata: org.apache.spark.rdd.RDD[String] = {
      multidata
    }

    val col = DNSWordCreation.getColumnNames(df_cols)

    def addcol(colname: String) = if (!col.keySet.exists(_ == colname)) {
      col(colname) = col.values.max + 1
    }

    val datagood = rawdata.map(line => line.split(",")).filter(line => (line.length == df_cols.length))

    val country_codes = sc.broadcast(DNSWordCreation.l_country_codes)

    logger.info("Computing subdomain info")

    var data_with_subdomains = datagood.map(row => row ++ DNSWordCreation.extractSubdomain(country_codes, row(col("dns_qry_name"))))
    addcol("domain")
    addcol("subdomain")
    addcol("subdomain.length")
    addcol("num.periods")

    data_with_subdomains = data_with_subdomains.map(data => data :+ Entropy.stringEntropy(data(col("subdomain"))).toString)
    addcol("subdomain.entropy")

    logger.info("calculating time cuts ...")
    time_cuts = Quantiles.computeDeciles(data_with_subdomains.map(r => r(col("unix_tstamp")).toDouble))
    logger.info(time_cuts.mkString(","))

    logger.info("calculating frame length cuts ...")
    frame_length_cuts = Quantiles.computeDeciles(data_with_subdomains.map(r => r(col("frame_len")).toDouble))
    logger.info(frame_length_cuts.mkString(","))
    logger.info("calculating subdomain length cuts ...")
    subdomain_length_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col("subdomain.length")).toDouble > 0).map(r => r(col("subdomain.length")).toDouble))
    logger.info(subdomain_length_cuts.mkString(","))
    logger.info("calculating entropy cuts")
    entropy_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col("subdomain.entropy")).toDouble > 0).map(r => r(col("subdomain.entropy")).toDouble))
    logger.info(entropy_cuts.mkString(","))
    logger.info("calculating num periods cuts ...")
    numperiods_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col("num.periods")).toDouble > 0).map(r => r(col("num.periods")).toDouble))
    logger.info(numperiods_cuts.mkString(","))

    var data = data_with_subdomains.map(line => line :+ {
      if (line(col("domain")) == "intel") {
        "2"
      } else if (top_domains.value contains line(col("domain"))) {
        "1"
      } else "0"
    })
    addcol("top_domain")

    logger.info("adding words")
    data = data.map(row => {
      val word = row(col("top_domain")) + "_" + DNSWordCreation.binColumn(row(col("frame_len")), frame_length_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("unix_tstamp")), time_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("subdomain.length")), subdomain_length_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("subdomain.entropy")), entropy_cuts) + "_" +
        DNSWordCreation.binColumn(row(col("num.periods")), numperiods_cuts) + "_" + row(col("dns_qry_type")) + "_" + row(col("dns_qry_rcode"))
      row :+ word
    })
    addcol("word")

    logger.info("Computing conditional probability")

    val src_scored = data.map(row => {
      val topic_mix = topics.value.getOrElse(row(col("ip_dst")), Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).asInstanceOf[Array[Double]]
      val word_prob = words.value.getOrElse(row(col("word")), Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).asInstanceOf[Array[Double]]
      var src_score = 0.0
      for (i <- 0 to 19) {
        src_score += topic_mix(i) * word_prob(i)
      }
      (src_score, row :+ src_score)
    })

    addcol("score")

    logger.info("Persisting data")


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

    logger.info("DNS Post LDA completed")
  }

}
