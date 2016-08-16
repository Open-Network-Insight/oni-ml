
package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.opennetworkinsight.utilities.{Entropy, Quantiles}
import org.slf4j.Logger
import org.opennetworkinsight.dns.{DNSSchema => Schema}
import scala.io.Source

/**
  * Contains routines for scoring incoming netflow records from a DNS suspicious connections model.
  */
object DNSPostLDA {

  def dnsPostLDA(inputPath: String, resultsFilePath: String, threshold: Double, documentResults: Array[String],
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
      df = df.select(Schema.Timestamp, Schema.UnixTimestamp, Schema.FrameLength, Schema.ClientIP, Schema.QueryName,
        Schema.QueryClass, Schema.QueryType, Schema.QueryResponseCode)
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

    var data_with_subdomains = datagood.map(row => row ++ DNSWordCreation.extractSubdomain(country_codes, row(col(Schema.QueryName))))
    addcol(Schema.Domain)
    addcol(Schema.Subdomain)
    addcol(Schema.SubdomainLength)
    addcol(Schema.NumPeriods)

    data_with_subdomains = data_with_subdomains.map(data => data :+ Entropy.stringEntropy(data(col(Schema.Subdomain))).toString)
    addcol(Schema.SubdomainEntropy)

    logger.info("calculating time cuts ...")
    time_cuts = Quantiles.computeDeciles(data_with_subdomains.map(r => r(col(Schema.UnixTimestamp)).toDouble))
    logger.info(time_cuts.mkString(","))

    logger.info("calculating frame length cuts ...")
    frame_length_cuts = Quantiles.computeDeciles(data_with_subdomains.map(r => r(col(Schema.FrameLength)).toDouble))
    logger.info(frame_length_cuts.mkString(","))
    logger.info("calculating subdomain length cuts ...")
    subdomain_length_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col(Schema.SubdomainLength)).toDouble > 0).map(r => r(col(Schema.SubdomainLength)).toDouble))
    logger.info(subdomain_length_cuts.mkString(","))
    logger.info("calculating entropy cuts")
    entropy_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col(Schema.SubdomainEntropy)).toDouble > 0).map(r => r(col(Schema.SubdomainEntropy)).toDouble))
    logger.info(entropy_cuts.mkString(","))
    logger.info("calculating num periods cuts ...")
    numperiods_cuts = Quantiles.computeQuintiles(data_with_subdomains.filter(r => r(col(Schema.NumPeriods)).toDouble > 0).map(r => r(col(Schema.NumPeriods)).toDouble))
    logger.info(numperiods_cuts.mkString(","))

    var data = data_with_subdomains.map(line => line :+ {
      if (line(col(Schema.Domain)) == "intel") {
        "2"
      } else if (top_domains.value contains line(col(Schema.Domain))) {
        "1"
      } else "0"
    })
    addcol(Schema.TopDomain)

    logger.info("adding words")
    data = data.map(row => {
      val word = row(col(Schema.TopDomain)) + "_" + DNSWordCreation.binColumn(row(col(Schema.FrameLength)), frame_length_cuts) + "_" +
        DNSWordCreation.binColumn(row(col(Schema.UnixTimestamp)), time_cuts) + "_" +
        DNSWordCreation.binColumn(row(col(Schema.SubdomainLength)), subdomain_length_cuts) + "_" +
        DNSWordCreation.binColumn(row(col(Schema.SubdomainEntropy)), entropy_cuts) + "_" +
        DNSWordCreation.binColumn(row(col(Schema.NumPeriods)), numperiods_cuts) + "_" + row(col(Schema.QueryType)) + "_" + row(col(Schema.QueryResponseCode))
      row :+ word
    })
    addcol("word")

    logger.info("Computing conditional probability")

    val src_scored = data.map(row => {
      val topic_mix = topics.value.getOrElse(row(col(Schema.ClientIP)), Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).asInstanceOf[Array[Double]]
      val word_prob = words.value.getOrElse(row(col(Schema.Word)), Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).asInstanceOf[Array[Double]]
      var src_score = 0.0
      for (i <- 0 to 19) {
        src_score += topic_mix(i) * word_prob(i)
      }
      (src_score, row :+ src_score)
    })

    addcol(Schema.Score)

    logger.info("Persisting data")
    val scored = src_scored.filter(elem => elem._1 < threshold).sortByKey().map(row => row._2.mkString(","))
    scored.saveAsTextFile(resultsFilePath)

    logger.info("DNS Post LDA completed")
  }

}
