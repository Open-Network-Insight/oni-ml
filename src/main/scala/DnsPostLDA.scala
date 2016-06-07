
package main.scala

import main.scala.DNSTransformation
import scala.io.Source
import org.apache.log4j.{Level, Logger => apacheLogger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.{LoggerFactory, Logger}

object DnsPostLDA {

    def run() = {

        val logger = LoggerFactory.getLogger(this.getClass)
        apacheLogger.getLogger("org").setLevel(Level.OFF)
        apacheLogger.getLogger("akka").setLevel(Level.OFF)

        logger.info("DNS post LDA starts")

        val conf = new SparkConf().setAppName("ONI ML: dns post lda")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val file_list = System.getenv("DNS_PATH")
        val topic_mix_file = System.getenv("HPATH") + "/doc_results.csv"
        val pword_file = System.getenv("HPATH") + "/word_results.csv"
        val scored_output_file = System.getenv("HPATH") + "/scored"
        val threshold: Double = System.getenv("TOL").toDouble

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

        val topics_lines = sc.textFile(topic_mix_file)
        val words_lines = sc.textFile(pword_file)

        val l_topics = topics_lines.map(line => {
            val ip = line.split(",")(0)
            val text = line.split(",")(1)
            val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
            (ip, text_no_quote)
        }).map(elem => elem._1 -> elem._2).collectAsMap()

        val topics = sc.broadcast(l_topics)

        val l_words = words_lines.map(line => {
            val word = line.split(",")(0)
            val text = line.split(",")(1)
            val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
            (word, text_no_quote)
        }).map(elem => elem._1 -> elem._2).collectAsMap()

        val words = sc.broadcast(l_words)

        val multidata = {
            var df = sqlContext.parquetFile(file_list.split(",")(0)).filter("frame_len is not null and unix_tstamp is not null")
            val files = file_list.split(",")
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

        val col = DNSTransformation.getColumnNames(df_cols)

        def addcol(colname: String) = if (!col.keySet.exists(_ == colname)) {
            col(colname) = col.values.max + 1
        }

        val datagood = rawdata.map(line => line.split(",")).filter(line => (line.length == df_cols.length))

        val country_codes = sc.broadcast(DNSTransformation.l_country_codes)

        logger.info("Computing subdomain info")

        var data_with_subdomains = datagood.map(row => row ++ DNSTransformation.extractSubdomain(country_codes,row(col("dns_qry_name"))))
        addcol("domain")
        addcol("subdomain")
        addcol("subdomain.length")
        addcol("num.periods")

        data_with_subdomains = data_with_subdomains.map(data => data :+ DNSTransformation.entropy(data(col("subdomain"))).toString)
        addcol("subdomain.entropy")

        logger.info("calculating time cuts ...")
        time_cuts = Quantiles.distributedQuantilesQuant(Quantiles.computeEcdf(data_with_subdomains.map(r => r(col("unix_tstamp")).toDouble)))
        logger.info(time_cuts.mkString(","))

        logger.info("calculating frame length cuts ...")
        frame_length_cuts = Quantiles.distributedQuantilesQuant(Quantiles.computeEcdf(data_with_subdomains.map(r => r(col("frame_len")).toDouble)))
        logger.info(frame_length_cuts.mkString(","))
        logger.info("calculating subdomain length cuts ...")
        subdomain_length_cuts = Quantiles.distributedQuantilesQuint(Quantiles.computeEcdf(data_with_subdomains.filter(r => r(col("subdomain.length")).toDouble > 0).map(r => r(col("subdomain.length")).toDouble)))
        logger.info(subdomain_length_cuts.mkString(","))
        logger.info("calculating entropy cuts")
        entropy_cuts = Quantiles.distributedQuantilesQuint(Quantiles.computeEcdf(data_with_subdomains.filter(r => r(col("subdomain.entropy")).toDouble > 0).map(r => r(col("subdomain.entropy")).toDouble)))
        logger.info(entropy_cuts.mkString(","))
        logger.info("calculating num periods cuts ...")
        numperiods_cuts = Quantiles.distributedQuantilesQuint(Quantiles.computeEcdf(data_with_subdomains.filter(r => r(col("num.periods")).toDouble > 0).map(r => r(col("num.periods")).toDouble)))
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
            val word = row(col("top_domain")) + "_" + DNSTransformation.binColumn(row(col("frame_len")), frame_length_cuts) + "_" +
              DNSTransformation.binColumn(row(col("unix_tstamp")), time_cuts) + "_" +
              DNSTransformation.binColumn(row(col("subdomain.length")), subdomain_length_cuts) + "_" +
              DNSTransformation.binColumn(row(col("subdomain.entropy")), entropy_cuts) + "_" +
              DNSTransformation.binColumn(row(col("num.periods")), numperiods_cuts) + "_" + row(col("dns_qry_type")) + "_" + row(col("dns_qry_rcode"))
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
        val scored = src_scored.filter(elem => elem._1 < threshold).sortByKey().map(row => row._2.mkString(","))
        scored.persist(StorageLevel.MEMORY_AND_DISK)
        scored.saveAsTextFile(scored_output_file)

        sc.stop()
        logger.info("DNS pre LDA completed")
    }
}