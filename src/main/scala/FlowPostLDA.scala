package main.scala

import main.scala.FlowTransformation
import main.scala.{FlowColumnIndex => indexOf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg._


object FlowPostLDA {
    def run() = {

        val conf = new SparkConf().setAppName("ONI ML: flow post lda")
        val sc = new SparkContext(conf)

        val file = System.getenv("FLOW_PATH")
        val topic_mix_file = System.getenv("HPATH") + "/doc_results.csv"
        val pword_file = System.getenv("HPATH") + "/word_results.csv"
        val scored_output_file = System.getenv("HPATH") + "/scored"
        val threshold: Double = System.getenv("TOL").toDouble

        var ibyt_cuts = new Array[Double](10)
        var ipkt_cuts = new Array[Double](5)
        var time_cuts = new Array[Double](10)

        println("loading machine learning results")
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

        println("loading data")
        val rawdata = sc.textFile(file)

        val datanoheader = FlowTransformation.removeHeader(rawdata)
        val datagood = datanoheader.filter(line => line.split(",").length == 27)

        val data_with_time = datagood.map(_.trim.split(",")).map(FlowTransformation.addTime)

        println("calculating time cuts ...")
        time_cuts = Quantiles.distributedQuantilesQuant(Quantiles.computeEcdf(data_with_time.map(row => row(indexOf.NUMTIME).toDouble)))
        println(time_cuts.mkString(","))
        println("calculating byte cuts ...")
        ibyt_cuts = Quantiles.distributedQuantilesQuant(Quantiles.computeEcdf(data_with_time.map(row => row(indexOf.IBYT).toDouble)))
        println(ibyt_cuts.mkString(","))
        println("calculating pkt cuts")
        ipkt_cuts = Quantiles.distributedQuantilesQuint(Quantiles.computeEcdf(data_with_time.map(row => row(indexOf.IPKT).toDouble)))
        println(ipkt_cuts.mkString(","))

        val binned_data = data_with_time.map(row => FlowTransformation.binIbytIpktTime(row, ibyt_cuts, ipkt_cuts, time_cuts))

        val data_with_words = binned_data.map(row => FlowTransformation.adjustPort(row))

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

        val scored = src_scored.filter(elem => elem._1 < threshold).sortByKey().map(row => row._2.mkString(","))

        scored.persist(StorageLevel.MEMORY_AND_DISK)
        scored.saveAsTextFile(scored_output_file)

        sc.stop()
    }

}