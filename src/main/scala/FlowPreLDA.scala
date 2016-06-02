
package main.scala



import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.math._
import scala.io.Source

object FlowPreLDA {

  def run() = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("ONI ML: flow pre lda")
    val sc = new SparkContext(conf)


    def removeHeader(input: org.apache.spark.rdd.RDD[String]) = {
      val header = input.first
      val output = input.filter(line => !(line == header))
      output
    }


    // Load and parse the data


    //0case class schema(time: String,
    //1 year: Double,
    //2 month: Double,
    //3 day: Double,
    //4 hour: Double,
    //5 minute: Double,
    //6 second: Double,
    //7 tdur: Double,
    //8 sip: String,
    //9 dip: String,
    //10 sport: Double,
    //11 dport: Double,
    //12 proto: String,
    //13 flag: String,
    //14 fwd: Double,
    //15 stos: Double,
    //16 ipkt: Double,
    //17 ibyt: Double,
    //18 opkt: Double,
    //19 obyt: Double,
    //20 input: Double,
    //21 output: Double,
    //22 sas: String,
    //23 das: Sring,
    //24 dtos: String,
    //25 dir: String,
    //26 rip: String)

    //----------Inputs-------------

    val scoredFile = System.getenv("LPATH") + "/flow_scores.csv"
    val file = System.getenv("FLOW_PATH")

    val output_file = System.getenv("HPATH") + "/word_counts"
    val output_file_for_lda = System.getenv("HPATH") + "/lda_word_counts"


    println("scoredFile:  " + scoredFile)
    println("outputFile:  " + output_file)
    println("output_file_for_lda:  " + output_file_for_lda)

    val quant = Array(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
    val quint = Array(0, 0.2, 0.4, 0.6, 0.8)
    var ibyt_cuts = new Array[Double](10)
    var ipkt_cuts = new Array[Double](5)
    var time_cuts = new Array[Double](10)


    //-----------------------------



    def convert_feedback_row_to_flow_row(feedBackRow: String) = {

      val row = feedBackRow.split(',')
      // when we
      val sev_feedback_index = 0
      val tstart_feedback_index = 1
      val srcIP_feedback_index = 2
      val dstIP_feedback_index = 3
      val sport_feedback_index = 4
      val dport_feedback_index = 5
      val proto_feedback_index = 6
      val flag_feedback_index = 7
      val ipkt_feedback_index = 8
      val ibyt_feedback_index = 9
      val lda_score_feedback_index = 10
      val rank_feedback_index = 11
      val srcIpInternal_feedback_index = 12
      val destIpInternal_feedback_index = 13
      val srcGeo_feedback_index = 14
      val dstGeo_feedback_index = 15
      val srcDomain_feedback_index = 16
      val dstDomain_feedback_index = 17
      val gtiSrcRep_feedback_index = 18
      val gtiDstRep_feedback_index = 19
      val norseSrcRep_feedback_index = 20
      val norseDstRep_feedback_index = 21

      val srcIP: String = row(srcIP_feedback_index)
      val dstIP: String = row(dstIP_feedback_index)
      val sport: String = row(sport_feedback_index)
      val dport: String = row(dport_feedback_index)
      val tstart: String = row(tstart_feedback_index)
      val ipkts: String = row(ipkt_feedback_index)
      val ibyts: String = row(ibyt_feedback_index)


      // it is assumed that the format of the time object coming from the feedback is
      //                  YYYY-MM-DD HH:MM:SS
      //   for example:   2016-04-21 03:58:13
      val hourMinSecond: Array[String] = tstart.split(' ')(1).split(':') // todo: error handling if the line is malformed
      val hour = hourMinSecond(0)
      val min = hourMinSecond(1)
      val sec = hourMinSecond(2)


      val time_flow_index = 0
      val year_flow_index = 1
      val month_flow_index = 2
      val day_flow_index = 3
      val hour_flow_index = 4
      val minute_flow_index = 5
      val second_flow_index = 6
      val tdur_flow_index = 7
      val sip_flow_index = 8
      val dip_flow_index = 9
      val sport_flow_index = 10
      val dport_flow_index = 11
      val proto_flow_index = 12
      val flag_flow_index = 13
      val fwd_flow_index = 14
      val stos_flow_index = 15
      val ipkt_flow_index = 16
      val ibyt_flow_index = 17
      val opkt_flow_index = 18
      val obyt_flow_index = 19
      val input_flow_index = 20
      val output_flow_index = 21
      val sas_flow_index = 22
      val das_flow_index = 23
      val dtos_flow_index = 24
      val dir_flow_index = 25
      val rip_flow_index = 26


      val buf = new StringBuilder
      for (i <- 0 to 26) {
        if (i == hour_flow_index) {
          buf ++= hour
        } else if (i == minute_flow_index) {
          buf ++= min
        } else if (i == second_flow_index) {
          buf ++= sec
        } else if (i == ipkt_flow_index) {
          buf ++= ipkts
        } else if (i == ibyt_flow_index) {
          buf ++= ibyts
        } else if (i == sport_flow_index) {
          buf ++= sport
        } else if (i == dport_flow_index) {
          buf ++= dport
        } else if (i == sip_flow_index) {
          buf ++= srcIP
        } else if (i == dip_flow_index) {
          buf ++= dstIP
        } else {
          buf ++= "##"
        }
        if (i < 26) {
          buf + ','
        }
      }
      buf.toString()
    }

    println("Trying to read file:  " + file)
    val rawdata: RDD[String] = sc.textFile(file)
    val datanoheader: RDD[String] = removeHeader(rawdata)


    val scoredFileExists = new java.io.File(scoredFile).exists

    val scoredData: Array[String] = if (scoredFileExists) {
      val duplicationFactor = System.getenv("DUPFACTOR").toInt

      val rowsToDuplicate = Source.fromFile(scoredFile).getLines().toArray.drop(1).filter(l => (l.split(',').length == 22) && l.split(',')(0).toInt == 3)
      println("User feedback read from: " + scoredFile + ". "
        + rowsToDuplicate.length + " many connections flagged nonthreatening.")
      println("Duplication factor: " + duplicationFactor)
      rowsToDuplicate.map(convert_feedback_row_to_flow_row(_)).flatMap(List.fill(duplicationFactor)(_))
    } else {
      Array[String]()
    }


    val totalData: RDD[String] = datanoheader.union(sc.parallelize(scoredData))

    val datagood: RDD[String] = totalData.filter(line => line.split(",").length == 27)

    def add_time(row: Array[String]): Array[String] = {
      val num_time = row(4).toDouble + row(5).toDouble / 60 + row(6).toDouble / 3600
      row.clone :+ num_time.toString
    }

    val data_with_time = datagood.map(_.trim.split(',')).map(add_time)



    println("calculating time cuts ...")
    time_cuts = Quantiles.distributed_quantiles(quant, Quantiles.compute_ecdf(data_with_time.map(row => row(27).toDouble)))
    println(time_cuts.mkString(","))
    println("calculating byte cuts ...")
    ibyt_cuts = Quantiles.distributed_quantiles(quant, Quantiles.compute_ecdf(data_with_time.map(row => row(17).toDouble)))
    println(ibyt_cuts.mkString(","))
    println("calculating pkt cuts")
    ipkt_cuts = Quantiles.distributed_quantiles(quint, Quantiles.compute_ecdf(data_with_time.map(row => row(16).toDouble)))
    println(ipkt_cuts.mkString(","))

    def bin_ibyt_ipkt_time(row: Array[String],
                           ibyt_cuts: Array[Double],
                           ipkt_cuts: Array[Double],
                           time_cuts: Array[Double]) = {
      val time = row(27).toDouble
      val ibyt = row(17).toDouble
      val ipkt = row(16).toDouble
      var time_bin = 0
      var ibyt_bin = 0
      var ipkt_bin = 0
      for (cut <- ibyt_cuts) {
        if (ibyt > cut) {
          ibyt_bin = ibyt_bin + 1
        }
      }
      for (cut <- ipkt_cuts) {
        if (ipkt > cut) {
          ipkt_bin = ipkt_bin + 1
        }
      }
      for (cut <- time_cuts) {
        if (time > cut) {
          time_bin = time_bin + 1
        }
      }
      row.clone :+ ibyt_bin.toString :+ ipkt_bin.toString :+ time_bin.toString
    }

    //s1 = bin_ibyt_ipkt_time(row = s1, ibyt_cuts, ipkt_cuts, time_cuts)
    val binned_data = data_with_time.map(row => bin_ibyt_ipkt_time(row, ibyt_cuts, ipkt_cuts, time_cuts))

    def adjust_port(row: Array[String]) = {
      var word_port = 111111.0
      val sip = row(8)
      val dip = row(9)
      val dport = row(10).toDouble
      val sport = row(11).toDouble
      val ipkt_bin = row(29).toDouble
      val ibyt_bin = row(28).toDouble
      val time_bin = row(30).toDouble
      var p_case = 0.0

      var ip_pair = row(9) + " " + row(8)
      if (sip < dip & sip != 0) {
        ip_pair = row(8) + " " + row(9)
      }

      if ((dport <= 1024 | sport <= 1024) & (dport > 1024 | sport > 1024) & min(dport, sport) != 0) {
        p_case = 2
        word_port = min(dport, sport)
      } else if (dport > 1024 & sport > 1024) {
        p_case = 3
        word_port = 333333
      } else if (dport == 0 & sport != 0) {
        word_port = sport
        p_case = 4
      } else if (sport == 0 & dport != 0) {
        word_port = dport
        p_case = 4
      } else {
        p_case = 1
        if (min(dport, sport) == 0) {
          word_port = max(dport, sport)
        } else {
          word_port = 111111
        }
      }
      //val word = ipkt_bin -1 + (ibyt_bin-1)*10 + (time_bin - 1)*100 + word_port*1000
      val word = word_port.toString + "_" + time_bin.toString + "_" + ibyt_bin.toString + "_" + ipkt_bin.toString
      var src_word = word
      var dest_word = word

      if (p_case == 2 & dport < sport) {
        dest_word = "-1_" + dest_word
      } else if (p_case == 2 & sport < dport) {
        src_word = "-1_" + src_word
      } else if (p_case == 4 & dport == 0) {
        src_word = "-1_" + src_word
      } else if (p_case == 4 & sport == 0) {
        dest_word = "-1_" + dest_word
      }

      row.clone :+ word_port.toString :+ ip_pair :+ src_word.toString :+ dest_word.toString
    }

    //s1 = adjust_port(s1)
    val data_with_words = binned_data.map(row => adjust_port(row))


    //next groupby src to get src word counts
    val src_word_counts = data_with_words.map(row => (row(8) + " " + row(33), 1)).reduceByKey(_ + _)


    //groupby dest to get dest word counts
    val dest_word_counts = data_with_words.map(row => (row(9) + " " + row(34), 1)).reduceByKey(_ + _)

    //val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => Array(row._1.split(" ")(0).toString, row._1.split(" ")(1).toString, row._2).toString)
    val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => (row._1.split(" ")(0) + "," + row._1.split(" ")(1).toString + "," + row._2).mkString)


    word_counts.saveAsTextFile(output_file)

  }

}
