
package org.opennetworkinsight

import org.apache.log4j.{Level, Logger => apacheLogger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import org.opennetworkinsight.{FlowColumnIndex => indexOf}

import scala.io.Source


/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming netflow records.
  */
object FlowPreLDA {


  case class Config(inputPath : String = "",
                   outputPath : String = "",
                   scoresFile : String = "",
                   duplicationFactor: Int = 1)

  val parser = new scopt.OptionParser[Config]("FlowPreLDA") {

    head("FlowPreLDA", "1.1")

    opt[String]('i', "input").required().valueName("<hdfs path>").
      action( (x, c) => c.copy(inputPath = x) ).
      text("HDFS path to netflow records")

    opt[String]('o', "output").required().valueName("<hdfs path>").
      action( (x, c) => c.copy(outputPath = x) ).
      text("HDFS path for output")

    opt[String]('f', "feedback").valueName("<local file>").
      action( (x, c) => c.copy(scoresFile = x) ).
      text("the local path of the file that contains the feedback scores")

    opt[Int]('d', "dupfactor").valueName("<non-negative integer>").
      action( (x, c) => c.copy(duplicationFactor = x) ).
      text("duplication factor controlling how to downgrade non-threatening connects from the feedback file")
  }


    def run(args: Array[String]) = {


      parser.parse(args.drop(1), Config()) match {

        case Some(config) => {
          val logger = LoggerFactory.getLogger(this.getClass)
          apacheLogger.getLogger("org").setLevel(Level.OFF)
          apacheLogger.getLogger("akka").setLevel(Level.OFF)

          logger.info("Flow pre LDA starts")

          val conf = new SparkConf().setAppName("ONI ML: flow pre lda")
          val sc = new SparkContext(conf)
          val sqlContext = new SQLContext(sc)

          val scoredFile = config.scoresFile
          logger.info("scoredFile:  " + scoredFile)
          logger.info("outputFile:  " + config.outputPath)

          var ibyt_cuts = new Array[Double](10)
          var ipkt_cuts = new Array[Double](5)
          var time_cuts = new Array[Double](10)

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

          logger.info("Trying to read file:  " + config.inputPath)
          val rawdata: RDD[String] = {
            val flowDataFrame = sqlContext.parquetFile(config.inputPath)
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

          val scoredFileExists =  if (scoredFile != "") new java.io.File(scoredFile).exists else false

          val scoredData: Array[String] = if (scoredFileExists) {
            val duplicationFactor = config.duplicationFactor

            val rowsToDuplicate = Source.fromFile(scoredFile).getLines().toArray.drop(1).filter(l => (l.split(',').length == 22) && l.split(',')(0).toInt == 3)
            logger.info("User feedback read from: " + scoredFile + ". "
              + rowsToDuplicate.length + " many connections flagged nonthreatening.")
            logger.info("Duplication factor: " + duplicationFactor)
            rowsToDuplicate.map(convert_feedback_row_to_flow_row(_)).flatMap(List.fill(duplicationFactor)(_))
          } else {
            Array[String]()
          }

          val totalData: RDD[String] = rawdata.union(sc.parallelize(scoredData))

          val data_with_time = totalData.map(_.trim.split(',')).map(FlowWordCreation.addTime)

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

          //next groupby src to get src word counts
          val src_word_counts = data_with_words.map(row => (row(indexOf.SOURCEIP) + " " + row(indexOf.SOURCEWORD), 1)).reduceByKey(_ + _)


          //groupby dest to get dest word counts
          val dest_word_counts = data_with_words.map(row => (row(indexOf.DESTIP) + " " + row(indexOf.DESTWORD), 1)).reduceByKey(_ + _)

          //val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => Array(row._1.split(" ")(0).toString, row._1.split(" ")(1).toString, row._2).toString)
          val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => (row._1.split(" ")(0) + "," + row._1.split(" ")(1).toString + "," + row._2).mkString)

          logger.info("Persisting data")
          word_counts.saveAsTextFile(config.outputPath)

          sc.stop()
          logger.info("Flow pre LDA completed")
        }

        case None => println("Error parsing arguments")
      }


  }

}
