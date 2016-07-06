
package org.opennetworkinsight

import org.apache.log4j.{Level, Logger => apacheLogger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import org.opennetworkinsight.{FlowColumnIndex => indexOf}

import scala.io.Source

object FlowLDA{

  case class Config(inputPath : String = "",
                    scoresFile : String = "",
                    duplicationFactor: Int = 1,
                    modelFile: String = "",
                    topicDocumentFile: String = "",
                    topicWordFile: String = "",
                    mpiPreparationCmd: String = "",
                    mpiCmd: String = "",
                    mpiProcessCount: String = "",
                    mpiTopicCount: String = "",
                    ldaOutputPath: String = "",
                    localPath: String = "",
                    localUser: String = "",
                    dataSource: String = "",
                    nodes: String = "",
                    hdfsScoredConnect: String = "",
                    threshold: Double = 1.0d
                    )

  val parser = new scopt.OptionParser[Config]("FlowLDA") {

    head("FlowLDA", "1.1")

    opt[String]('i', "input").required().valueName("<hdfs path>").
      action( (x, c) => c.copy(inputPath = x) ).
      text("HDFS path to netflow records")

    opt[String]('f', "feedback").valueName("<local file>").
      action( (x, c) => c.copy(scoresFile = x) ).
      text("the local path of the file that contains the feedback scores")

    opt[Int]('d', "dupfactor").valueName("<non-negative integer>").
      action( (x, c) => c.copy(duplicationFactor = x) ).
      text("duplication factor controlling how to downgrade non-threatening connects from the feedback file")

    opt[String]('m', "model").required().valueName("<local file>").
      action( (x, c) => c.copy(modelFile = x) ).
      text("Model file location")

    opt[String]('o', "topicdoc").required().valueName("<local file>").
      action( (x, c) => c.copy(topicDocumentFile = x) ).
      text("final.gamma file location")

    opt[String]('w', "topicword").required().valueName("<local file>").
      action( (x, c) => c.copy(topicWordFile = x) ).
      text("final.beta file location")

    opt[String]('p', "mpiprep").valueName("<mpi command>").
      action( (x, c) => c.copy(mpiPreparationCmd = x) ).
      text("MPI preparation command")

    opt[String]('c', "mpicmd").required().valueName("<mpi command>").
      action( (x, c) => c.copy(mpiCmd = x) ).
      text("MPI command")

    opt[String]('t', "proccount").required().valueName("<mpi param>").
      action( (x, c) => c.copy(mpiProcessCount = x) ).
      text("MPI process count")

    opt[String]('u', "topiccount").required().valueName("<mpi param>").
      action( (x, c) => c.copy(mpiTopicCount = x) ).
      text("MPI topic count")

    opt[String]('l', "ldaoutput").required().valueName("<mpi param>").
      action( (x, c) => c.copy(ldaOutputPath = x) ).
      text("LDA output file")

    opt[String]('a', "lpath").required().valueName("<local path>").
      action( (x, c) => c.copy(localPath = x) ).
      text("Local Path")

    opt[String]('r', "luser").required().valueName("<local path>").
      action( (x, c) => c.copy(localUser = x) ).
      text("Local user path")

    opt[String]('s', "dsource").required().valueName("<input param>").
      action( (x, c) => c.copy(dataSource = x) ).
      text("Data source")

    opt[String]('n', "nodes").required().valueName("<input param>").
      action( (x, c) => c.copy(nodes = x) ).
      text("Node list")

    opt[String]('h', "scored").required().valueName("<hdfs path>").
      action((x, c) => c.copy(hdfsScoredConnect = x)).
      text("HDFS path for results")

    opt[Double]('e', "threshold").required().valueName("float64").
      action((x, c) => c.copy(threshold = x)).
      text("probability threshold for declaring anomalies")
  }

  def run(args: Array[String]) = {

    parser.parse(args.drop(1), Config()) match {
      case Some(config) => {
        val logger = LoggerFactory.getLogger(this.getClass)
        apacheLogger.getLogger("org").setLevel(Level.OFF)
        //apacheLogger.getLogger("akka").setLevel(Level.OFF)

        logger.info("Flow LDA starts")

        val conf = new SparkConf().setAppName("ONI ML: flow pre lda")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val docWordCount = FlowPreLDA.flowPreLDA(config.inputPath, config.scoresFile, config.duplicationFactor, sc,
          sqlContext, logger)

        val ldaResult = LDAWrapper.runLDA(docWordCount, config.modelFile, config.topicDocumentFile, config.topicWordFile,
          config.mpiPreparationCmd, config.mpiCmd, config.mpiProcessCount, config.mpiTopicCount, config.ldaOutputPath,
          config.localPath, config.localUser, config.dataSource, config.nodes)

        FlowPostLDA.flowPostLDA(config.inputPath, config.hdfsScoredConnect, config.threshold, ldaResult("document_results"),
          ldaResult("word_results"), sc, sqlContext, logger)

        sc.stop()
      }
      case None => println("Error parsing arguments")
    }

  }

}