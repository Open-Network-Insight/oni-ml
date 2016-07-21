
package org.opennetworkinsight

import org.apache.log4j.{Level, Logger => apacheLogger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import org.opennetworkinsight.LDAArgumentParser.Config

import scala.io.Source

object LDA {

  val parser = {
    LDAArgumentParser.getParser()
  }

  def main(args: Array[String]) {

    val dataSource = args(0)

    parser.parse(args.drop(1), Config()) match {
      case Some(config) => {

        val logger = LoggerFactory.getLogger(this.getClass)
        apacheLogger.getLogger("org").setLevel(Level.OFF)
        apacheLogger.getLogger("akka").setLevel(Level.OFF)

        val sparkConfig = new SparkConf().setAppName("ONI ML:  " + dataSource + " lda")
        val sparkContext = new SparkContext(sparkConfig)
        val sqlContext = new SQLContext(sparkContext)

        dataSource match {

          case "flow" => {
            FlowLDA run(config, sparkContext, sqlContext, logger)
          }
          case "dns" => {
            DNSLDA run(config, sparkContext, sqlContext, logger)
          }
        }

        sparkContext.stop()
      }
      case None => println("Error parsing arguments")
    }

    System.exit(0)
  }

}