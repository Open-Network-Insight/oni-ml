package org.opennetworkinsight

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.opennetworkinsight.dns.DNSSuspiciousConnectsAnalysis
import org.opennetworkinsight.netflow.FlowSuspiciousConnects
import org.opennetworkinsight.proxy.ProxySuspiciousConnectsAnalysis

/**
  * Top level entrypoint to execute suspicious connections analysis on network data.
  * Supported analyses:
  *  flow  : netflow data
  *  dns : DNS server logs
  *  proxy : proxy server logs
  */

object SuspiciousConnects {

  val OutputDelimiter = "\t"

  /**
    * Execute suspicious connections analysis on network data.
    *
    * @param args Command line arguments.
    */
  def main(args: Array[String]) {

    val parser = SuspiciousConnectsArgumentParser.parser

    parser.parse(args, SuspiciousConnectsConfig()) match {
      case Some(config) =>
        val logger = LoggerFactory.getLogger(this.getClass)
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val analysis = config.analysis
        val sparkConfig = new SparkConf().setAppName("ONI ML:  " + analysis + " lda")
        val sparkContext = new SparkContext(sparkConfig)
        val sqlContext = new SQLContext(sparkContext)
        implicit val outputDelimiter = OutputDelimiter

        analysis match {
          case "flow" => FlowSuspiciousConnects.run(config, sparkContext, sqlContext, logger)
          case "dns" => DNSSuspiciousConnectsAnalysis.run(config, sparkContext, sqlContext, logger)
          case "proxy" => ProxySuspiciousConnectsAnalysis.run(config, sparkContext, sqlContext, logger)
          case _ => println("ERROR:  unsupported (or misspelled) analysis: " + analysis)
        }

        sparkContext.stop()

      case None => println("Error parsing arguments")
    }

    System.exit(0)
  }


}