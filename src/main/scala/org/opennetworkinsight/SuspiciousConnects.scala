package org.opennetworkinsight

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import org.opennetworkinsight.SuspiciousConnectsArgumentParser.Config
import org.opennetworkinsight.dns.DNSSuspiciousConnects
import org.opennetworkinsight.netflow.FlowSuspiciousConnects
import org.opennetworkinsight.proxy.ProxySuspiciousConnects

/**
  * Execute suspicious connections analysis on network data.
  */
object SuspiciousConnects {

  def main(args: Array[String]) {

    val parser = SuspiciousConnectsArgumentParser.parser

    parser.parse(args, Config()) match {
      case Some(config) => {
        val logger = LoggerFactory.getLogger(this.getClass)
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val analysis = config.analysis
        val sparkConfig = new SparkConf().setAppName("ONI ML:  " + analysis + " lda")
        val sparkContext = new SparkContext(sparkConfig)
        val sqlContext = new SQLContext(sparkContext)

        analysis match {
          case "flow" => FlowSuspiciousConnects.run(config, sparkContext, sqlContext, logger)
          case "dns" => DNSSuspiciousConnects.run(config, sparkContext, sqlContext, logger)
          case "proxy" => ProxySuspiciousConnects.run(config, sparkContext, sqlContext, logger)
          case _ => println("ERROR:  unsupported (or misspelled) analysis: " + analysis)
        }

        sparkContext.stop()
      }
      case None => println("Error parsing arguments")
    }

    System.exit(0)
  }
}