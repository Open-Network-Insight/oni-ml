package org.opennetworkinsight

/**
  * Top-level entry point for suspicious connection analyses.
  *
  */

object Dispatcher {

  /**
    *
    * @param args CLI takes one argument, the process to be invoked:
    *             dns_pre_lda  : perform word creation for DNS analysis
    *             dns_post_lda : score connections based on a DNS suspicious connects model
    *             flow_pre_lda : perform word creation for netflow analysis
    *             flow_post_lda : score connections based on a netflow suspicious connects model
    */
    def main(args: Array[String]) {

//      val f = args(0)

//      f match {
//        case "dns_post_lda" => DnsPostLDA.run(args)
//        case "dns_pre_lda" => DnsPreLDA.run(args)
//        //case "flow_post_lda" =>FlowPostLDA.run(args)
//        //case "flow_pre_lda" => FlowPreLDA.run(args)
//        case "flow_lda" => FlowLDA.run(args)
//      }


      System.exit(0)
    }
}
