package main.scala



object Dispatcher {

    def main(args: Array[String]) {

      val f = args(0)

      f match {
        case "dns_post_lda" => DnsPostLDA.run()
        case "dns_pre_lda" => DnsPreLDA.run()
        case "flow_post_lda" =>FlowPostLDA.run()
        case "flow_pre_lda" => FlowPreLDA.run()
      }

      System.exit(0)
    }
}
