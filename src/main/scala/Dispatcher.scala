package main.scala



object Dispatcher {

    def main(args: Array[String]) {

      val f = args(0)

      f match {
        case "dns_post_lda" => dns_post_lda.run()
        case "dns_pre_lda" => dns_pre_lda.run()
        case "flow_post_lda" =>flow_post_lda.run()
        case "flow_pre_lda" => FlowPreLDA.run()
      }

      System.exit(0)
    }
}
