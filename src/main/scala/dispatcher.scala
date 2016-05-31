package main.scala



class dispatcher {

    def main(args: Array[String]) {

      val f = args(1)

      f match {
        case "dns_post" => dns_post_lda.run()
        case "dns_pre" => dns_pre_lda.run()
        case "flow_post" =>flow_post_lda.run()
        case "flow_pre" => flow_pre_lda.run()
      }

      System.exit(0)
    }
}
