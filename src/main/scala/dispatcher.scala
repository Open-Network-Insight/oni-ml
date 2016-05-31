package main.scala



class dispatcher {

    def main(args: Array[String]) {

      val f = args(1)

      f match {
        case "dns_post" => println("dns_post")
        case "dns_pre" => println("dns_pre")
        case "flow_post" => println("flow_post")
        case "flow_pre" => flow_pre_lda.run()
      }

      System.exit(0)
    }
}
