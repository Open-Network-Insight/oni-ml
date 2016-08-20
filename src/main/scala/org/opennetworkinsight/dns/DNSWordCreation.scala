package org.opennetworkinsight.dns

import org.apache.spark.broadcast.Broadcast

  object DNSWordCreation {

    val l_country_codes = Set("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au",
      "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bm", "bn", "bo", "bq", "br", "bs", "bt",
      "bv", "bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr", "cu", "cv",
      "cw", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "eu", "fi",
      "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr",
      "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "io", "iq", "ir",
      "is", "it", "je", "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "krd", "kw", "ky", "kz", "la",
      "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mk", "ml", "mm",
      "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni",
      "nl", "no", "np", "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "ps", "pt",
      "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "", "sk",
      "sl", "sm", "sn", "so", "sr", "ss", "st", "su", "sv", "sx", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk",
      "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve",
      "vg", "vi", "vn", "vu", "wf", "ws", "ye", "yt", "za", "zm", "zw")

    def getColumnNames(input: Array[String], sep: Char = ','): scala.collection.mutable.Map[String, Int] = {
      val columns = scala.collection.mutable.Map[String, Int]()
      val header = input.zipWithIndex
      header.foreach(tuple => columns(tuple._1) = tuple._2)
      columns
    }

    def extractSubdomain(country_codes: Broadcast[Set[String]], url: String): Array[String] = {
      var spliturl = url.split("[.]")
      var numparts = spliturl.length
      var domain = "None"
      var subdomain = "None"


      //first check if query is an Ip address e.g.: 123.103.104.10.in-addr.arpa or a name
      val is_ip = {
        if (numparts > 2) {
          if (spliturl(numparts - 1) == "arpa" & spliturl(numparts - 2) == "in-addr") {
            "IP"
          } else "Name"
        } else "Unknown"
      }

      if (numparts > 2 && is_ip != "IP") {
        //This might try to parse things with only 1 or 2 numparts
        //test if last element is a country code or tld
        //use: Array(spliturl(numparts-1)).exists(country_codes contains _)
        // don't use: country_codes.exists(spliturl(numparts-1).contains) this doesn't test exact match, might just match substring
        if (Array(spliturl(numparts - 1)).exists(country_codes.value contains _)) {
          domain = spliturl(numparts - 3)
          if (1 <= numparts - 3) {
            subdomain = spliturl.slice(0, numparts - 3).mkString(".")
          }
        }
        else {
          domain = spliturl(numparts - 2)
          subdomain = spliturl.slice(0, numparts - 2).mkString(".")
        }
      }
      Array(domain, subdomain, {
        if (subdomain != "None") {
          subdomain.length.toString
        } else {
          "0"
        }
      }, numparts.toString)
    }

    def binColumn(value: String, cuts: Array[Double]) = {
      var bin = 0
      for (cut <- cuts) {
        if (value.toDouble > cut) {
          bin = bin + 1
        }
      }
      bin.toString
    }

  }


