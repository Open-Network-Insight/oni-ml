package org.opennetworkinsight.utilities

import org.apache.spark.broadcast.Broadcast

import scala.io.Source


object DomainProcessor extends Serializable {

  val COUNTRY_CODES = Set("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au",
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

  val TOP_LEVEL_DOMAIN_NAMES = Set("com", "org", "net", "int", "edu", "gov", "mil")
  val NO_DOMAIN = "None"

  def extractDomain(url: String): String = {

    val spliturl = url.split('.')
    val numParts = spliturl.length

    // First check if query is an IP address e.g.: 123.103.104.10.in-addr.arpa or a name.
    // Such URLs receive a domain of NO_DOMAIN

    if (numParts > 2 && spliturl(numParts - 1) == "arpa" && spliturl(numParts - 2) == "in-addr") {
      NO_DOMAIN  // it's an address
    } else if (!COUNTRY_CODES.contains(spliturl.last) && !TOP_LEVEL_DOMAIN_NAMES.contains(spliturl.last)) {
      NO_DOMAIN  //  it does not have a valid top-level domain name
    } else {
      val strippedSplitURL = removeTopLevelDomainName(removeCountryCode(spliturl))
      if (strippedSplitURL.length > 0) {
        strippedSplitURL.last
      } else {
        // invalid URL... nothing that is not TLD.countrycode
        NO_DOMAIN
      }
    }
  }

  def removeCountryCode(urlComponents: Array[String]): Array[String] = {
    if (COUNTRY_CODES.contains(urlComponents.last)) {
      urlComponents.dropRight(1)
    } else {
      urlComponents
    }
  }

  def removeTopLevelDomainName(urlComponents: Array[String]): Array[String] = {
    if (TOP_LEVEL_DOMAIN_NAMES.contains(urlComponents.last)) {
      urlComponents.dropRight(1)
    } else {
      urlComponents
    }
  }
}
