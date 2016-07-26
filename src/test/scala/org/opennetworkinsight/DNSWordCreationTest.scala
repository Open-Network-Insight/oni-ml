package org.opennetworkinsight


import org.opennetworkinsight.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

class DNSWordCreationTest extends TestingSparkContextFlatSpec with Matchers{

  val countryCodesSet = Set("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au",
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

  "extractSubdomain" should "return domain=None, subdomina= None, subdomain lenght= 0 and number of parts = 6" in {

    val url = "123.103.104.10.in-addr.arpa"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "None"
    result(1) shouldBe "None"
    result(2) shouldBe "0"
    result(3) shouldBe "6"
  }

  it should "return domain=url index 'number of parts -3' , subdoamin=subset of url from index 0 to index 'number of parts" +
    "- 3', subdomain lenght= subdomain.lenght" in {

    val url = "services.amazon.com.mx"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "amazon"
    result(1) shouldBe "services"
    result(2) shouldBe "8"
    result(3) shouldBe "4"
  }

  it should "return domain=index 'number of parts -2' and subdomain=None, subdomain lenght=0" in {

    val url = "amazon.com.mx"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "amazon"
    result(1) shouldBe "None"
    result(2) shouldBe "0"
    result(3) shouldBe "3"
  }

  it should "return domain=index 'number of parts -2' and subdomain=subset of url parts from index 0 to index 'number " +
    "of parts -2', subdomain lenght!=0" in {

    val url = "services.amazon.com"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "amazon"
    result(1) shouldBe "services"
    result(2) shouldBe "8"
    result(3) shouldBe "3"
  }

  "extractSubdomain" should "return domain=None, subdomina= None, subdomain lenght= 0 and number of parts = 2" in {

    val url = "amazon.com"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "None"
    result(1) shouldBe "None"
    result(2) shouldBe "0"
    result(3) shouldBe "2"
  }


  "binColumn" should "return 3 when the value is not bigger than the forth quintile" in {
    val quintiles = Array(1.0, 2.0, 3.0, 4.0, 5.0)

    val result = DNSWordCreation.binColumn("3.5", quintiles)

    result shouldBe "3"
  }

  it should "return 5 when the value is Double.PositiveInfinity" in {
    val quintiles = Array(1.0, 2.0, 3.0, 4.0, 5.0)

    val result = DNSWordCreation.binColumn("Infinity", quintiles)

    result shouldBe "5"
  }

  it should "return 0 when the value is less than the first quintile" in {
    val quintiles = Array(1.0, 2.0, 3.0, 4.0, 5.0)

    val result = DNSWordCreation.binColumn("0", quintiles)

    result shouldBe "0"
  }

  it should "return 0 when no cuts" in {
    val cuts = Array[Double]()

    val result = DNSWordCreation.binColumn("Infinity", cuts)

    result shouldBe "0"
  }

  "entropy" should "return 2.807354922057603 with value abcdefg" in {
    val value = "abcdefg"

    val result = DNSWordCreation.entropy(value)

    result shouldBe 2.807354922057604
  }

  "getColumnNames" should "return Map[String,Int] with index of each column header" in {
    val header = Array("column1", "column2", "column3")

    val result = DNSWordCreation.getColumnNames(header)

    result.isEmpty shouldBe false
    result.size shouldBe 3
    result("column1") shouldBe 0
    result("column2") shouldBe 1
    result("column3") shouldBe 2
  }
}
