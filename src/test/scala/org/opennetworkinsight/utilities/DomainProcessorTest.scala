package org.opennetworkinsight.utilities


import org.opennetworkinsight.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

/**
  * Created by nlsegerl on 8/9/16.
  */
class DomainProcessorTest extends TestingSparkContextFlatSpec with Matchers {

  "extractDomain" should "return domain when provided a url with top-level domain and country code" in {

    val url = "fatosdesconhecidos.com.br"
    val result = DomainProcessor.extractDomain(url)

    result shouldEqual ("fatosdesconhecidos")
  }

  it should "return domain when provided a short url with no top-level domain but only a country code" in {

    val url = "panasonic.jp"
    val result = DomainProcessor.extractDomain(url)

    result shouldEqual ("panasonic")
  }

  it should "return domain when provided a long url with no top-level domain but only a country code" in {

    val url = "get.your.best.electronic.at.panasonic.jp"
    val result = DomainProcessor.extractDomain(url)

    result shouldEqual ("panasonic")
  }


  it should "return domain when provided a short url with a top-level domain no country code" in {

    val url = "forrealz.net"
    val result = DomainProcessor.extractDomain(url)

    result shouldEqual ("forrealz")
  }

  it should "return domain when provided a long url with a top-level domain no country code" in {

    val url = "wow.its.really.long.forrealz.net"
    val result = DomainProcessor.extractDomain(url)

    result shouldEqual ("forrealz")
  }

  it should "should return  NONE when provided an address" in {

    val url = "123.103.104.10.in-addr.arpa"
    val result = DomainProcessor.extractDomain(url)

    result shouldEqual ("None")
  }

  it should "return NONE when provided a short url with a bad top-level domain / country code" in {

    val url = "panasonic.c"
    val result = DomainProcessor.extractDomain(url)

    result shouldEqual ("None")
  }
}
