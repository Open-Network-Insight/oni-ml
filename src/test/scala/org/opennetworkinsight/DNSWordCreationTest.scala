package org.opennetworkinsight


import org.opennetworkinsight.dns.DNSWordCreation
import org.opennetworkinsight.testutils.TestingSparkContextFlatSpec
import org.opennetworkinsight.utilities.Entropy
import org.scalatest.Matchers

class DNSWordCreationTest extends TestingSparkContextFlatSpec with Matchers{

  val countryCodesSet = DNSWordCreation.countryCodes

  "extractSubdomain" should "return domain=None, subdomain= None, subdomain length= 0 and number of parts = 6" in {

    val url = "123.103.104.10.in-addr.arpa"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "None"
    result(1) shouldBe "None"
    result(2) shouldBe 0
    result(3) shouldBe 6
  }

  it should "return domain=url index 'number of parts -3' , subdoamin=subset of url from index 0 to index 'number of parts" +
    "- 3', subdomain length= subdomain.length" in {

    val url = "services.amazon.com.mx"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "amazon"
    result(1) shouldBe "services"
    result(2) shouldBe 8
    result(3) shouldBe 4
  }

  it should "return domain=index 'number of parts -2' and subdomain=None, subdomain length=0" in {

    val url = "amazon.com.mx"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "amazon"
    result(1) shouldBe "None"
    result(2) shouldBe 0
    result(3) shouldBe 3
  }

  it should "return domain=index 'number of parts -2' and subdomain=subset of url parts from index 0 to index 'number " +
    "of parts -2', subdomain length!=0" in {

    val url = "services.amazon.com"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "amazon"
    result(1) shouldBe "services"
    result(2) shouldBe 8
    result(3) shouldBe 3
  }

  "extractSubdomain" should "return domain=None, subdomain= None, subdomain length= 0 and number of parts = 2" in {

    val url = "amazon.com"
    val countryCodes = sparkContext.broadcast(countryCodesSet)

    val result = DNSWordCreation.extractSubdomain(countryCodes, url)

    result.length shouldBe 4
    result(0) shouldBe "None"
    result(1) shouldBe "None"
    result(2) shouldBe 0
    result(3) shouldBe 2
  }


  "entropy" should "return 2.807354922057603 with value abcdefg" in {
    val value = "abcdefg"

    val result = Entropy.stringEntropy(value)

    result shouldBe 2.807354922057604
  }
}
