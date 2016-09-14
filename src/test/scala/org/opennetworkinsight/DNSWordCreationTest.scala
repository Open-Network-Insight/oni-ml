package org.opennetworkinsight


import javax.swing.text.Utilities

import org.opennetworkinsight.dns.DNSWordCreation.DerivedFields
import org.opennetworkinsight.dns.{DNSSuspiciousConnectsAnalysis, DNSWordCreation}
import org.opennetworkinsight.testutils.TestingSparkContextFlatSpec
import org.opennetworkinsight.utilities.{CountryCodes, Entropy, TopDomains}
import org.scalatest.Matchers

class DNSWordCreationTest extends TestingSparkContextFlatSpec with Matchers {

  val countryCodesSet = CountryCodes.CountryCodes

  "createDerivedFields" should "handle an in-addr.arpa url" in {

    val url = "123.103.104.10.in-addr.arpa"
    val countryCodes = sparkContext.broadcast(countryCodesSet)
    val topDomains = sparkContext.broadcast(TopDomains.TOP_DOMAINS)

    // case class DerivedFields(topDomain: String, subdomainLength: Double, subdomainEntropy: Double, numPeriods: Double)
    val result = DNSWordCreation.createDerivedFields(countryCodes, topDomains, url)

    result shouldBe DerivedFields(topDomainClass = 0, subdomainLength = 0, subdomainEntropy = 0, numPeriods = 6)
  }

  it should "handle an Alexa top 1M domain with a subdomain, top-level domain name and country code" in {

    val url = "services.amazon.com.mx"
    val countryCodes = sparkContext.broadcast(countryCodesSet)
    val topDomains = sparkContext.broadcast(TopDomains.TOP_DOMAINS)

    val result = DNSWordCreation.createDerivedFields(countryCodes, topDomains, url)

    result shouldBe DerivedFields(topDomainClass = 1, subdomainLength = 8, subdomainEntropy = 2.5, numPeriods = 4)
  }

  it should "handle an Alexa top 1M domain with a top-level domain name and country code but no subdomain" in {

    val url = "amazon.com.mx"
    val countryCodes = sparkContext.broadcast(countryCodesSet)
    val topDomains = sparkContext.broadcast(TopDomains.TOP_DOMAINS)

    val result = DNSWordCreation.createDerivedFields(countryCodes, topDomains, url)

    result shouldBe DerivedFields(topDomainClass = 1, subdomainLength = 0, subdomainEntropy = 0, numPeriods = 3)
  }

  it should "handle an Alexa top 1M domain with a subdomain and top-level domain name but no country code" in {

    val url = "services.amazon.com"
    val countryCodes = sparkContext.broadcast(countryCodesSet)
    val topDomains = sparkContext.broadcast(TopDomains.TOP_DOMAINS)

    val result = DNSWordCreation.createDerivedFields(countryCodes, topDomains, url)

    result shouldBe DerivedFields(topDomainClass = 1, subdomainLength = 8, subdomainEntropy = 2.5, numPeriods = 3)
  }

  // this is the inherited behavior... but is it what we want? shouldn't this URL get an Alexa TopDomain class for
  // having the domain "amazon" ???

  it should "handle an Alexa top 1M domain with no subdomain or country code" in {

    val url = "amazon.com"
    val countryCodes = sparkContext.broadcast(countryCodesSet)
    val topDomains = sparkContext.broadcast(TopDomains.TOP_DOMAINS)
    val result = DNSWordCreation.createDerivedFields(countryCodes, topDomains, url)

    result shouldBe DerivedFields(topDomainClass = 0, subdomainLength = 0, subdomainEntropy = 0, numPeriods = 2)
  }


  "entropy" should "return 2.807354922057603 with value abcdefg" in {
    val value = "abcdefg"

    val result = Entropy.stringEntropy(value)

    result shouldBe 2.807354922057604
  }

}
