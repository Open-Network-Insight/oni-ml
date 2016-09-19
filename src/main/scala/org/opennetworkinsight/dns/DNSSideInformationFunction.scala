package org.opennetworkinsight.dns

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.opennetworkinsight.dns.DNSWordCreation.SubdomainInfo
import org.opennetworkinsight.utilities.Entropy


class DNSSideInformationFunction(fieldNames: Array[String],
                                 frameLengthCuts: Array[Double],
                                 timeCuts: Array[Double],
                                 subdomainLengthCuts: Array[Double],
                                 entropyCuts: Array[Double],
                                 numberPeriodsCuts: Array[Double],
                                 countryCodesBC: Broadcast[Set[String]],
                                 topDomainsBC: Broadcast[Set[String]]) extends Serializable {

  def getSideFields(timeStamp: String,
                    unixTimeStamp: Long,
                    frameLength: Int,
                    clientIP: String,
                    queryName: String,
                    queryClass: String,
                    dnsQueryType: Int,
                    dnsQueryRcode: Int): SideFields = {

    val SubdomainInfo(domain, subdomain, subdomainLength, numPeriods) =
      DNSWordCreation.extractSubomain(countryCodesBC, queryName)

    val topDomain = DNSWordCreation.getTopDomain(topDomainsBC, domain)
    val subdomainEntropy = if (subdomain != "None") Entropy.stringEntropy(subdomain) else 0d

    val word = DNSWordCreation.dnsWord(timeStamp,
      unixTimeStamp,
      frameLength,
      clientIP,
      queryName,
      queryClass,
      dnsQueryType,
      dnsQueryRcode,
      frameLengthCuts,
      timeCuts,
      subdomainLengthCuts,
      entropyCuts,
      numberPeriodsCuts,
      countryCodesBC,
      topDomainsBC)

    SideFields(domain = domain,
      topDomain = topDomain,
      subdomain = subdomain,
      subdomainLength = subdomainLength,
      subdomainEntropy = subdomainEntropy,
      numPeriods = numPeriods,
      word = word)
  }


  def addSideFieldArray(timeStamp: String,
                    unixTimeStamp: Long,
                    frameLength: Int,
                    clientIP: String,
                    queryName: String,
                    queryClass: String,
                    dnsQueryType: Int,
                    dnsQueryRcode: Int): Array[Any] = {

    val sideFields = getSideFields(timeStamp,
      unixTimeStamp,
      frameLength,
      clientIP,
      queryName,
      queryClass,
      dnsQueryType,
      dnsQueryRcode)

    Array(sideFields.domain, sideFields.topDomain, sideFields.subdomain, sideFields.subdomainLength, sideFields.subdomainEntropy, sideFields.numPeriods, sideFields.word)
  }


  def getStringField(r: Row, field: String) = r.getString(fieldNames.indexOf(field))
  def getIntegerField(r: Row, field: String) = r.getInt(fieldNames.indexOf(field))
  def getLongField(r: Row, field: String) = r.getLong(fieldNames.indexOf(field))
}
