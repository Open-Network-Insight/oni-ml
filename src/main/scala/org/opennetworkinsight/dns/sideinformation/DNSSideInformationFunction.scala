package org.opennetworkinsight.dns.sideinformation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.opennetworkinsight.dns.DNSWordCreation
import org.opennetworkinsight.utilities.DomainProcessor.{DomainInfo, extractDomainInfo}


class DNSSideInformationFunction(fieldNames: Array[String],
                                 frameLengthCuts: Array[Double],
                                 timeCuts: Array[Double],
                                 subdomainLengthCuts: Array[Double],
                                 entropyCuts: Array[Double],
                                 numberPeriodsCuts: Array[Double],
                                 countryCodesBC: Broadcast[Set[String]],
                                 topDomainsBC: Broadcast[Set[String]]) extends Serializable {

  val dnsWordCreator = new DNSWordCreation(frameLengthCuts, timeCuts, subdomainLengthCuts, entropyCuts, numberPeriodsCuts, topDomainsBC)

  def getSideFields(timeStamp: String,
                    unixTimeStamp: Long,
                    frameLength: Int,
                    clientIP: String,
                    queryName: String,
                    queryClass: String,
                    dnsQueryType: Int,
                    dnsQueryRcode: Int): SideFields = {

    val DomainInfo(domain, topDomain, subdomain, subdomainLength, subdomainEntropy, numPeriods) =
      extractDomainInfo(queryName, topDomainsBC)


    val word = dnsWordCreator.dnsWord(timeStamp,
      unixTimeStamp,
      frameLength,
      clientIP,
      queryName,
      queryClass,
      dnsQueryType,
      dnsQueryRcode)

    SideFields(domain = domain,
      topDomain = topDomain,
      subdomain = subdomain,
      subdomainLength = subdomainLength,
      subdomainEntropy = subdomainEntropy,
      numPeriods = numPeriods,
      word = word)
  }


  def addSideFieldSeq(timeStamp: String,
                      unixTimeStamp: Long,
                      frameLength: Int,
                      clientIP: String,
                      queryName: String,
                      queryClass: String,
                      dnsQueryType: Int,
                      dnsQueryRcode: Int): Seq[Any] = {

    val sideFields = getSideFields(timeStamp,
      unixTimeStamp,
      frameLength,
      clientIP,
      queryName,
      queryClass,
      dnsQueryType,
      dnsQueryRcode)

    // this is probably the bug
    // the order does not match the frame schema
    /*
    object DNSSideInformation {
  val sideFieldSchema = StructType(List(DomainField,
    SubdomainField,
    SubdomainLengthField,
    SubdomainEntropyField,
    TopDomainField,
    NumPeriodsField,
    WordField))
}
     */
    Seq(sideFields.domain, sideFields.subdomain, sideFields.subdomainLength, sideFields.subdomainEntropy, sideFields.topDomain, sideFields.numPeriods, sideFields.word)
  }


  def getStringField(r: Row, field: String) = r.getString(fieldNames.indexOf(field))
  def getIntegerField(r: Row, field: String) = r.getInt(fieldNames.indexOf(field))
  def getLongField(r: Row, field: String) = r.getLong(fieldNames.indexOf(field))
}
