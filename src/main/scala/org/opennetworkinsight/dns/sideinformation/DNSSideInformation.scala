package org.opennetworkinsight.dns.sideinformation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.DNSSchema._
import org.opennetworkinsight.dns.model.DNSSuspiciousConnectsModel
import org.opennetworkinsight.utilities.{CountryCodes, TopDomains}



/**
  * Create the side information required by the OA layer.
  * @param model An instance of [[org.opennetworkinsight.dns.model.DNSSuspiciousConnectsModel]]
  *              Necessary because much of the side information is auxilliary information used to
  *              construct the model.
  */
class DNSSideInformation(model: DNSSuspiciousConnectsModel) {


  /**
    * Add side information to a dataframe.
    * @param sparkContext Spark context.
    * @param sqlContext Spark SQL context.
    * @param inDF dataframe containing at least the rows of
    *             [[org.opennetworkinsight.dns.model.DNSSuspiciousConnectsModel.ModelSchema]]
    * @return copy of the dataframe with the columsns [[DNSSideInformation.SideInfoSchema]] added
    */
  def addSideInformationForOA(sparkContext: SparkContext,
                              sqlContext: SQLContext,
                              inDF: DataFrame): DataFrame = {

    val countryCodesBC = sparkContext.broadcast(CountryCodes.CountryCodes)
    val topDomainsBC = sparkContext.broadcast(TopDomains.TOP_DOMAINS)

    val schemaWithAddedFields = StructType(inDF.schema.fields ++ DNSSideInformation.SideInfoSchema)

    val addSideInformationFunction = new DNSSideInformationFunction(inDF.schema.fieldNames,
      model.frameLengthCuts,
      model.timeCuts,
      model.subdomainLengthCuts,
      model.entropyCuts,
      model.numberPeriodsCuts,
      countryCodesBC,
      topDomainsBC)


    import addSideInformationFunction.{addSideFieldSeq, getIntegerField, getLongField, getStringField}

    val dataWithSideInformation: RDD[Row] = inDF.rdd.map(row =>
      Row.fromSeq {row.toSeq ++ addSideFieldSeq(timeStamp = getStringField(row, Timestamp),
          unixTimeStamp = getLongField(row, UnixTimestamp),
          frameLength = getIntegerField(row, FrameLength),
          clientIP = getStringField(row, ClientIP),
          queryName = getStringField(row, QueryName),
          queryClass = getStringField(row, QueryClass),
          dnsQueryType = getIntegerField(row, QueryType),
          dnsQueryRcode = getIntegerField(row, QueryResponseCode))})

    sqlContext.createDataFrame(dataWithSideInformation, schemaWithAddedFields)
  }
}

/**
  * Contains schema information for the DNS Side Information.
  */
object DNSSideInformation {
  val SideInfoSchema = StructType(List(DomainField,
    SubdomainField,
    SubdomainLengthField,
    SubdomainEntropyField,
    TopDomainField,
    NumPeriodsField,
    WordField))
}

/**
  * The per DNS log entry values in the side information.
  * @param domain
  * @param topDomain
  * @param subdomain
  * @param subdomainLength
  * @param subdomainEntropy
  * @param numPeriods
  * @param word
  */
case class SideFields(domain: String,
                      topDomain: Int,
                      subdomain: String,
                      subdomainLength: Int,
                      subdomainEntropy: Double,
                      numPeriods: Int,
                      word: String)
