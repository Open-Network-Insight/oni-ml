package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.dns.DNSWordCreation.SubdomainInfo
import org.opennetworkinsight.dns.DNSSchema._
import org.opennetworkinsight.utilities.{CountryCodes, TopDomains}

case class SideFields(domain: String,
                      topDomain: Int,
                      subdomain: String,
                      subdomainLength: Double,
                      subdomainEntropy: Double,
                      numPeriods: Double,
                      word: String)

class DNSSideInformation(model: DNSSuspiciousConnectsModel) {

  val timeCuts = model.timeCuts
  val frameLengthCuts = model.frameLengthCuts
  val subdomainLengthCuts = model.subdomainLengthCuts
  val numberPeriodsCuts = model.numberPeriodsCuts
  val entropyCuts = model.entropyCuts





  val sideFieldSchema = refArrayOps(Array(StructField(Domain, StringType),
    StructField(TopDomain, IntegerType),
    StructField(Subdomain, StringType),
    StructField(SubdomainLength, DoubleType),
    StructField(SubdomainEntropy, DoubleType),
    StructField(NumPeriods, DoubleType),
    StructField(Word, StringType)))




  def addSideInformationForOA(sparkContext: SparkContext,
                              sqlContext: SQLContext,
                              inDF: DataFrame): DataFrame = {

    val countryCodesBC = sparkContext.broadcast(CountryCodes.CountryCodes)
    val topDomainsBC = sparkContext.broadcast(TopDomains.TOP_DOMAINS)


    val queryNameIndex = inDF.schema.fieldNames.indexOf(QueryName)


    val schemaWithAddedFields = StructType(inDF.schema.fields ++ sideFieldSchema)

    val addFields = new DNSSideInformationFunction(inDF.schema.fieldNames, frameLengthCuts,
      timeCuts,
      subdomainLengthCuts,
      entropyCuts,
      numberPeriodsCuts,
      countryCodesBC,
      topDomainsBC)


    val dataWithSideInformation: RDD[Row] = inDF.rdd.map(row =>
      Row.fromSeq {
        row.toSeq ++ addFields.addSideFieldArray(timeStamp = addFields.getStringField(row, Timestamp),
          unixTimeStamp = addFields.getLongField(row, UnixTimestamp),
          frameLength = addFields.getIntegerField(row, FrameLength),
          clientIP = addFields.getStringField(row, ClientIP),
          queryName = addFields.getStringField(row, QueryName),
          queryClass = addFields.getStringField(row, QueryClass),
          dnsQueryType = addFields.getIntegerField(row, QueryType),
          dnsQueryRcode = addFields.getIntegerField(row, QueryResponseCode))
      })

    // Update data frame schema with newly added columns. This happens b/c we are adding more than one column at once.

    sqlContext.createDataFrame(dataWithSideInformation, schemaWithAddedFields).select(OAColumns :_*)
  }




}
