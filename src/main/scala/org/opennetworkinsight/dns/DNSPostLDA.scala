
package org.opennetworkinsight.dns

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.opennetworkinsight.utilities.{Entropy, Quantiles}
import org.slf4j.Logger

import scala.io.Source

/**
  * Contains routines for scoring incoming netflow records from a DNS suspicious connections model.
  */
object DNSPostLDA {

  def dnsPostLDA(inputPath: String, resultsFilePath: String, threshold: Double, topK: Int, documentResults: Array[String],
                 wordResults: Array[String], sc: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("DNS post LDA starts")

    import sqlContext.implicits._

    var timeCuts = new Array[Double](10)
    var frameLengthCuts = new Array[Double](10)
    var subdomainLengthCuts = new Array[Double](5)
    var numberPeriodsCuts = new Array[Double](5)
    var entropyCuts = new Array[Double](5)
    var rawDataDFColumns = new Array[String](0)

    val topDomains = sc.broadcast(Source.fromFile("top-1m.csv").getLines.map(line => {
      val parts = line.split(",")
      parts(1).split("[.]")(0)
    }).toSet)

    val topicLines = documentResults.map(line => {
      val ip = line.split(",")(0)
      val text = line.split(",")(1)
      val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
      (ip, text_no_quote)
    }).map(elem => elem._1 -> elem._2).toMap

    val topics = sc.broadcast(topicLines)

    val wordLines = wordResults.map(line => {
      val word = line.split(",")(0)
      val text = line.split(",")(1)
      val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
      (word, text_no_quote)
    }).map(elem => elem._1 -> elem._2).toMap

    val words = sc.broadcast(wordLines)

    val rawDataRDD = {
      val df = sqlContext.parquetFile(inputPath.split(",")(0))
        .filter("frame_len is not null and unix_tstamp is not null")
        .select("frame_time",
          "unix_tstamp",
          "frame_len",
          "ip_dst",
          "dns_qry_name",
          "dns_qry_class",
          "dns_qry_type",
          "dns_qry_rcode")
      // Need to extract raw data columns to reference index in future lines. rawDataDFColumns will be zipped with index.
      rawDataDFColumns = df.columns
      df.map(_.mkString(","))
    }

    val totalDataDF = rawDataRDD.toDF

    val rawDataColumnWithIndex = DNSWordCreation.getColumnNames(rawDataDFColumns)

    val countryCodes = sc.broadcast(DNSWordCreation.countryCodes)

    logger.info("Computing subdomain info")

    val dataWithSubdomainsRDD: RDD[Row] = totalDataDF.rdd.map(row =>
      Row.fromSeq {
        row.toSeq ++
          DNSWordCreation.extractSubdomain(countryCodes, row.getAs[String](rawDataColumnWithIndex("dns_qry_name")))
      })

    // Update data frame schema with newly added columns. This happens b/c we are adding more than one column at once.
    val schemaWithSubdomain = {
      StructType(totalDataDF.schema.fields ++
        refArrayOps(Array(StructField("domain", StringType),
          StructField("subdomain", StringType),
          StructField("subdomain.length", DoubleType),
          StructField("num.periods", DoubleType))))
    }

    val dataWithSubDomainsDF = sqlContext.createDataFrame(dataWithSubdomainsRDD, schemaWithSubdomain)

    logger.info("Computing subdomain entropy")

    val udfStringEntropy = DNSWordCreation.udfStringEntropy()

    val dataWithSubdomainEntropyDF = dataWithSubDomainsDF.withColumn("subdomain.entropy",
      udfStringEntropy(dataWithSubDomainsDF("subdomain")))

    logger.info("Calculating time cuts ...")

    timeCuts = Quantiles.computeDeciles(dataWithSubdomainEntropyDF
      .select("unix_tstamp")
      .rdd
      .map({ case Row(unixTimeStamp: Int) => unixTimeStamp.toDouble }))

    logger.info(timeCuts.mkString(","))

    logger.info("Calculating frame length cuts ...")

    frameLengthCuts = Quantiles.computeDeciles(dataWithSubdomainEntropyDF
      .select("frame_len")
      .rdd
      .map({ case Row(frameLen: Int) => frameLen.toDouble }))

    logger.info(frameLengthCuts.mkString(","))

    logger.info("Calculating subdomain length cuts ...")

    subdomainLengthCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
      .filter("subdomain.length > 0")
      .select("subdomain.length")
      .rdd
      .map({ case Row(subdomainLength: Double) => subdomainLength }))

    logger.info(subdomainLengthCuts.mkString(","))

    logger.info("Calculating entropy cuts")

    entropyCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
      .filter("subdomain.entropy > 0")
      .select("subdomain.entropy")
      .rdd
      .map({ case Row(subdomainEntropy: Double) => subdomainEntropy }))

    logger.info(entropyCuts.mkString(","))

    logger.info("Calculating num periods cuts ...")

    numberPeriodsCuts = Quantiles.computeQuintiles(dataWithSubdomainEntropyDF
      .filter("num.periods > 0")
      .select("num.periods")
      .rdd
      .map({ case Row(numberPeriods: Double) => numberPeriods }))

    logger.info(numberPeriodsCuts.mkString(","))

    val udfGetTopDomain = DNSWordCreation.udfGetTopDomain(topDomains)

    val dataWithTopDomainDF = dataWithSubdomainEntropyDF.withColumn("top_domain", udfGetTopDomain(dataWithSubdomainEntropyDF("domain")))

    logger.info("Adding words")

    val udfWordCreation = DNSWordCreation.udfWordCreation(frameLengthCuts, timeCuts, subdomainLengthCuts, entropyCuts, numberPeriodsCuts)

    val dataWithWordDF = dataWithTopDomainDF.withColumn("word", udfWordCreation(
      dataWithTopDomainDF("top_domain"),
      dataWithTopDomainDF("frame_len"),
      dataWithTopDomainDF("unix_tstamp"),
      dataWithTopDomainDF("subdomain.length"),
      dataWithTopDomainDF("subdomain.entropy"),
      dataWithTopDomainDF("num.periods"),
      dataWithTopDomainDF("dns_qry_type"),
      dataWithTopDomainDF("dns_qry_rcode"))).select("ip_dst, word")

    logger.info("Computing conditional probability")

    def scoreFunction(ip: String, word: String) : Double = {

      val uniformProb = Array.fill(20){0.05d}

      val topicGivenDocProbs  = topics.value.getOrElse(ip, uniformProb)
      val wordGivenTopicProbs = words.value.getOrElse(word, uniformProb)

      topicGivenDocProbs.zip(wordGivenTopicProbs)
        .map({case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic*pTopicGivenDoc })
        .sum
    }

    def udfScoreFunction = udf((ip: String, word: String) => scoreFunction(ip,word))


    val dataScored = dataWithWordDF.withColumn("score", udfScoreFunction(dataWithWordDF("ip_dst"), dataWithWordDF("word")))

//    val dataScored = dataWithWordDF.map(row => {
//      val topic_mix = topics.value.getOrElse(row(rawDataColumnWithIndex("ip_dst")), Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).asInstanceOf[Array[Double]]
//      val word_prob = words.value.getOrElse(row(rawDataColumnWithIndex("word")), Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).asInstanceOf[Array[Double]]
//      var src_score = 0.0
//      for (i <- 0 to 19) {
//        src_score += topic_mix(i) * word_prob(i)
//      }
//      (src_score, row :+ src_score)
//    })
//
//    addcol("score")

    logger.info("Persisting data")


    val filteredDF = dataScored.filter("score < " + threshold)

    val count = filteredDF.count

    val takeCount  = if (topK == -1 || count < topK) {
      count.toInt
    } else {
      topK
    }

    val scoreIndex = filteredDF.schema.fieldNames.indexOf("score")

    class DataOrdering() extends Ordering[(Double,Array[Any])] {
      def compare(p1: (Double, Array[Any]), p2: (Double, Array[Any]))    = p1._1.compare(p2._1)
    }

    implicit val ordering = new DataOrdering()

    val top : Array[Row] = filteredDF.rdd.takeOrdered(takeCount)

    val outputRDD = sc.parallelize(top).sortBy(row => row.getDouble(scoreIndex))

    outputRDD.map(_.mkString(","))saveAsTextFile(resultsFilePath)

    logger.info("DNS Post LDA completed")
  }

}
