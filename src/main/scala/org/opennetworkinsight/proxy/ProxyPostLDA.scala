package org.opennetworkinsight.proxy

import org.apache.log4j.{Logger => ApacheLogger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opennetworkinsight.utilities._
import org.slf4j.Logger
import  org.opennetworkinsight.proxy.{ProxySchema => Schema}
/**
  *
  */
object ProxyPostLDA {

  def getResults(inputPath: String, resultsFilePath: String, topicCount: Int, threshold: Double, topK: Int,
                 documentResults: Array[String],  wordResults: Array[String],
                 sc: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("Proxy post LDA starts")

    val ipToTopicMix : Map[String, Array[Double]] = getIpToTopicMix(documentResults)

    val wordsToProbPerTopic : Map[String, Array[Double]]   = wordResults.map(line => {
      val word = line.split(",")(0)
      val probPerTopic = line.split(",")(1).split(' ').map(_.toDouble)
      (word, probPerTopic)
    }).map({case (word, probPerTopic)  => word -> probPerTopic}).toMap


    val rawDataDF = sqlContext.parquetFile(inputPath)
      .filter(Schema.Date +  " is not null and " + Schema.Time + " is not null and " + Schema.ClientIP + " is not null")
      .select(Schema.Date,
        Schema.Time,
        Schema.ClientIP,
        Schema.Host,
        Schema.ReqMethod,
        Schema.UserAgent,
        Schema.ResponseContentType,
        Schema.Duration,
        Schema.UserName,
        Schema.WebCat,
        Schema.Referer,
        Schema.RespCode,
        Schema.URIPort,
        Schema.URIPath,
        Schema.URIQuery,
        Schema.ServerIP,
        Schema.SCBytes,
        Schema.CSBytes,
        Schema.FullURI)

    logger.info("Computing conditional probability")
    val scoredDF : DataFrame = score(sc, rawDataDF, topicCount, ipToTopicMix, wordsToProbPerTopic)

    val filteredDF = scoredDF.filter("score < " + threshold)

    val count = filteredDF.count

    val takeCount  = if (topK == -1 || count < topK) {
      count.toInt
    } else {
      topK
    }

    val scoreIndex = filteredDF.schema.fieldNames.indexOf("score")

    class DataOrdering() extends Ordering[Row] {
      def compare(row1: Row, row2: Row) = row1.getDouble(scoreIndex).compare(row2.getDouble(scoreIndex))
    }

    implicit val rowOrdering = new DataOrdering()
    val topRows : Array[Row] = filteredDF.rdd.takeOrdered(takeCount)

    val outputRDD = sc.parallelize(topRows).sortBy(row => row.getDouble(scoreIndex))
    outputRDD.map(_.mkString("\t")).saveAsTextFile(resultsFilePath)


    logger.info("Persisting data")
    logger.info("proxy post LDA completed")
  }

  def getIpToTopicMix(documentResults: Array[String]) : Map[String, Array[Double]] = {
    documentResults.map(line => {
      val ip = line.split(",")(0)
      val topicProbs = line.split(",")(1).split(' ').map(_.toDouble)
      (ip, topicProbs)
    }).map({ case (ip, topicProbs) => ip -> topicProbs }).toMap
  }

  def score(sc: SparkContext, dataFrame: DataFrame, topicCount: Int, ipToTopicMIx: Map[String, Array[Double]],
            wordToPerTopicProb: Map[String, Array[Double]]) : DataFrame = {

    val topDomains: Broadcast[Set[String]] = sc.broadcast(TopDomains.TOP_DOMAINS)

    def getTimeAsDouble(timeStr: String) = {
      val s = timeStr.split(":")
      val hours = s(0).toInt
      val minutes = s(1).toInt
      val seconds = s(2).toInt

      (3600*hours + 60*minutes + seconds).toDouble
    }

    val timeCuts =
      Quantiles.computeDeciles(dataFrame.select(Schema.Time).rdd.map(r => getTimeAsDouble(r(0).toString())))

    val entropyCuts = Quantiles.computeQuintiles(dataFrame.select(Schema.FullURI).
      rdd.map({case Row(uri: String) => Entropy.stringEntropy(uri)}))

    val agentToCount: Map[String, Long] =
      dataFrame.select(Schema.UserAgent).rdd.map({case Row(ua: String) => (ua,1L)}).reduceByKey(_+_).collect().toMap

    val agentToCountBC = sc.broadcast(agentToCount)

    val agentCuts = Quantiles.computeQuintiles(dataFrame.select(Schema.UserAgent).rdd.map({case Row(ua: String) => agentToCountBC.value(ua)}))

    val udfWordCreation = ProxyWordCreation.udfWordCreation(topDomains, agentToCountBC, timeCuts, entropyCuts, agentCuts)

    val wordedDataFrame = dataFrame.withColumn(Schema.Word,  udfWordCreation(dataFrame(Schema.Host), dataFrame(Schema.Time),
      dataFrame(Schema.ReqMethod), dataFrame(Schema.FullURI), dataFrame(Schema.ResponseContentType),
      dataFrame(Schema.UserAgent), dataFrame(Schema.RespCode)))

    val ipToTopicMixBC = sc.broadcast(ipToTopicMIx)
    val wordToPerTopicProbBC  = sc.broadcast(wordToPerTopicProb)

    def scoreFunction(ip: String, word: String) : Double = {

      val uniformProb = Array.fill(topicCount){1.0d/topicCount}

      val topicGivenDocProbs  = ipToTopicMixBC.value.getOrElse(ip, uniformProb)
      val wordGivenTopicProbs = wordToPerTopicProbBC.value.getOrElse(word, uniformProb)

      topicGivenDocProbs.zip(wordGivenTopicProbs)
        .map({case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic*pTopicGivenDoc })
        .sum
    }

    def udfScoreFunction = udf((ip: String, word: String) => scoreFunction(ip,word))
    wordedDataFrame.withColumn(Schema.Score, udfScoreFunction(wordedDataFrame(Schema.ClientIP), wordedDataFrame(Schema.Word)))
  }
}
