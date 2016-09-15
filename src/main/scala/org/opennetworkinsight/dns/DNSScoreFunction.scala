package org.opennetworkinsight.dns

import org.apache.spark.broadcast.Broadcast
import org.opennetworkinsight.SuspiciousConnectsScoreFunction


class DNSScoreFunction(frameLengthCuts: Array[Double],
                       timeCuts: Array[Double],
                       subdomainLengthCuts: Array[Double],
                       entropyCuts: Array[Double],
                       numberPeriodsCuts: Array[Double],
                       topicCount: Int,
                       ipToTopicMixBC: Broadcast[Map[String, Array[Double]]],
                       wordToPerTopicProbBC: Broadcast[Map[String, Array[Double]]],
                       countryCodesBC: Broadcast[Set[String]],
                       topDomainsBC: Broadcast[Set[String]]) extends Serializable {


  val suspiciousConnectsScoreFunction =
    new SuspiciousConnectsScoreFunction(topicCount, ipToTopicMixBC, wordToPerTopicProbBC)


  def score(timeStamp: String,
            unixTimeStamp: Long,
            frameLength: Int,
            clientIP: String,
            queryName: String,
            queryClass: String,
            queryType: Int,
            queryResponseCode: Int) : Double = {

    val word = DNSWordCreation.dnsWord(timeStamp,
      unixTimeStamp,
      frameLength,
      clientIP,
      queryName,
      queryClass,
      queryType,
      queryResponseCode,
      frameLengthCuts,
      timeCuts,
      subdomainLengthCuts,
      entropyCuts,
      numberPeriodsCuts,
      countryCodesBC,
      topDomainsBC)

    suspiciousConnectsScoreFunction.score(clientIP,word)
  }
}
