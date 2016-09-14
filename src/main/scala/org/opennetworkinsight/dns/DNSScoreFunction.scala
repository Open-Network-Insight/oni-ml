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
                       wordToPerTopicProbBC: Broadcast[Map[String, Array[Double]]]) extends Serializable {


  val suspiciousConnectsScoreFunction =
    new SuspiciousConnectsScoreFunction(topicCount, ipToTopicMixBC, wordToPerTopicProbBC)


  def score(timeStamp: String,
            unixTimeStamp: String,
            frameLength: Int,
            clientIP: String,
            queryName: String,
            queryClass: String,
            queryType: String,
            queryResponseCode: String) : Double = {

0d
  }
}
