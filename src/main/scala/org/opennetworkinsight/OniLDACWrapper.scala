package org.opennetworkinsight

import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File

import scala.io.Source._
import scala.sys.process._

/**
  * Contains routines for LDA including pre and post operations
  * 1. Creates list of unique documents, words and model based on those two
  * 2. Processes the model calling MPI
  * 3. Reads MPI results: Topic distributions per document and words per topic
  * 4. Calculates and returns probability of word given topic: p(w|z)
  */

object OniLDACWrapper {

  case class OniLDACInput(doc: String, word: String, count: Int) extends Serializable

  case class OniLDACOutput(docToTopicMix: Map[String, Array[Double]], wordResults: Map[String, Array[Double]])

  def runLDA(docWordCount: RDD[OniLDACInput],
             modelFile: String,
             topicDocumentFile: String,
             topicWordFile: String,
             mpiPreparationCmd: String,
             mpiCmd: String,
             mpiProcessCount: String,
             mpiTopicCount: String,
             localPath: String,
             ldaPath: String,
             localUser: String,
             dataSource: String,
             nodes: String):   OniLDACOutput =  {

    // Create word Map Word,Index for further usage
    val wordDictionary: Map[String, Int] = {
      val words = docWordCount
        .cache
        .map({case OniLDACInput(doc, word, count) => word})
        .distinct
        .collect
      words.zipWithIndex.toMap
    }

    val distinctDocument = docWordCount.map({case OniLDACInput(doc, word, count) => doc}).distinct.collect
    //distinctDocument.cache()

    // Create document Map Index, Document for further usage
    val documentDictionary: Map[Int, String] = {
      distinctDocument
        //.collect
        .zipWithIndex
        .sortBy(_._2)
        .map(kv => (kv._2, kv._1))
        .toMap
    }

    // Create model for MPI
    val model = createModel(docWordCount, wordDictionary, distinctDocument)

    // Persist model.dat
    val modelWriter = new PrintWriter(new File(modelFile))
    model foreach (row => modelWriter.write("%s\n".format(row)))
    modelWriter.close()

    // Copy model.dat to each machinefile node
    val nodeList = nodes.replace("'","").split(",")
    for (node <- nodeList){
      sys.process.Process(Seq("ssh", node, "mkdir " + localUser + "/ml/" + dataSource)).!
      sys.process.Process(Seq("scp", "-r", localPath, node + ":" + localUser + "/ml/" + dataSource )).!
    }

    // Execute Pre MPI command
    if(mpiPreparationCmd != "" && mpiPreparationCmd != null)
      stringToProcess(mpiPreparationCmd).!!

    // Execute MPI

    sys.process.Process(Seq(mpiCmd, "-n", mpiProcessCount, "-f", "machinefile", "./lda", "est", "2.5",
      mpiTopicCount, "settings.txt", mpiProcessCount, modelFile, "random", localPath), new java.io.File(ldaPath)) #> (System.out) !!

    // Read topic info per document

    val topicDocumentFileExists = if (topicDocumentFile != "") new File(topicDocumentFile).exists else false
    val topicWordFileExists = if (topicWordFile != "") new File(topicWordFile).exists() else false

    val documentTopicMixRawLines = {
      if (topicDocumentFileExists) {
        fromFile(topicDocumentFile).getLines().toArray
      }
      else Array[String]()
    }

    // Read words per topic
    val topicWordData = {
      if (topicWordFileExists) {
        fromFile(topicWordFile).getLines().toArray
      }
      else Array[String]()
    }

    // Create document results
    val docToTopicMix = getDocumentResults(documentTopicMixRawLines, documentDictionary)

    // Create word results
    val wordResults = getWordToProbPerTopicMap(topicWordData, wordDictionary)

    OniLDACOutput(docToTopicMix, wordResults)
  }

  /**
    * getWordProbabilitiesFromTopicLine
    *
    * @param topicLine A line of text encoding the probabilities of the word given a topic.
    * @return probability of each word conditioned on this topic
    */
  def getWordProbabilitesFromTopicLine(topicLine: String) : Array[Double] = {

    val logWordProbs: Array[Double] = topicLine.trim().split(" ").map(_.toDouble)

    val wordProbs: Array[Double] = logWordProbs.map(math.exp)

    // renormalize to account for any weirdness from the log/exp transformations
    val sumRawWord = wordProbs.sum
    wordProbs.map(_ / sumRawWord)
  }

  def getTopicDocument(document: String, line: String) : (String, Array[Double])  = {
    val topics = line.split(" ").map(_.toDouble)
    val topicsSum = topics.sum

    if (topicsSum > 0) {
      val topicsProb = topics.map(_ / topicsSum)
      document -> topicsProb
    }
    else {
      val topicsProb = Array.fill(20)(0d)
      document ->  topicsProb
    }
  }

  def createModel(documentWordData: RDD[OniLDACInput], wordToIndex: Map[String, Int], distinctDocument: Array[String])
  : Array[String]
  = {
    val documentCount = documentWordData
      .map({case OniLDACInput(doc, word, count) => doc})
      .map(document => (document, 1))
      .reduceByKey(_ + _)
      .collect
      .toMap

    val wordIndexdocWordCount = documentWordData
      .map({case OniLDACInput(doc, word, count) => (doc, wordToIndex(word) + ":" + count)})
      .groupByKey()
      .map(x => (x._1, x._2.mkString(" ")))
      .collect
      .toMap

    distinctDocument
      //.collect
      .map(doc => documentCount(doc)
        + " "
        + wordIndexdocWordCount(doc))
  }

  def getDocumentResults(topicDocumentData: Array[String],
                         docIndexToDocument: Map[Int, String]) : Map[String, Array[Double]] = {

    topicDocumentData.zipWithIndex
      .map({case (topic, docIdx) => getTopicDocument(docIndexToDocument(docIdx), topic)})
      .toMap
  }

  def getWordToProbPerTopicMap(topicWordData: Array[String],
                               wordToIndex: Map[String, Int]): Map[String, Array[Double]] = {


    val probabilityOfWordGivenTopic = topicWordData.map(getWordProbabilitesFromTopicLine).transpose

    val indexToWord = {
      val addedIndex = wordToIndex.size
      val tempWordDictionary = wordToIndex + ("0_0_0_0_0" -> addedIndex)
      tempWordDictionary.map({
        case (k, v) => (v, k)
      })
    }

    probabilityOfWordGivenTopic.zipWithIndex.map({ case (probOfWordGivenTopic, index) => (indexToWord(index) ->
      probOfWordGivenTopic)}).toMap

  }
}



