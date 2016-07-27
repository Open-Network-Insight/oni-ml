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

object LDAWrapper {

  def runLDA(docWordCount: RDD[String], modelFile: String, topicDocumentFile: String, topicWordFile: String,
             mpiPreparationCmd: String, mpiCmd: String, mpiProcessCount: String, mpiTopicCount: String,
             localPath: String, ldaPath: String, localUser: String, dataSource: String, nodes: String):
  (Array[String], Array[String])
  =
  {

    val documentWordData = docWordCount.map(_.split(","))
    // Create word Map Word,Index for further usage
    val wordDictionary: Map[String, Int] = {
      val words = documentWordData
        .cache
        .map(row => row(1))
        .distinct
        .collect
      words.zipWithIndex.toMap
    }

    val distinctDocument = documentWordData.map(row => row(0)).distinct.collect
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
    val model = createModel(documentWordData, wordDictionary, distinctDocument)

    // Persis model.dat
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
      mpiTopicCount, "settings.txt", mpiProcessCount, modelFile, "random", localPath), new java.io.File(ldaPath)).!!

    // Read topic info per document

    val topicDocumentFileExists = if (topicDocumentFile != "") new File(topicDocumentFile).exists else false
    val topicWordFileExists = if (topicWordFile != "") new File(topicWordFile).exists() else false

    val topicDocumentData = {
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
    val documentResults = getDocumentResults(topicDocumentData, documentDictionary)

    // Create word results
    val wordResults = getWordResults(topicWordData, wordDictionary)

    (documentResults, wordResults)

  }

  def normalizeWord(wordProbability: String)
  = {

    val topics: Array[Double] = wordProbability.trim().split(" ").map(_.toDouble)
    // calculate the exp of each element and return array
    val rawWord: Array[Double] = topics.map(math.exp)
    // sum all exponential
    val sumRawWord = rawWord.sum
    // calculate normalized value for each element: for each each val => exp(val)/sum
    rawWord.map(_ / sumRawWord)

  }

  def getTopicDocument(document: String, line: String)
  = {
    val topics = line.split(" ").map(_.toDouble)
    val topicsSum = topics.sum

    if (topicsSum > 0) {
      val topicsProb = topics.map(_ / topicsSum)
      document + "," + topicsProb.mkString(" ")
    }
    else {
      val topicsProb = List.fill(20)("0.0")
      document + "," + topicsProb.mkString(" ")
    }
  }

  def createModel(documentWordData: RDD[Array[String]], wordDictionary: Map[String, Int], distinctDocument: Array[String])
  : Array[String]
  = {
    val documentCount = documentWordData
      .map(row => row(0))
      .map(document => (document, 1))
      .reduceByKey(_ + _)
      .collect
      .toMap

    val wordIndexdocWordCount = documentWordData
      .map(row => (row(0), wordDictionary(row(1)) + ":" + row(2)))
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

  def getDocumentResults(topicDocumentData: Array[String], documentDictionary: Map[Int, String])
  = {
    topicDocumentData.zipWithIndex.map({
      case (k, v) => getTopicDocument(documentDictionary(v), k)
    })
  }

  def getWordResults(topicWordData: Array[String], wordDictionary: Map[String, Int])
  ={
    // invert wordDictionary Map[Int, String]
    val indexWordDictionary = {
      val addedIndex = wordDictionary.size
      val tempWordDictionary = wordDictionary + ("0_0_0_0_0" -> addedIndex)
      tempWordDictionary.map({
        case (k, v) => (v, k)
      })
    }

    // Normalize p(w|z)
    val probabilityOfWordGivenTopic = topicWordData.map(normalizeWord).transpose

    probabilityOfWordGivenTopic.zipWithIndex.map({ case (k, v) => indexWordDictionary(v) +
      "," + k.mkString(" ") })

  }
}



