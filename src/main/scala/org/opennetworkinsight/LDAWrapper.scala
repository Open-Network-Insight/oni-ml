package org.opennetworkinsight

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter
import java.io.File

import scala.math
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

  def runLDA(docWordCount: RDD[Array[String]], modelFile, topicDocumentFile, topicWordFile, wordProbabilityFile,
             mpiPreparationCmd, mpiCmd, mpiProcessCount, mpiTopicCount, ldaOutputPath)
  = {

    // Create word Map Word,Index for further usage
    val wordDictionary: Map[String, Int] = {
      val words = docWordCount
        .cache
        .map(row => row(1))
        .distinct()
        .toArray()
      words.zipWithIndex.toMap
    }

    val distinctDocument = docWordCount.map(row => row(0)).distinct

    // Create document Map Index, Document for further usage
    val documentDictionary: Map[Int, String] = {
      distinctDocument
        .toArray()
        .zipWithIndex
        .sortBy(_._2)
        .map(kv => (kv._2, kv._1))
        .toMap
    }

    // Create model for MPI
    val model = {
      val documentCount = docWordCount
        .map(row => row(0))
        .map(document => (document, 1))
        .reduceByKey(_ + _)
        .toArray
        .toMap

      val wordIndexdocWordCount = docWordCount
        .map(row => (row(0), wordDictionary(row(1)) + ":" + row(2)))
        .groupByKey()
        .map(x => (x._1, x._2.mkString(" ")))
        .toArray
        .toMap

      distinctDocument
        .toArray()
        .map(doc => documentCount(doc)
          + " "
          + wordIndexdocWordCount(doc))
    }

    // Persis model.dat
    val modelWriter = new PrintWriter(new File(modelFile))
    model foreach ((row) => modelWriter.write("%s\n".format(row)))
    modelWriter.close()

    // Execute MPI
    /*${MPI_PREP_CMD}
    time ${MPI_CMD} -n ${PROCESS_COUNT} -f machinefile ./lda est 2.5 ${TOPIC_COUNT} settings.txt \
      ${PROCESS_COUNT} ../${LDA_OUTPUT_DIR}/model.dat random ../${LDA_OUTPUT_DIR}
    wait*/
    var mpiPreparationCmdResult = mpiPreparationCmd.!!
    val result = sys.process.Process(Seq(mpiCmd, "-n", mpiProcessCount, "-f", "machinefile", "./lda", "est", "2.5",
      mpiTopicCount, "settings.txt", mpiProcessCount, modelFile, "random", ldaOutputPath),
      new java.io.File("/home/duxbury/ml/oni-lda-c")).!!

    // Read topic info per document

    val topicDocumentFileExists = if (topicDocumentFile != "") new File(topicDocumentFile).exists else false
    val topicWordFileExists = if (topicWordFile != "") new File(topicWordFile).exists() else false

    val topicDocumentData = {
      if (topicDocumentFileExists) {
        fromFile(topicDocumentFile).getLines().toArray
      }
      else Array[String]()
    }

    // Create document results
    val documentTopic = topicDocumentData.zipWithIndex.map({
      case (k, v) => getTopicDocument(documentDictionary(v), k)
    })

    // Read words per topic
    val topicWordData = {
      if (topicWordFileExists) {
        fromFile(topicWordFile).getLines().toArray
      }
      else Array[String]()
    }

    // invert wordDictionary Map[Int, String]
    val indexWordDictionary = {
      val addedIndex = wordDictionary.size
      val tempWordDictionary = wordDictionary + ("0_0_0_0_0" -> addedIndex)
      tempWordDictionary.map({
        case (k, v) => (v, k)
      })
    }

    // Normalize p(w|z)
    val pwgz = topicWordData.map(normalizeWord).transpose

    // Create word results
    val wordTopic = pwgz.zipWithIndex.map({ case (k, v) => indexWordDictionary(v) + "," + k.mkString(" ") })

  }

  def normalizeWord(wordProbability: String)
  = {

    val topics: Array[Double] = wordProbability.trim().split(" ").map(_.toDouble).toArray
    // calculate the exp of each element and return array
    val rawWord: Array[Double] = topics.map(math.exp(_)).toArray
    // sum all exponentials
    val sumRawWord = rawWord.sum
    // calculate normalized value for each element: for each each val => exp(val)/sum
    rawWord.map(_ / sumRawWord).toArray

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

}



