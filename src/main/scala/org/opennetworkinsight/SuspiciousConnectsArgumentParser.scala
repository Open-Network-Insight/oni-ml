package org.opennetworkinsight


/**
  * Parses arguments for the suspicious connections analysis.
  */
object SuspiciousConnectsArgumentParser {

  case class Config(analysis: String = "",
                    inputPath: String = "",
                    scoresFile: String = "",
                    duplicationFactor: Int = 1,
                    modelFile: String = "",
                    topicDocumentFile: String = "",
                    topicWordFile: String = "",
                    mpiPreparationCmd: String = "",
                    mpiCmd: String = "",
                    mpiProcessCount: String = "",
                    mpiTopicCount: String = "",
                    localPath: String = "",
                    localUser: String = "",
                    ldaPath: String = "",
                    dataSource: String = "",
                    nodes: String = "",
                    hdfsScoredConnect: String = "",
                    threshold: Double = 1.0d)

  val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("LDA") {

    head("LDA Process", "1.1")

    opt[String]('z', "analysis").required().valueName("< flow | proxy | dns >").
      action((x, c) => c.copy(analysis = x)).
      text("choice of suspicious connections analysis to perform")

    opt[String]('i', "input").required().valueName("<hdfs path>").
      action((x, c) => c.copy(inputPath = x)).
      text("HDFS path to netflow records")

    opt[String]('f', "feedback").valueName("<local file>").
      action((x, c) => c.copy(scoresFile = x)).
      text("the local path of the file that contains the feedback scores")

    opt[Int]('d', "dupfactor").valueName("<non-negative integer>").
      action((x, c) => c.copy(duplicationFactor = x)).
      text("duplication factor controlling how to downgrade non-threatening connects from the feedback file")

    opt[String]('m', "model").required().valueName("<local file>").
      action((x, c) => c.copy(modelFile = x)).
      text("Model file location")

    opt[String]('o', "topicdoc").required().valueName("<local file>").
      action((x, c) => c.copy(topicDocumentFile = x)).
      text("final.gamma file location")

    opt[String]('w', "topicword").required().valueName("<local file>").
      action((x, c) => c.copy(topicWordFile = x)).
      text("final.beta file location")

    opt[String]('p', "mpiprep").valueName("<mpi command>").
      action((x, c) => c.copy(mpiPreparationCmd = x)).
      text("MPI preparation command")

    opt[String]('c', "mpicmd").required().valueName("<mpi command>").
      action((x, c) => c.copy(mpiCmd = x)).
      text("MPI command")

    opt[String]('t', "proccount").required().valueName("<mpi param>").
      action((x, c) => c.copy(mpiProcessCount = x)).
      text("MPI process count")

    opt[String]('u', "topiccount").required().valueName("<mpi param>").
      action((x, c) => c.copy(mpiTopicCount = x)).
      text("MPI topic count")

    opt[String]('l', "lpath").required().valueName("<local path>").
      action((x, c) => c.copy(localPath = x)).
      text("Local Path")

    opt[String]('a', "ldapath").required().valueName("<local path>").
      action((x, c) => c.copy(ldaPath = x)).
      text("LDA Path")

    opt[String]('r', "luser").required().valueName("<local path>").
      action((x, c) => c.copy(localUser = x)).
      text("Local user path")

    opt[String]('s', "dsource").required().valueName("<input param>").
      action((x, c) => c.copy(dataSource = x)).
      text("Data source")

    opt[String]('n', "nodes").required().valueName("<input param>").
      action((x, c) => c.copy(nodes = x)).
      text("Node list")

    opt[String]('h', "scored").required().valueName("<hdfs path>").
      action((x, c) => c.copy(hdfsScoredConnect = x)).
      text("HDFS path for results")

    opt[Double]('e', "threshold").required().valueName("float64").
      action((x, c) => c.copy(threshold = x)).
      text("probability threshold for declaring anomalies")
  }
}
