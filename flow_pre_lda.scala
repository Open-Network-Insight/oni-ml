val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg._
import breeze.stats.DescriptiveStats._
import breeze.linalg._

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

def isNumeric(input: String): Boolean = {
    if (input == "") {false}
    else {input.forall(x => (x.isDigit || x == '.' || x =='E' || x == 'e')) && input.count(_ == '.')<2 && (input.count(_ == 'E') + input.count(_ == 'e')) <2}
}
def toDouble(s: String, default:Double=Double.NaN) = {if (isNumeric(s)) s.toDouble else default}
//def isHeader(line: Array[String], header: Array[String]) = line.deep == header.deep

def removeHeader(input: org.apache.spark.rdd.RDD[String]) = {
    val header = input.first
    val output = input.filter(line => !(line == header))
    output
}

def matrixToRdd(m: org.apache.spark.mllib.linalg.Matrix) : org.apache.spark.rdd.RDD[(Long, org.apache.spark.mllib.linalg.DenseVector)] = {
    val cols = m.toArray.grouped(m.numRows)
    val rows = cols.toSeq.transpose
    val vectors = rows.zipWithIndex.map{case (row, index) => (index.toLong, new org.apache.spark.mllib.linalg.DenseVector(row.toArray))}
    sc.parallelize(vectors)
}


def print(input: org.apache.spark.rdd.RDD[String]) = input.take(10).foreach(println)


// Load and parse the data

class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
}



val time_flow_index = 0
val year_flow_index = 1
val month_flow_index = 2
val day_flow_index = 3
val hour_flow_index = 4
val minute_flow_index = 5
val second_flow_index = 6
val tdur_flow_index = 7
val sip_flow_index = 8
val dip_flow_index = 9
val sport_flow_index = 10
val dport_flow_index = 11
val proto_flow_index = 12
val flag_flow_index = 13
val fwd_flow_index = 14
val stos_flow_index = 15
val ipkt_flow_index  = 16
val ibyt_flow_index = 17
val opkt_flow_index = 18
val obyt_flow_index = 19
val input_flow_index= 20
val output_flow_index = 21
val sas_flow_index = 22
val das_flow_index = 23
val dtos_flow_index = 24
val dir_flow_index  = 25
val rip_flow_index = 26

//----------Inputs-------------
//val file = "/user/history/hiveflow/netflow/year=2015/month=6/day=18/hour=0/*"
val file = System.getenv("DPATH")


val scoredFile = System.getenv("HPATH") + "/flow_scores.csv"
//val output_file = "/user/history/hiveflow/netflow/word_counts_for_20150618"
val output_file = System.getenv("HPATH") + "/word_counts"
//val output_file_for_lda = "/user/history/hiveflow/netflow/lda_word_counts_for_20150618"
val output_file_for_lda = System.getenv("HPATH") + "/lda_word_counts"

println("scoredFile:  " + scoredFile)
println("outputFile:  " + output_file)
println("output_file_for_lda:  " + output_file_for_lda)

val compute_quantiles : Boolean = true
val quant = Array(0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
val quint = Array(0, 0.2, 0.4, 0.6, 0.8)
var ibyt_cuts = new Array[Double](10)
var ipkt_cuts = new Array[Double](5)
var time_cuts = new Array[Double](10)
//val cuts_input = System.getenv("CUT")
//var ibyt_cuts : Array[Double] = cuts_input.split(",")(0).split(" ").map(_.toDouble)
//var ipkt_cuts : Array[Double] = cuts_input.split(",")(1).split(" ").map(_.toDouble)
//var time_cuts : Array[Double] = cuts_input.split(",")(2).split(" ").map(_.toDouble)

//-----------------------------

def compute_ecdf(x : org.apache.spark.rdd.RDD[Double]) : org.apache.spark.rdd.RDD[(Double, Double)] ={
    val counts = x.map( v => (v,1)).reduceByKey(_+_).sortByKey().cache()
    // compute the partition sums
    val partSums: Array[Double] = 0.0 +: counts.mapPartitionsWithIndex {
        case (index, partition) => Iterator(partition.map { case (sample, count) => count }.sum.toDouble)
    }.collect()

    // get sample size
    val numValues = partSums.sum

    // compute empirical cumulative distribution
    val sumsRdd = counts.mapPartitionsWithIndex {
        case (index, partition) => {
            var startValue = 0.0
            for (i <- 0 to index) {
                startValue += partSums(i)
            }
            partition.scanLeft((0.0, startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
        }
    }
    sumsRdd.map( elem => (elem._1, elem._2 / numValues))
}

def distributed_quantiles(quantiles: Array[Double], ecdf: org.apache.spark.rdd.RDD[(Double, Double)]): Array[Double] ={
    def dqSeqOp(acc: Array[Double], value: (Double, Double) ) : Array[Double]= {
        var newacc: Array[Double] = acc
        for ( (quant, pos) <- quantiles.zipWithIndex) {
            newacc(pos) = if (value._2 < quant ) {max(newacc(pos), value._1)}else{newacc(pos)}
        }
        acc
    }

    def dqCombOp(acc1: Array[Double], acc2: Array[Double]) = { (acc1 zip acc2).map(tuple => max(tuple._1, tuple._2)) }

    ecdf.aggregate(Array.fill[Double](quantiles.length)(0)) ((acc, value) =>dqSeqOp(acc, value), (acc1, acc2)=>dqCombOp(acc1, acc2))
}

def bin_column(value: String, cuts: Array[Double]) = {
    var bin = 0
    for (cut <- cuts){ if (value.toDouble > cut) { bin = bin+1 } }
    bin.toString
}


def convert_feedback_row_to_flow_row(feedBackRow: Array[String]) = {
    // when we
    val sev_feedback_index = 0
    val tstart_feedback_index = 1
    val srcIP_feedback_index = 2
    val dstIP_feedback_index = 3
    val sport_feedback_index = 4
    val dport_feedback_index = 5
    val proto_feedback_index = 6
    val flag_feedback_index = 7
    val ipkt_feedback_index = 8
    val ibyt_feedback_index = 9
    val lda_score_feedback_index = 10
    val rank_feedback_index = 11
    val srcIpInternal_feedback_index = 12
    val destIpInternal_feedback_index = 13
    val srcGeo_feedback_index = 14
    val dstGeo_feedback_index= 15
    val srcDomain_feedback_index = 16
    val dstDomain_feedback_index = 17
    val gtiSrcRep_feedback_index = 18
    val gtiDstRep_feedback_index = 19
    val norseSrcRep_feedback_index = 20
    val norseDstRep_feedback_index = 21

    val srcIP : String = feedBackRow(srcIP_feedback_index)
    val dstIP : String  = feedBackRow(dstIP_feedback_index)
    val sport : String = feedBackRow(sport_feedback_index)
    val dport : String = feedBackRow(dport_feedback_index)
    val tstart : String = feedBackRow(tstart_feedback_index)
    val ipkts : String = feedBackRow(ipkt_feedback_index)
    val ibyts : String = feedBackRow(ibyt_feedback_index)


    // it is assumed that the format of the time object coming from the feedback is
    //                  YYYY-MM-DD HH:MM:SS
    //   for example:   2016-04-21 03:58:13
    val hourMinSecond : Array[String] = tstart.split(' ')(1).split(':')  // todo: error handling if the line is malformed
    val hour = hourMinSecond(0)
    val min  = hourMinSecond(1)
    val sec = hourMinSecond(2)

    val buf  = new StringBuilder
    for (i <- 0 to 26) {
        if ( i == hour_flow_index) {
            buf ++= hour
        } else if (i == minute_flow_index) {
            buf ++= min
        } else if (i == second_flow_index) {
            buf ++= sec
        } else if (i == ipkt_flow_index) {
            buf ++= ipkts
        } else if (i == ibyt_flow_index) {
            buf ++= ibyts
        } else if (i == sport_flow_index) {
            buf ++= sport
        } else if (i == dport_flow_index) {
            buf ++= dport
        } else if (i == sip_flow_index) {
            buf ++= srcIP
        } else if ( i == dip_flow_index) {
            buf ++= dstIP
        } else {
            buf ++= " "
        }
        if (i < 26) {
            buf + ','
        }
    }
    buf.toString()
}

val rawdata = sc.textFile(file)
val datanoheader = removeHeader(rawdata)
val datagood = datanoheader.filter(line => line.split(",").length == 27)

// test if the scored file exists
val hadoopConf = sc.hadoopConfiguration
val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
val scoredFileExists = fs.exists(new org.apache.hadoop.fs.Path(scoredFile))



val totalData = if (scoredFileExists) {
    val scoreData : RDD[String] = sc.textFile(scoredFile).map(_.split(',')).map(convert_feedback_row_to_flow_row)
    val duplicationFactor = 1000 // we should read this from a config file
    val duplicatedScoreData = scoreData.flatMap(x=> List.fill(duplicationFactor)(x))
    datagood.union(duplicatedScoreData)
} else {
    datagood
}




def add_time(row: Array[String]) = {
    val num_time = row(4).toDouble + row(5).toDouble/60 + row(6).toDouble/3600
    row.clone :+ num_time.toString
}

val data_with_time = totalData.map(_.trim.split(",")).map(add_time)

if (compute_quantiles == true){
    println("calculating time cuts ...")
    time_cuts = distributed_quantiles(quant, compute_ecdf(data_with_time.map(row => row(27).toDouble )))
    println(time_cuts.mkString(",") )
    println("calculating byte cuts ...")
    ibyt_cuts = distributed_quantiles(quant, compute_ecdf(data_with_time.map(row => row(17).toDouble )))
    println(ibyt_cuts.mkString(",") )
    println("calculating pkt cuts")
    ipkt_cuts = distributed_quantiles(quint, compute_ecdf(data_with_time.map(row => row(16).toDouble )))
    println(ipkt_cuts.mkString(",") )
}

def bin_ibyt_ipkt_time(row: Array[String],
                       ibyt_cuts: Array[Double],
                       ipkt_cuts: Array[Double],
                       time_cuts: Array[Double]) = {
    val time = row(27).toDouble
    val ibyt = row(17).toDouble
    val ipkt = row(16).toDouble
    var time_bin = 0
    var ibyt_bin = 0
    var ipkt_bin = 0
    for (cut <- ibyt_cuts){
        if (ibyt > cut) { ibyt_bin = ibyt_bin+1 }
    }
    for (cut <- ipkt_cuts){
        if (ipkt > cut) { ipkt_bin = ipkt_bin+1 }
    }
    for (cut <- time_cuts){
        if (time > cut) { time_bin = time_bin+1 }
    }
    row.clone :+ ibyt_bin.toString :+ ipkt_bin.toString :+ time_bin.toString
}

//s1 = bin_ibyt_ipkt_time(row = s1, ibyt_cuts, ipkt_cuts, time_cuts)
val binned_data = data_with_time.map(row => bin_ibyt_ipkt_time(row, ibyt_cuts, ipkt_cuts, time_cuts))

def adjust_port(row: Array[String]) = {
    var word_port = 111111.0
    val sip = row(8)
    val dip = row(9)
    val dport = row(10).toDouble
    val sport = row(11).toDouble
    val ipkt_bin = row(29).toDouble
    val ibyt_bin = row(28).toDouble
    val time_bin = row(30).toDouble
    var p_case = 0.0

    var ip_pair = row(9) + " " + row(8)
    if (sip < dip & sip != 0) { ip_pair = row(8) + " " + row(9) }

    if ((dport <= 1024 | sport<=1024) & (dport > 1024 | sport>1024) & min(dport,sport) != 0){
        p_case = 2
        word_port = min(dport, sport)
    }else if (dport >1024 & sport>1024){
        p_case = 3
        word_port = 333333
    }else if (dport == 0 & sport != 0){
        word_port = sport
        p_case = 4
    }else if (sport == 0 & dport != 0){
        word_port = dport
        p_case = 4
    }else {
        p_case = 1
        if (min(dport, sport) == 0) {word_port = max(dport, sport)
        }else { word_port = 111111}
    }
    //val word = ipkt_bin -1 + (ibyt_bin-1)*10 + (time_bin - 1)*100 + word_port*1000
    val word = word_port.toString+"_"+time_bin.toString+"_"+ibyt_bin.toString+"_"+ipkt_bin.toString
    var src_word = word
    var dest_word = word

    if (p_case == 2 & dport < sport){ dest_word = "-1_"+dest_word
    }else if (p_case == 2 & sport < dport){  src_word = "-1_"+src_word
    }else if (p_case == 4 & dport == 0){  src_word = "-1_"+src_word
    }else if (p_case == 4 & sport ==0){ dest_word = "-1_"+dest_word }

    row.clone :+ word_port.toString :+ ip_pair :+ src_word.toString :+ dest_word.toString
}

//s1 = adjust_port(s1)
val data_with_words = binned_data.map(row => adjust_port(row))


//next groupby src to get src word counts
val src_word_counts = data_with_words.map(row => (row(8) + " " + row(33), 1)).reduceByKey(_ + _)


//groupby dest to get dest word counts
val dest_word_counts = data_with_words.map(row => (row(9) + " " + row(34), 1)).reduceByKey(_ + _)

//val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => Array(row._1.split(" ")(0).toString, row._1.split(" ")(1).toString, row._2).toString)
val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => (row._1.split(" ")(0)+","+ row._1.split(" ")(1).toString +","+ row._2).mkString)


//word_counts_strings.persist(StorageLevel.MEMORY_AND_DISK)
//word_counts_strings.take(10)

// see about saving to local file system
word_counts.saveAsTextFile(output_file)


//val result = word_counts.map( row => (row.split(",")(0), (row.split(",")(1).toDouble.asInstanceOf[Long], row.split(",")(2).toDouble.asInstanceOf[Long]))).aggregateByKey(Map[Long,Long]())( (accum,value) => accum +(value._1 -> value._2), (accum1, accum2) => accum1 ++ accum2).map{ case (ip: String, wordMap: Map[Long,Long]) => {
//    val array = wordMap.toArray
//    (ip, array.map(_._1), array.map(_._2))
//    }}

val result = word_counts.map( row => (row.split(",")(0), (row.split(",")(1), row.split(",")(2).toDouble.asInstanceOf[Long]))).aggregateByKey(Map[String,Long]())( (accum,value) => accum +(value._1 -> value._2), (accum1, accum2) => accum1 ++ accum2).map{ case (ip: String, wordMap: Map[String,Long]) => {
    val array = wordMap.toArray
    (ip, array)
}}

val result2 = result.map( row =>{
    val ip = row._1
    val array = row._2
    val num_words = array.length
    val x = array.map(t => t._1.toString+":"+t._2.toString).mkString(",")
    ip +","+num_words+","+x
})

//result2.take(10)
//result2.saveAsTextFile(output_file_for_lda)

System.exit(0)
