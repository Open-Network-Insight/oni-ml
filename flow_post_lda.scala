val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
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





//----------Inputs-------------
val file = System.getenv("FLOW_PATH")
val topic_mix_file = System.getenv("HPATH") + "/doc_results.csv"
val pword_file = System.getenv("HPATH")+"/word_results.csv"
val scored_output_file = System.getenv("HPATH") + "/scored"
val threshold : Double = System.getenv("TOL").toDouble

val compute_quantiles : Boolean = true
val quant = Array(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
val quint = Array(0, 0.2, 0.4, 0.6, 0.8)
var ibyt_cuts = new Array[Double](10)
var ipkt_cuts = new Array[Double](5)
var time_cuts = new Array[Double](10)

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

println("loading machine learning results")
val topics_lines = sc.textFile(topic_mix_file)
//print(topics_lines)
val words_lines = sc.textFile(pword_file)
//print(words_lines)

val l_topics = topics_lines.map(line => {
    val ip = line.split(",")(0)
    val text = line.split(",")(1)
    val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
    (ip,text_no_quote)
    }).map(elem => elem._1 -> elem._2).collectAsMap()

val topics = sc.broadcast(l_topics)

val l_words = words_lines.map(line => {
    val word = line.split(",")(0)
    val text = line.split(",")(1)
    val text_no_quote = text.replaceAll("\"", "").split(" ").map(v => v.toDouble)
    (word, text_no_quote)
    }).map(elem => elem._1 -> elem._2).collectAsMap()

val words = sc.broadcast(l_words)

println("loading data")
val rawdata = sc.textFile(file)
//2015-04-12 00:01:06,2015,4,12,0,1,6,1.340,10.0.121.115,192.168.1.33,80,54048,TCP,.AP.SF,0,0,9,5084,0,0,2,3,0,0,0,0,10.219.32.250

val datanoheader = removeHeader(rawdata)
val datagood = datanoheader.filter(line => line.split(",").length == 27)

//Array(tr, try, trm, trd, trh, trm, trs, td, sa, da, sp, dp, pr, flg, fwd, stos, ipkt, ibyt, opkt, obyt, in, out, sas, das, dtos, dir, ra)

def add_time(row: Array[String]) = {
    val num_time = row(4).toDouble + row(5).toDouble/60 + row(6).toDouble/3600
    row.clone :+ num_time.toString
}

val data_with_time = datagood.map(_.trim.split(",")).map(add_time)

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
    row :+ ibyt_bin.toString :+ ipkt_bin.toString :+ time_bin.toString
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
    
    row :+ word_port.toString :+ ip_pair :+ src_word.toString :+ dest_word.toString
}

//s1 = adjust_port(s1)
val data_with_words = binned_data.map(row => adjust_port(row))


val src_scored = data_with_words.map(row => {
	val topic_mix_1 = topics.value.getOrElse(row(8),Array(0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05) ).asInstanceOf[Array[Double]]
	val word_prob_1 = words.value.getOrElse(row(33),Array(0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05) ).asInstanceOf[Array[Double]]
	val topic_mix_2 = topics.value.getOrElse(row(9),Array(0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05) ).asInstanceOf[Array[Double]]
	val word_prob_2 = words.value.getOrElse(row(34),Array(0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05) ).asInstanceOf[Array[Double]]
        var src_score = 0.0 
        var dest_score = 0.0 
	for ( i <- 0 to 19) {
		 src_score += topic_mix_1(i) * word_prob_1(i)
		 dest_score += topic_mix_2(i) * word_prob_2(i)
      	}
	 (min(src_score, dest_score), row :+ src_score :+ dest_score)
	})


//src_scored.take(10)


var scored = src_scored.filter(elem => elem._1 < threshold).sortByKey().map( row => row._2.mkString(",") )

scored.persist(StorageLevel.MEMORY_AND_DISK)
scored.saveAsTextFile(scored_output_file)


System.exit(0)



















