val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import breeze.stats.DescriptiveStats._
import breeze.linalg._
import scala.io.Source
import scala.math._
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


def get_column_names(input: org.apache.spark.rdd.RDD[String], sep : Char = ',') : scala.collection.mutable.Map[String, Int] = {
    val columns = scala.collection.mutable.Map[String, Int]()
    val header = input.first.split(sep).zipWithIndex
    header.foreach(tuple => columns(tuple._1) = tuple._2)
    columns
}

def print_columns(columns : scala.collection.mutable.Map[String, Int]) = {
    val arr = columns.toArray
    arr.sortBy(_._2).foreach(println)
}


def print(input: org.apache.spark.rdd.RDD[String]) = input.take(10).foreach(println)

def print_rdd(input: org.apache.spark.rdd.RDD[Array[String]]) = input.take(20).foreach(m => println(m.mkString(",") ))

// Load and parse the data

class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
}





//----------Inputs-------------
//val file = System.getenv("DPATH")
val file_list = System.getenv("DPATH")
val topic_mix_file =System.getenv("HPATH")  + "/doc_results.csv"
val pword_file = System.getenv("HPATH") + "/word_results.csv"
val scored_output_file = System.getenv("HPATH") + "/scored"
val threshold : Double = System.getenv("TOL").toDouble

val quant = Array(0, 0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
val quint = Array(0, 0.2, 0.4, 0.6, 0.8)
var time_cuts = new Array[Double](10)
var frame_length_cuts = new Array[Double](5)
var subdomain_length_cuts = new Array[Double](5)
var numperiods_cuts = new Array[Double](5)
var entropy_cuts = new Array[Double](5)
val compute_quantiles = true

val l_top_domains = Source.fromFile("top-1m.csv").getLines.map(line => {
                      val parts = line.split(",")
                     val l = parts.length
                    parts(1).split("[.]")(0)
                    }).toSet
val top_domains = sc.broadcast(l_top_domains)

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

//-----------------------------

var multidata = {
    var tempRDD: org.apache.spark.rdd.RDD[String] = sc.textFile( file_list.split(",")(0) )
    val files = file_list.split(",")
    for ( (file, index) <- files.zipWithIndex){
        if (index > 1) {tempRDD = tempRDD.union(sc.textFile(file))}
    }
    tempRDD
}
var rawdata :org.apache.spark.rdd.RDD[String] = {
     multidata
}

// only needed if the schema is different than the standard solution setup
//val indices: Array[Int] = Array(0,1,2,8,9,10,11,12,13,14)
//rawdata = rawdata.map(line => line.split(",")).map(inner => indices.map(inner).mkString(","))
rawdata.take(10).foreach(println)

val col = get_column_names(rawdata)

//frame.time_epoch  frame.len  ip.src  ip.dst  dns.qry.name  dns.qry.type  dns.qry.class  dns.flags.rcode  dns.a 

def addcol(colname: String) = if (!col.keySet.exists(_==colname) ){col(colname) = col.values.max+1}



val datanoheader = removeHeader(rawdata)
//print_rdd(datagood)
val datagood = datanoheader.map(line => line.split(",") ).filter(line => (line.length == 9))

//val databad = datanoheader.filter(line => (line.split(",").length != 24 & line.split(",").length != 23))

print_rdd(datagood)
println(datagood.count())

val l_country_codes = Set("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bm", "bn", "bo", "bq", "br", "bs", "bt", "bv", "bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr", "cu", "cv", "cw", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "io", "iq", "ir", "is", "it", "je", "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "krd", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mk", "ml", "mm", "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "", "sk", "sl", "sm", "sn", "so", "sr", "ss", "st", "su", "sv", "sx", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "ye", "yt", "za", "zm", "zw")

val country_codes = sc.broadcast(l_country_codes)


def extract_subdomain(url: String): Array[String ]= {



       
    var spliturl = url.split("[.]")
    var numparts = spliturl.length
    var domain = "None"
    var subdomain = "None"
    //var country ="None"
    //var tld = "None"
    var host = {if (numparts>0){spliturl(0)} else "None" }
    
    //first check if query is an Ip address e.g.: 123.103.104.10.in-addr.arpa or a name
    val is_ip ={ if (numparts>2){if (spliturl(numparts-1) == "arpa" & spliturl(numparts-2) == "in-addr"){"IP"} else "Name"} else "Unknown" }
    
    if (numparts>2 & is_ip !="IP"){
        //This might try to parse things with only 1 or 2 numparts
        //test if last element is a country code or tld
        //use: Array(spliturl(numparts-1)).exists(country_codes contains _)
        // don't use: country_codes.exists(spliturl(numparts-1).contains) this doesn't test exact match, might just match substring
        if (Array(spliturl(numparts-1)).exists(country_codes.value contains _)){
            //country = spliturl(numparts-1)
            //tld = {if(Array(spliturl(numparts-2)).exists(tlds.value contains _)){spliturl(numparts-2)} else "None" }
            domain = spliturl(numparts-3)
            if (1<numparts-3){ subdomain = spliturl.slice(1,numparts-3).mkString(".") } 
        }
        else{
            //tld = {if(Array(spliturl(numparts-1)).exists(tlds contains _)){spliturl(numparts-1)} else "None" }
            domain = spliturl(numparts-2)
            if (1<numparts-2){ subdomain = spliturl.slice(1,numparts-2).mkString(".") }
        }
    }
    //Array(domain, subdomain, host, country, tld, numparts.toString, { if (subdomain !="None"){subdomain.length.toString} else {"0"}}, url.length.toString, is_ip )
    Array( domain,subdomain, { if (subdomain !="None"){subdomain.length.toString} else {"0"}}, numparts.toString)
}
println("Computing subdomain info")

var data_with_subdomains = datagood.map(row => row ++ extract_subdomain(row(col("dns.qry.name")) ) )
addcol("domain")
addcol("subdomain")
addcol("subdomain.length")
addcol("num.periods")

print_rdd(data_with_subdomains)
println(data_with_subdomains.count())


def bin_column(value: String, cuts: Array[Double]) = {
    var bin = 0
    for (cut <- cuts){ if (value.toDouble > cut) { bin = bin+1 } }
    bin.toString
}
 
def entropy( v:String ) : Double = { v
  .groupBy (a => a)
  .values
  .map( i => i.length.toDouble / v.length )
  .map( p => -p * log10(p) / log10(2))
  .sum
}

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

data_with_subdomains = data_with_subdomains.map(data => data :+ entropy(data(col("subdomain"))).toString )
addcol("subdomain.entropy")

//frame.time_epoch  frame.len  ip.src  ip.dst  dns.qry.name  dns.qry.type  dns.qry.class  dns.flags.rcode  dns.a 


if (compute_quantiles == true){
    //println("calculating time cuts ...")
    //time_cuts = distributed_quantiles(quant, compute_ecdf(data_with_subdomains.map(r => r(col("unix_timestamp")).toDouble )))
    //println(time_cuts.mkString(",") )
    println("calculating frame length cuts ...")
    frame_length_cuts = distributed_quantiles(quant, compute_ecdf(data_with_subdomains.map(r => r(col("frame.len")).toDouble )))
    println(frame_length_cuts.mkString(",") )
    println("calculating subdomain length cuts ...")
    subdomain_length_cuts = distributed_quantiles(quint, compute_ecdf(data_with_subdomains.filter(r => r(col("subdomain.length")).toDouble > 0 ).map(r => r(col("subdomain.length")).toDouble )))
    println(subdomain_length_cuts.mkString(",") )
    println("calculating entropy cuts")
    entropy_cuts = distributed_quantiles(quint, compute_ecdf(data_with_subdomains.filter(r => r(col("subdomain.entropy")).toDouble > 0 ).map(r => r(col("subdomain.entropy")).toDouble )))
    println(entropy_cuts.mkString(",") )
    println("calculating num periods cuts ...")
    numperiods_cuts = distributed_quantiles(quint, compute_ecdf(data_with_subdomains.filter(r => r(col("num.periods")).toDouble > 0 ).map(r => r(col("num.periods")).toDouble )))
    println(numperiods_cuts.mkString(",") )
}
println("count after cuts")
println(data_with_subdomains.count())

                        
//println("loading top domains")
//println(top_domains.count())
top_domains.value.take(10).foreach(println)

var data = data_with_subdomains.map(line => line :+ {if(line(col("domain")) == "intel"){"2"} else if(top_domains.value contains line(col("domain")) ) {"1"} else "0"})
addcol("top_domain")
print_rdd(data)
//frame.time_epoch  frame.len  ip.src  ip.dst  dns.qry.name  dns.qry.type  dns.qry.class  dns.flags.rcode  dns.a 

println("adding words")
data = data.map(row => {
        val word = row(col("top_domain")) + "_" + bin_column(row(col("frame.len")), frame_length_cuts) + "_" + //  bin_column(row(col("unix_timestamp")), time_cuts) + "_" +
        	bin_column(row(col("subdomain.length")), subdomain_length_cuts) + "_" +
                bin_column(row(col("subdomain.entropy")), entropy_cuts) + "_" +  
		bin_column(row(col("num.periods")), numperiods_cuts) + "_" + row(col("dns.qry.type"))+ "_" +row(col("dns.flags.rcode"))
row :+ word} )
addcol("word")

print_rdd(data)

//val source = data.map(row => (row(col("ip.dst)), row) )


print_columns(col)
//src_top_w.take(10)

//val src_top_word = src_top_w.join(words.value)
//src_top_word.take(10)

println("Computing conditional probability")

val src_scored = data.map(row => {
	val topic_mix = topics.value.getOrElse(row(col("id.orig_h")),Array(0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1) ).asInstanceOf[Array[Double]]
	val word_prob = words.value.getOrElse(row(col("word")),Array(0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1) ).asInstanceOf[Array[Double]]
        var src_score = 0.0 
	for ( i <- 0 to 19) {
         src_score += topic_mix(i) * word_prob(i)
      	}
	//val src_score = topic_mix :* wordprob
	(src_score, row :+ src_score)
	})
addcol("score")

src_scored.take(10)

var scored = src_scored.filter(elem => elem._1 < threshold).sortByKey().map( row => row._2.mkString(",") )
//var scored = src_scored.top(elem => elem._1 < threshold).sortByKey().map( row => row._2.mkString(",") )

println(scored.count() )
scored.persist(StorageLevel.MEMORY_AND_DISK)
scored.saveAsTextFile(scored_output_file)

