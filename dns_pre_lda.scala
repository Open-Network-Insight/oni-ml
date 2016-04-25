// spark-shell --master yarn-client --executor-memory 35840m  --driver-memory 64g --num-executors 62 --executor-cores 12 --conf spark.shuffle.io.preferDirectBufs=false --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="12g" 
// spark-shell --executor-memory 35840m  --driver-memory 64g --num-executors 62 --executor-cores 12 --conf spark.shuffle.io.preferDirectBufs=false --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="12g" 

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import breeze.stats.DescriptiveStats._
//import breeze.linalg._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.math._
import scala.io.Source

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)


def removeHeader(input: org.apache.spark.rdd.RDD[String], col: scala.collection.mutable.Map[String,Int]) = {
    val header = input.first
    val output = input.filter(line => !(line == header))
    output
}

def get_column_names(input: Array[String], sep : Char = ',') : scala.collection.mutable.Map[String, Int] = {
    val columns = scala.collection.mutable.Map[String, Int]()
    val header = input.zipWithIndex
    header.foreach(tuple => columns(tuple._1) = tuple._2)
    columns
}

def print_columns(columns : scala.collection.mutable.Map[String, Int]) = {
    val arr = columns.toArray
    arr.sortBy(_._2).foreach(println)
}

def print_lines(input: org.apache.spark.rdd.RDD[String]) = input.take(10).foreach(println)

def print_rdd(input: org.apache.spark.rdd.RDD[Array[String]]) = input.take(20).foreach(m => println(m.mkString(",") ))

/**
def bin_column(row: Array[String], column: String, cuts: Array[Double]) = {
    var bin = 0
    for (cut <- cuts){
        if (row(col(column)).toDouble > cut) { bin = bin+1 }
    }
    col(column + "_bin") = row.length + 1   //update the column index automatically
    row :+ bin.toString
}
*/


def isNumeric(input: String): Boolean = {
    if (input == "") {false}
    else {input.forall(x => (x.isDigit || x == '.' || x =='E' || x == 'e')) && input.count(_ == '.')<2 && (input.count(_ == 'E') + input.count(_ == 'e')) <2}
}
def toDouble(s: String, default:Double=Double.NaN) = {if (isNumeric(s)) s.toDouble else default}


val file_list = System.getenv("DPATH")
val feedback_file = "None"
val duplication_factor = 100
val outputfile = System.getenv("HPATH")  + "/word_counts"
val quant = Array(0, 0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
val quint = Array(0, 0.2, 0.4, 0.6, 0.8)
var time_cuts = new Array[Double](10)
var frame_length_cuts = new Array[Double](10)
var subdomain_length_cuts = new Array[Double](5)
var numperiods_cuts = new Array[Double](5)
var entropy_cuts = new Array[Double](5)
val compute_quantiles = true
var df_cols = new Array[String](0)

val l_top_domains = Source.fromFile("top-1m.csv").getLines.map(line => {
                      val parts = line.split(",")
                     val l = parts.length
                    parts(1).split("[.]")(0)
                    }).toSet
val top_domains = sc.broadcast(l_top_domains)

/**
val file_list = System.getenv("DPATH")    //This can be one file or many file pathes separated by commas
val domain_table = System.getenv("DOMAIN_TABLE")
val feedback_file = System.getenv("FPATH")
val duplication_factor = System.getenv("DUPFACTOR").toInt
val output_file = System.getenv("HPATH") + "/dns_word_counts"
val output_file_for_lda = System.getenv("HPATH") + "/dns_lda_word_counts"
*/

//-----------------------------

var multidata = {
    var df = sqlContext.parquetFile( file_list.split(",")(0) ).filter("frame_len is not null")
    val files = file_list.split(",")
    for ( (file, index) <- files.zipWithIndex){
        if (index > 1) {
	    df = df.unionAll(sqlContext.parquetFile(file).filter("frame_len is not null"))
	}
    }
    df_cols = df.columns
    val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(",")) 
    tempRDD
}

var rawdata :org.apache.spark.rdd.RDD[String] = {
    if (feedback_file == "None") { multidata
    }else {
        var data :org.apache.spark.rdd.RDD[String] = multidata
        val feedback :org.apache.spark.rdd.RDD[String] = sc.textFile(feedback_file)
        val falsepositives = feedback.filter(line => line.split(",").last == "3")
        var i = 1
        while (i < duplication_factor) {
            data = data.union(falsepositives)
            i = i+1
        }
    data        
    }
}

// only needed if the schema is different than the standard solution setup
val indices: Array[Int] = Array(0,1,2,3,4,5,6,7,8)
rawdata = rawdata.map(line => line.split(",")).map(inner => indices.map(inner).mkString(","))

//print_lines(rawdata)
println(rawdata.count())
var col = get_column_names(df_cols)

def addcol(colname: String) = if (!col.keySet.exists(_==colname) ){col(colname) = col.values.max+1}
if (feedback_file != "None") { addcol("feedback") }


val datanoheader = rawdata
//print_rdd(datagood)
val datagood = datanoheader.map(line => line.split(",") ).filter(line => (line.length == df_cols.length)).map(line => {
    if (feedback_file != "None"){ line  :+ "None"
    }else {line}
    })


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
    
    if (numparts>2 && is_ip !="IP"){
        //This might try to parse things with only 1 or 2 numparts
        //test if last element is a country code or tld
        //use: Array(spliturl(numparts-1)).exists(country_codes contains _)
        // don't use: country_codes.exists(spliturl(numparts-1).contains) this doesn't test exact match, might just match substring
        if (Array(spliturl(numparts-1)).exists(country_codes.value contains _)){
            //country = spliturl(numparts-1)
            //tld = {if(Array(spliturl(numparts-2)).exists(tlds.value contains _)){spliturl(numparts-2)} else "None" }
            domain = spliturl(numparts-3)
            if (1<=numparts-3){ subdomain = spliturl.slice(0,numparts-3).mkString(".") } 
        }
        else{
            //tld = {if(Array(spliturl(numparts-1)).exists(tlds contains _)){spliturl(numparts-1)} else "None" }
            domain = spliturl(numparts-2)
            if (1<=numparts-2){ subdomain = spliturl.slice(0,numparts-2).mkString(".") }
        }
    }
    //Array(domain, subdomain, host, country, tld, numparts.toString, { if (subdomain !="None"){subdomain.length.toString} else {"0"}}, url.length.toString, is_ip )
    Array( domain,subdomain, { if (subdomain !="None"){subdomain.length.toString} else {"0"}}, numparts.toString)
}
println("Computing subdomain info")

var data_with_subdomains = datagood.map(row => row ++ extract_subdomain(row(col("dns_qry_name")) ) )
addcol("domain")
addcol("subdomain")
addcol("subdomain.length")
addcol("num.periods")

print_rdd(data_with_subdomains)
println(data_with_subdomains.count())

//--------------------------------------------------------------

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

 
def entropy( v:String ) : Double = { v
  .groupBy (a => a)
  .values
  .map( i => i.length.toDouble / v.length )
  .map( p => -p * log10(p) / log10(2))
  .sum
}

data_with_subdomains = data_with_subdomains.map(data => data :+ entropy(data(col("subdomain"))).toString )
addcol("subdomain.entropy")

if (compute_quantiles == true){
    println("calculating time cuts ...")
    time_cuts = distributed_quantiles(quant, compute_ecdf(data_with_subdomains.map(r => r(col("unix_tstamp")).toDouble )))
    println(time_cuts.mkString(",") )

    println("calculating frame length cuts ...")
    frame_length_cuts = distributed_quantiles(quant, compute_ecdf(data_with_subdomains.map(r => r(col("frame_len")).toDouble )))
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

println("adding words")
data = data.map(row => {
        val word = row(col("top_domain")) + "_" + bin_column(row(col("frame_len")), frame_length_cuts) + "_" + 
		bin_column(row(col("unix_tstamp")), time_cuts) + "_" +
        	bin_column(row(col("subdomain.length")), subdomain_length_cuts) + "_" +
                bin_column(row(col("subdomain.entropy")), entropy_cuts) + "_" +  
		bin_column(row(col("num.periods")), numperiods_cuts) + "_" + row(col("dns_qry_type"))+ "_" +row(col("dns_qry_rcode"))
row :+ word} )
addcol("word")

print_rdd(data)
val wc = data.map(row => (row(col("ip_dst")) + " " + row(col("word")), 1)).reduceByKey(_ + _).map(row => (row._1.split(" ")(0)+","+ row._1.split(" ")(1).toString +","+ row._2).mkString)

wc.persist(StorageLevel.MEMORY_AND_DISK)
print(outputfile)
wc.saveAsTextFile(outputfile)

System.exit(0)
