import org.apache.spark.rdd.RDD

import scala.math._

package main.scala {

  object Quantiles extends Serializable {


    def compute_ecdf(x: RDD[Double]): RDD[(Double, Double)] = {
      val counts = x.map(v => (v, 1)).reduceByKey(_ + _).sortByKey().cache()
      // compute the partition sums
      val partSums: Array[Double] = 0.0 +: counts.mapPartitionsWithIndex {
        case (index, partition) => Iterator(partition.map({ case (sample, count) => count }).sum.toDouble)
      }.collect()

      // get sample size
      val numValues = partSums.sum

      // compute empirical cumulative distribution
      val sumsRdd = counts.mapPartitionsWithIndex {
        case (index, partition) =>
          var startValue = 0.0
          for (i <- 0 to index) {
            startValue += partSums(i)
          }
          partition.scanLeft((0.0, startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
      }
      sumsRdd.map(elem => (elem._1, elem._2 / numValues))
    }

    def distributed_quantiles(quantiles: Array[Double], ecdf: RDD[(Double, Double)]): Array[Double] = {
      def dqSeqOp(acc: Array[Double], value: (Double, Double)): Array[Double] = {
        val newacc: Array[Double] = acc
        for ((quant, pos) <- quantiles.zipWithIndex) {
          newacc(pos) = if (value._2 < quant) {
            max(newacc(pos), value._1)
          } else {
            newacc(pos)
          }
        }
        acc
      }

      def dqCombOp(acc1: Array[Double], acc2: Array[Double]) = {
        (acc1 zip acc2).map(tuple => max(tuple._1, tuple._2))
      }

      ecdf.aggregate(Array.fill[Double](quantiles.length)(0))((acc, value) => dqSeqOp(acc, value), (acc1, acc2) => dqCombOp(acc1, acc2))
    }
  }

}
