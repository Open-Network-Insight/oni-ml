/**
  * Created by nlsegerl on 6/2/16.
  */

package main.scala {

  import testutils.TestingSparkContextFlatSpec
  import org.scalatest.Matchers
  import main.scala.Quantiles

  class QuantilesTest extends TestingSparkContextFlatSpec with Matchers {

    val allOnes = List(1.0, 1.0, 1.0, 1.0, 1.0)
    val onesAndTwos = List(1.0, 2.0, 1.0, 2.0)
    val countToTen = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)


    "ecdf" should "on a constant list" in {
      val rddIn = sparkContext.parallelize(allOnes)
      val rddOut = Quantiles.compute_ecdf(rddIn)

      val out = rddOut.collect()

      out.length shouldBe 1
      out(0) shouldBe (1.0, 1.0)
    }

    "ecdf" should "on a split 50/50 list" in {
      val rddIn = sparkContext.parallelize(onesAndTwos)
      val rddOut = Quantiles.compute_ecdf(rddIn)

      val out = rddOut.collect()

      out.length shouldBe 2
      out(0) shouldBe (1.0, 0.5)
      out(1) shouldBe (2.0, 1.0)
    }

    "ecdf" should "on count-to-ten list" in {
      val rddIn = sparkContext.parallelize(countToTen)
      val rddOut = Quantiles.compute_ecdf(rddIn)

      val out = rddOut.collect()

      out.length shouldBe 10
      out(0) shouldBe (1.0, 0.1)
      out(1) shouldBe (2.0, 0.2)
      out(2) shouldBe (3.0, 0.3)
      out(3) shouldBe (4.0, 0.4)
      out(4) shouldBe (5.0, 0.5)
      out(5) shouldBe (6.0, 0.6)
      out(6) shouldBe (7.0, 0.7)
      out(7) shouldBe (8.0, 0.8)
      out(8) shouldBe (9.0, 0.9)
      out(9) shouldBe (10.0, 1.0)
    }

    "distributed_quantiles" should "on a split 50/50 list" in {
      val rddIn = sparkContext.parallelize(onesAndTwos)
      val quantile_cutoffs = Array(0.0, 0.6)
      val quantiles_out = Quantiles.distributed_quantiles(quantile_cutoffs, Quantiles.compute_ecdf(rddIn))


      quantiles_out.length shouldBe 2
      quantiles_out(0) shouldBe 0.0
      quantiles_out(1) shouldBe 1.0

    }
  }

}