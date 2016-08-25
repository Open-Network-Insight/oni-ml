package org.opennetworkinsight


import org.opennetworkinsight.netflow.FlowWordCreation
import org.scalatest.{FlatSpec, Matchers}

class FlowWordCreationTest extends FlatSpec with Matchers {

  // Replace ports in index 10 and 11
  val rowSrcIPLess = Array("2016-05-05 12:59:32",	"2016",	"5",	"5",	"12",	"59",	"32",	"0",	"10.0.2.115",	"172.16.0.107",
    "-", "-",	"TCP",	".AP...",	"0",	"0",	"32",	"46919",	"0",	"0",	"2",	"3",	"0",	"0",	"0",	"0",
    "10.219.100.251", "12.99222222", "7", "4", "7")

  val rowDstIPLess = Array("2016-05-05 12:59:32",	"2016",	"5",	"5",	"12",	"59",	"32",	"0",  "172.16.0.107", "10.0.2.115",
    "-", "-",	"TCP",	".AP...",	"0",	"0",	"32",	"46919",	"0",	"0",	"2",	"3",	"0",	"0",	"0",	"0",
    "10.219.100.251", "12.99222222", "7", "4", "7")

  // 1. Test when sip is less than dip and sip is not 0 and dport is <= 1024 & sport > 1024 and min(dport, sport) !=0 +
  "adjustPort" should "create word with ip_pair as sourceIp-destIp, port is dport and dest_word direction is -1" in {
    rowSrcIPLess(10) = "2132"
    rowSrcIPLess(11) = "23"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess(8), rowSrcIPLess(9), rowSrcIPLess(11).toInt, rowSrcIPLess(10).toInt,
      rowSrcIPLess(29).toDouble, rowSrcIPLess(28).toDouble, rowSrcIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "23.0"
    result(3) shouldBe "-1_23.0_7.0_7.0_4.0"
    result(2) shouldBe "23.0_7.0_7.0_4.0"

  }

  // 2. Test when sip is less than dip and sip is not 0 and sport is <= 1024 & dport > 1024 and min(dport, sport) !=0 +
  it should "create word with ip_pair as sourceIp-destIp, port is sport and src_word direction is -1" in {
    rowSrcIPLess(10) = "23"
    rowSrcIPLess(11) = "2132"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess(8), rowSrcIPLess(9), rowSrcIPLess(11).toInt, rowSrcIPLess(10).toInt,
      rowSrcIPLess(29).toDouble, rowSrcIPLess(28).toDouble, rowSrcIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "23.0"
    result(3) shouldBe "23.0_7.0_7.0_4.0"
    result(2) shouldBe "-1_23.0_7.0_7.0_4.0"
  }

  // 3. Test when sip is less than dip and sip is not 0 and dport and sport are > 1024 +
  it should "create word with ip_pair as sourceIp-destIp, port is 333333.0 and both words direction is 1 (not showing)" in {
    rowSrcIPLess(10) = "8392"
    rowSrcIPLess(11) = "9874"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess(8), rowSrcIPLess(9), rowSrcIPLess(11).toInt, rowSrcIPLess(10).toInt,
      rowSrcIPLess(29).toDouble, rowSrcIPLess(28).toDouble, rowSrcIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "333333.0"
    result(3) shouldBe "333333.0_7.0_7.0_4.0"
    result(2) shouldBe "333333.0_7.0_7.0_4.0"
  }

  // 4. Test when sip is less than dip and sip is not 0 and dport is 0 but sport is not +
  it should "create word with ip_pair as sourceIp-destIp, port is sport and source_word direction is -1" in {
    rowSrcIPLess(10) = "80"
    rowSrcIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess(8), rowSrcIPLess(9), rowSrcIPLess(11).toInt, rowSrcIPLess(10).toInt,
      rowSrcIPLess(29).toDouble, rowSrcIPLess(28).toDouble, rowSrcIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "80.0"
    result(3) shouldBe "80.0_7.0_7.0_4.0"
    result(2) shouldBe "-1_80.0_7.0_7.0_4.0"
  }

  // 5. Test when sip is less than dip and sip is not 0 and sport is 0 but dport is not +
  it should "create word with ip_pair as sourceIp-destIp, port is dport and dest_word direction is -1 II" in {
    rowSrcIPLess(10) = "0"
    rowSrcIPLess(11) = "43"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess(8), rowSrcIPLess(9), rowSrcIPLess(11).toInt, rowSrcIPLess(10).toInt,
      rowSrcIPLess(29).toDouble, rowSrcIPLess(28).toDouble, rowSrcIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "43.0"
    result(3) shouldBe "-1_43.0_7.0_7.0_4.0"
    result(2) shouldBe "43.0_7.0_7.0_4.0"
  }

  // 6. Test when sip is less than dip and sip is not 0 and sport and dport are less or equal than 1024 +
  it should "create word with ip_pair as sourceIp-destIp, port is 111111.0 and both words direction is 1 (not showing)" in {
    rowSrcIPLess(10) = "1024"
    rowSrcIPLess(11) = "80"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess(8), rowSrcIPLess(9), rowSrcIPLess(11).toInt, rowSrcIPLess(10).toInt,
      rowSrcIPLess(29).toDouble, rowSrcIPLess(28).toDouble, rowSrcIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "111111.0"
    result(3) shouldBe "111111.0_7.0_7.0_4.0"
    result(2) shouldBe "111111.0_7.0_7.0_4.0"
  }

  // 7. Test when sip is less than dip and sip is not 0 and sport and dport are 0+
  it should "create word with ip_pair as sourceIp-destIp, port is max(0,0) and both words direction is 1 (not showing)" in {
    rowSrcIPLess(10) = "0"
    rowSrcIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess(8), rowSrcIPLess(9), rowSrcIPLess(11).toInt, rowSrcIPLess(10).toInt,
      rowSrcIPLess(29).toDouble, rowSrcIPLess(28).toDouble, rowSrcIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "0.0"
    result(3) shouldBe "0.0_7.0_7.0_4.0"
    result(2) shouldBe "0.0_7.0_7.0_4.0"
  }

  // 8. Test when sip is not less than dip and dport is <= 1024 & sport > 1024 and min(dport, sport) !=0+
  it should "create word with ip_pair as destIp-sourceIp, port is dport and dest_word direction is -1" in {
    rowDstIPLess(10) = "3245"
    rowDstIPLess(11) = "43"

    val result = FlowWordCreation.adjustPort(rowDstIPLess(8), rowDstIPLess(9), rowDstIPLess(11).toInt, rowDstIPLess(10).toInt,
      rowDstIPLess(29).toDouble, rowDstIPLess(28).toDouble, rowDstIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "43.0"
    result(3) shouldBe "-1_43.0_7.0_7.0_4.0"
    result(2) shouldBe "43.0_7.0_7.0_4.0"

  }

  // 9. Test when sip is not less than dip and sport is <= 1024 & dport > 1024 and min(dport, sport) !=0 +
  it should "create word with ip_pair as destIp-sourceIp, port is sport and src_word direction is -1" in {
    rowDstIPLess(10) = "80"
    rowDstIPLess(11) = "2435"

    val result = FlowWordCreation.adjustPort(rowDstIPLess(8), rowDstIPLess(9), rowDstIPLess(11).toInt, rowDstIPLess(10).toInt,
      rowDstIPLess(29).toDouble, rowDstIPLess(28).toDouble, rowDstIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "80.0"
    result(3) shouldBe "80.0_7.0_7.0_4.0"
    result(2) shouldBe "-1_80.0_7.0_7.0_4.0"

  }

  // 10. Test when sip is not less than dip and dport and sport are > 1024 +
  it should "create word with ip_pair as destIp-sourceIp, port is 333333.0 and both words direction is 1 (not showing)" in {
    rowDstIPLess(10) = "2354"
    rowDstIPLess(11) = "2435"

    val result = FlowWordCreation.adjustPort(rowDstIPLess(8), rowDstIPLess(9), rowDstIPLess(11).toInt, rowDstIPLess(10).toInt,
      rowDstIPLess(29).toDouble, rowDstIPLess(28).toDouble, rowDstIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "333333.0"
    result(3) shouldBe "333333.0_7.0_7.0_4.0"
    result(2) shouldBe "333333.0_7.0_7.0_4.0"
  }

  // 11. Test when sip is not less than dip and dport is 0 but sport is not +
  it should "create word with ip_pair as destIp-sourceIp, port is sport and src_word direction is -1 II" in {
    rowDstIPLess(10) = "80"
    rowDstIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowDstIPLess(8), rowDstIPLess(9), rowDstIPLess(11).toInt, rowDstIPLess(10).toInt,
      rowDstIPLess(29).toDouble, rowDstIPLess(28).toDouble, rowDstIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "80.0"
    result(3) shouldBe "80.0_7.0_7.0_4.0"
    result(2) shouldBe "-1_80.0_7.0_7.0_4.0"
  }

  // 12. Test when sip is not less than dip and sport is 0 but dport is not +
  it should "create word with ip_pair as destIp-sourceIp, port is dport and dest_word direction is -1 II" in {
    rowDstIPLess(10) = "0"
    rowDstIPLess(11) = "2435"

    val result = FlowWordCreation.adjustPort(rowDstIPLess(8), rowDstIPLess(9), rowDstIPLess(11).toInt, rowDstIPLess(10).toInt,
      rowDstIPLess(29).toDouble, rowDstIPLess(28).toDouble, rowDstIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "2435.0"
    result(3) shouldBe "-1_2435.0_7.0_7.0_4.0"
    result(2) shouldBe "2435.0_7.0_7.0_4.0"
  }

  // 13. Test when sip is not less than dip and sport and dport are less or equal than 1024
  it should "create word with ip_pair as destIp-sourceIp, port 111111.0 and both words direction is 1 (not showing)" in {
    rowDstIPLess(10) = "80"
    rowDstIPLess(11) = "1024"

    val result = FlowWordCreation.adjustPort(rowDstIPLess(8), rowDstIPLess(9), rowDstIPLess(11).toInt, rowDstIPLess(10).toInt,
      rowDstIPLess(29).toDouble, rowDstIPLess(28).toDouble, rowDstIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "111111.0"
    result(3) shouldBe "111111.0_7.0_7.0_4.0"
    result(2) shouldBe "111111.0_7.0_7.0_4.0"
  }

  // 14. Test when sip is not less than dip and sport and dport are 0
  it should "create word with ip_pair as destIp-sourceIp, port is max(0,0) and both words direction is 1 (not showing)" in {
    rowDstIPLess(10) = "0"
    rowDstIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowDstIPLess(8), rowDstIPLess(9), rowDstIPLess(11).toInt, rowDstIPLess(10).toInt,
      rowDstIPLess(29).toDouble, rowDstIPLess(28).toDouble, rowDstIPLess(30).toDouble)

    result.length shouldBe 4
    result(1) shouldBe "10.0.2.115 172.16.0.107"
    result(0) shouldBe "0.0"
    result(3) shouldBe "0.0_7.0_7.0_4.0"
    result(2) shouldBe "0.0_7.0_7.0_4.0"
  }

}
