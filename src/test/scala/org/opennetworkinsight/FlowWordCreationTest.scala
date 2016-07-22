package org.opennetworkinsight


import org.scalatest.FlatSpec
import org.scalatest.Matchers

class FlowWordCreationTest extends FlatSpec with Matchers {

  val timeCuts = Array(12.924999999999999, 12.932777777777778, 12.957777777777777, 12.966666666666667,
    12.975277777777778, 12.984166666666665, 12.991388888888888, 12.998888888888889, Double.PositiveInfinity, Double.PositiveInfinity)
  val ibytCuts = Array(52.0, 58.0, 58.0, 73.0, 108.0, 470.0, 1664.0, 46919.0, 464622.0, Double.PositiveInfinity)
  val ipktCuts = Array(1.0, 1.0, 3.0, 29.0, Double.PositiveInfinity)

  "binIbytIpktTime" should "count 5 ibyt_bin for ibyt 208, 3 ipkt_bin for ipkt 4 and 0 time_bin for time " +
    "12.9236111111111" in {

    val row = Array("2016-05-05 12:55:25",	"2016",	"5",	"5",	"12",	"55",	"25",	"0.16",	"172.16.0.152",	"10.0.2.239",
      "49504",	"80",	"TCP",	".A....",	"0",	"0",	"4",	"208",	"0",	"0",	"3",	"2",	"0",	"0",	"0",	"1",
      "10.219.100.251",	"12.92361111")

    val result = FlowWordCreation.binIbytIpktTime(row, ibytCuts, ipktCuts, timeCuts)

    result.length shouldBe 31
    result(FlowColumnIndex.IBYTBIN) shouldBe "5"
    result(FlowColumnIndex.IPKTYBIN) shouldBe "3"
    result(FlowColumnIndex.TIMEBIN) shouldBe "0"

  }

  it should "count 7 ibyt_bin for ibyt 46919, 4 ipkt_bin for ipkt 32 and 7 time_bin for time 12.9922222222222" in {

    val row = Array("2016-05-05 12:59:32",	"2016",	"5",	"5",	"12",	"59",	"32",	"0",	"10.0.2.115",	"172.16.0.107",
      "80", "60797",	"TCP",	".AP...",	"0",	"0",	"32",	"46919",	"0",	"0",	"2",	"3",	"0",	"0",	"0",	"0",
      "10.219.100.251", "12.99222222")

    val result = FlowWordCreation.binIbytIpktTime(row, ibytCuts, ipktCuts, timeCuts)

    result.length shouldBe 31
    result(FlowColumnIndex.IBYTBIN) shouldBe "7"
    result(FlowColumnIndex.IPKTYBIN) shouldBe "4"
    result(FlowColumnIndex.TIMEBIN) shouldBe "7"
  }

  it should "count 0 ibyt_bin for ibyt 0, 0 ipkt_bin for ipkt 0 and 0 time_bin for time 0" in {

    val row = Array("2016-05-05 12:59:32",	"2016",	"5",	"5",	"12",	"59",	"32",	"0",	"10.0.2.115",	"172.16.0.107",
      "80", "60797",	"TCP",	".AP...",	"0",	"0",	"0",	"0",	"0",	"0",	"2",	"3",	"0",	"0",	"0",	"0",
      "10.219.100.251", "0")

    val result = FlowWordCreation.binIbytIpktTime(row, ibytCuts, ipktCuts, timeCuts)

    result.length shouldBe 31
    result(FlowColumnIndex.IBYTBIN) shouldBe "0"
    result(FlowColumnIndex.IPKTYBIN) shouldBe "0"
    result(FlowColumnIndex.TIMEBIN) shouldBe "0"
  }

  it should "count 9 ibyt_bin for ibyt Infinity, 4 ipkt_bin for ipkt Infinity and 8 time_bin for time Infinity" in {

    val row = Array("2016-05-05 12:59:32",	"2016",	"5",	"5",	"12",	"59",	"32",	"0",	"10.0.2.115",	"172.16.0.107",
      "80", "60797",	"TCP",	".AP...",	"0",	"0",	"Infinity",	"Infinity",	"0",	"0",	"2",	"3",	"0",	"0",	"0",	"0",
      "10.219.100.251", "Infinity")

    val result = FlowWordCreation.binIbytIpktTime(row, ibytCuts, ipktCuts, timeCuts)

    result.length shouldBe 31
    result(FlowColumnIndex.IBYTBIN) shouldBe "9"
    result(FlowColumnIndex.IPKTYBIN) shouldBe "4"
    result(FlowColumnIndex.TIMEBIN) shouldBe "8"
  }

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

    val result = FlowWordCreation.adjustPort(rowSrcIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "23.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "-1_23.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "23.0_7.0_7.0_4.0"

  }

  // 2. Test when sip is less than dip and sip is not 0 and sport is <= 1024 & dport > 1024 and min(dport, sport) !=0 +
  it should "create word with ip_pair as sourceIp-destIp, port is sport and src_word direction is -1" in {
    rowSrcIPLess(10) = "23"
    rowSrcIPLess(11) = "2132"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "23.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "23.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "-1_23.0_7.0_7.0_4.0"
  }

  // 3. Test when sip is less than dip and sip is not 0 and dport and sport are > 1024 +
  it should "create word with ip_pair as sourceIp-destIp, port is 333333.0 and both words direction is 1 (not showing)" in {
    rowSrcIPLess(10) = "8392"
    rowSrcIPLess(11) = "9874"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "333333.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "333333.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "333333.0_7.0_7.0_4.0"
  }

  // 4. Test when sip is less than dip and sip is not 0 and dport is 0 but sport is not +
  it should "create word with ip_pair as sourceIp-destIp, port is sport and source_word direction is -1" in {
    rowSrcIPLess(10) = "80"
    rowSrcIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "80.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "80.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "-1_80.0_7.0_7.0_4.0"
  }

  // 5. Test when sip is less than dip and sip is not 0 and sport is 0 but dport is not +
  it should "create word with ip_pair as sourceIp-destIp, port is dport and dest_word direction is -1 II" in {
    rowSrcIPLess(10) = "0"
    rowSrcIPLess(11) = "43.0"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "43.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "-1_43.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "43.0_7.0_7.0_4.0"
  }

  // 6. Test when sip is less than dip and sip is not 0 and sport and dport are less or equal than 1024 +
  it should "create word with ip_pair as sourceIp-destIp, port is 111111.0 and both words direction is 1 (not showing)" in {
    rowSrcIPLess(10) = "1024"
    rowSrcIPLess(11) = "80"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "111111.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "111111.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "111111.0_7.0_7.0_4.0"
  }

  // 7. Test when sip is less than dip and sip is not 0 and sport and dport are 0+
  it should "create word with ip_pair as sourceIp-destIp, port is max(0,0) and both words direction is 1 (not showing)" in {
    rowSrcIPLess(10) = "0"
    rowSrcIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowSrcIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "0.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "0.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "0.0_7.0_7.0_4.0"
  }

  // 8. Test when sip is not less than dip and dport is <= 1024 & sport > 1024 and min(dport, sport) !=0+
  it should "create word with ip_pair as destIp-sourceIp, port is dport and dest_word direction is -1" in {
    rowDstIPLess(10) = "3245"
    rowDstIPLess(11) = "43"

    val result = FlowWordCreation.adjustPort(rowDstIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "43.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "-1_43.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "43.0_7.0_7.0_4.0"

  }

  // 9. Test when sip is not less than dip and sport is <= 1024 & dport > 1024 and min(dport, sport) !=0 +
  it should "create word with ip_pair as destIp-sourceIp, port is sport and src_word direction is -1" in {
    rowDstIPLess(10) = "80"
    rowDstIPLess(11) = "2435"

    val result = FlowWordCreation.adjustPort(rowDstIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "80.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "80.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "-1_80.0_7.0_7.0_4.0"

  }

  // 10. Test when sip is not less than dip and dport and sport are > 1024 +
  it should "create word with ip_pair as destIp-sourceIp, port is 333333.0 and both words direction is 1 (not showing)" in {
    rowDstIPLess(10) = "2354"
    rowDstIPLess(11) = "2435"

    val result = FlowWordCreation.adjustPort(rowDstIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "333333.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "333333.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "333333.0_7.0_7.0_4.0"
  }

  // 11. Test when sip is not less than dip and dport is 0 but sport is not +
  it should "create word with ip_pair as destIp-sourceIp, port is sport and src_word direction is -1 II" in {
    rowDstIPLess(10) = "80"
    rowDstIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowDstIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "80.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "80.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "-1_80.0_7.0_7.0_4.0"
  }

  // 12. Test when sip is not less than dip and sport is 0 but dport is not +
  it should "create word with ip_pair as destIp-sourceIp, port is dport and dest_word direction is -1 II" in {
    rowDstIPLess(10) = "0"
    rowDstIPLess(11) = "2435"

    val result = FlowWordCreation.adjustPort(rowDstIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "2435.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "-1_2435.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "2435.0_7.0_7.0_4.0"
  }

  // 13. Test when sip is not less than dip and sport and dport are less or equal than 1024
  it should "create word with ip_pair as destIp-sourceIp, port 111111.0 and both words direction is 1 (not showing)" in {
    rowDstIPLess(10) = "80"
    rowDstIPLess(11) = "1024"

    val result = FlowWordCreation.adjustPort(rowDstIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "111111.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "111111.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "111111.0_7.0_7.0_4.0"
  }

  // 14. Test when sip is not less than dip and sport and dport are 0
  it should "create word with ip_pair as destIp-sourceIp, port is max(0,0) and both words direction is 1 (not showing)" in {
    rowDstIPLess(10) = "0"
    rowDstIPLess(11) = "0"

    val result = FlowWordCreation.adjustPort(rowDstIPLess)

    result.length shouldBe 35
    result(FlowColumnIndex.IPPAIR) shouldBe "10.0.2.115 172.16.0.107"
    result(FlowColumnIndex.PORTWORD) shouldBe "0.0"
    result(FlowColumnIndex.DESTWORD) shouldBe "0.0_7.0_7.0_4.0"
    result(FlowColumnIndex.SOURCEWORD) shouldBe "0.0_7.0_7.0_4.0"
  }

}
