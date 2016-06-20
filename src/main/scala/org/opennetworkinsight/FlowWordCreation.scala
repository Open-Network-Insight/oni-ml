package org.opennetworkinsight

import org.apache.spark.rdd.RDD

import scala.math._

object FlowWordCreation {

    def addTime(row: Array[String]): Array[String] = {
      val numTime = row(FlowColumnIndex.HOUR).toDouble + row(FlowColumnIndex.MINUTE).toDouble / 60 +
        row(FlowColumnIndex.SECOND).toDouble / 3600
      row.clone :+ numTime.toString
    }

    def binIbytIpktTime(row: Array[String],
                           ibyt_cuts: Array[Double],
                           ipkt_cuts: Array[Double],
                           time_cuts: Array[Double]) = {
      val time = row(FlowColumnIndex.NUMTIME).toDouble
      val ibyt = row(FlowColumnIndex.IBYT).toDouble
      val ipkt = row(FlowColumnIndex.IPKT).toDouble
      var time_bin = 0
      var ibyt_bin = 0
      var ipkt_bin = 0

      for (cut <- ibyt_cuts) {
        if (ibyt > cut) ibyt_bin = ibyt_bin + 1
      }
      for (cut <- ipkt_cuts) {
        if (ipkt > cut) ipkt_bin = ipkt_bin + 1
      }
      for (cut <- time_cuts) {
        if (time > cut) time_bin = time_bin + 1
      }
      row.clone :+ ibyt_bin.toString :+ ipkt_bin.toString :+ time_bin.toString
    }

    def adjustPort(row: Array[String]) = {
      var word_port = 111111.0
      val sip = row(FlowColumnIndex.SOURCEIP)
      val dip = row(FlowColumnIndex.DESTIP)
      val dport = row(FlowColumnIndex.SOURCEPORT).toDouble
      val sport = row(FlowColumnIndex.DESTPORT).toDouble
      val ipkt_bin = row(FlowColumnIndex.IPKTYBIN).toDouble
      val ibyt_bin = row(FlowColumnIndex.IBYTBIN).toDouble
      val time_bin = row(FlowColumnIndex.TIMEBIN).toDouble
      var p_case = 0.0

      var ip_pair = row(FlowColumnIndex.DESTIP) + " " + row(FlowColumnIndex.SOURCEIP)
      if (sip < dip & sip != 0) {
        ip_pair = row(FlowColumnIndex.SOURCEIP) + " " + row(FlowColumnIndex.DESTIP)
      }

      if ((dport <= 1024 | sport <= 1024) & (dport > 1024 | sport > 1024) & min(dport, sport) != 0) {
        p_case = 2
        word_port = min(dport, sport)
      } else if (dport > 1024 & sport > 1024) {
        p_case = 3
        word_port = 333333
      } else if (dport == 0 & sport != 0) {
        word_port = sport
        p_case = 4
      } else if (sport == 0 & dport != 0) {
        word_port = dport
        p_case = 4
      } else {
        p_case = 1
        if (min(dport, sport) == 0) {
          word_port = max(dport, sport)
        } else {
          word_port = 111111
        }
      }
      //val word = ipkt_bin -1 + (ibyt_bin-1)*10 + (time_bin - 1)*100 + word_port*1000
      val word = word_port.toString + "_" + time_bin.toString + "_" + ibyt_bin.toString + "_" + ipkt_bin.toString
      var src_word = word
      var dest_word = word

      if (p_case == 2 & dport < sport) {
        dest_word = "-1_" + dest_word
      } else if (p_case == 2 & sport < dport) {
        src_word = "-1_" + src_word
      } else if (p_case == 4 & dport == 0) {
        src_word = "-1_" + src_word
      } else if (p_case == 4 & sport == 0) {
        dest_word = "-1_" + dest_word
      }
      row :+ word_port.toString :+ ip_pair :+ src_word.toString :+ dest_word.toString
    }

    def removeHeader(input: RDD[String]) = {
      val header = input.first
      val output = input.filter(line => !(line == header))
      output
    }
  }

