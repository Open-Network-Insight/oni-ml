package org.opennetworkinsight.netflow

object FlowSchema {

  // input fields

  val TimeReceived = "treceived"
  val UnixTimestamp = "unix_tstamp"
  val Year = "tryear"
  val Month = "trmonth"
  val Day = "trday"
  val Hour = "trhour"
  val Minute = "trminute"
  val Second = "trsec"
  val Duration = "tdur"
  val SourceIP = "sip "
  val DestinationIP = "dip"
  val SourcePort = "sport"
  val DestinationPort = "dport"
  val proto = "proto"
  val Flag = "flag"
  val fwd = "fwd"
  val stos = "stos"
  val ipkt = "ipkt"
  val ibyt = "ibyt"
  val opkt = "opkt"
  val obyt = "obyt"
  val input = "input"
  val output = "output"
  val sas = "sas"
  val das = "das"
  val dtos = "dtos"
  val dir = "dir"
  val rip = "rip"

  // derived and intermediate fields

  // output fields

  val Word = "word"
  val Score = "score"
}
