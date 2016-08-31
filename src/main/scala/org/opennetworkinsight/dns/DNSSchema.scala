package org.opennetworkinsight.dns
/**
  * Data frame column names used in the DNS suspicious connects analysis.
  */
object DNSSchema {

  // input fields

  val Timestamp = "frame_time"
  val UnixTimestamp = "unix_tstamp"
  val FrameLength = "frame_len"
  val ClientIP = "ip_dst"
  val ServerIP = "ip_src"
  val QueryName = "dns_qry_name"
  val QueryClass = "dns_qry_class"
  val QueryType = "dns_qry_type"
  val QueryResponseCode = "dns_qry_rcode"
  val AnswerAddress = "dns_a"

  // intermediate and derived fields

  val Feedback = "feedback"
  val Domain = "domain"
  val TopDomain = "top_domain"
  val Subdomain = "subdomain"
  val SubdomainLength = "subdomain_length"
  val NumPeriods = "num_periods"
  val SubdomainEntropy = "subdomain_entropy"

  // output fields

  val Word = "word"
  val Score = "score"
}
