package org.opennetworkinsight.utilities

import scala.io.Source


object TopDomains {

  val path = "top-1m.csv"
  val topDomains : Set[String] = Source.fromFile(path).getLines.map(line => {
    val parts = line.split(",")
    val l = parts.length
    parts(1).split("[.]")(0)
  }).toSet
}
