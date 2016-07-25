package org.opennetworkinsight

/**
  * Created by nlsegerl on 7/19/16.
  */
object ProxyWordCreation {

  def proxyWord(proxyHost: String, proxyReqMethod: String, proxyRespCode: String, proxyFullURI: String) : String  = {
    List(proxyHost, proxyReqMethod, proxyRespCode,proxyFullURI.split('/').length.toString()).mkString("|")  }
}
