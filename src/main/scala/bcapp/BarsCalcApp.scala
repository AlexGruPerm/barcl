package bcapp

import bcpackage.BarCalculator
import org.slf4j.LoggerFactory

/*
package bcapp
import bcpackage.BarCalculator
import org.slf4j.LoggerFactory
*/

/**
  * Main application for run bars calculation.
  * As example of using "barcl".
  * readBySecs - read ticks by parts in seconds - 1 day by default.
  */

//todo: add config with cassandra ip.
object BarsCalcApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "xx.xx.xx.xx"
  val dbType: String = "cassandra"
  val readBySecs: Long = 60 * 60 * 3
  try {
    (new BarCalculator(node, dbType, readBySecs)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }
}


