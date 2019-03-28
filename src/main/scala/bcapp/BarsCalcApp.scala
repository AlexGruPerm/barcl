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

object BarsCalcApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "192.168.122.192"
  val dbType: String = "cassandra"
  val readBySecs: Long = 60 * 60 * 12 //read by 12 hours.
  try {
    (new BarCalculator(node, dbType, readBySecs)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }
}


