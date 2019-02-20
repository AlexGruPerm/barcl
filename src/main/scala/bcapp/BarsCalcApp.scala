package bcapp

import bcpackage.BarCalculator
import dbpackage.DBConnector
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * Main application for run bars calculation.
  * As example of using "barcl".
  */
object BarsCalcApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "193.124.112.90"
  val session = (new DBConnector(node,"cassandra")).getDBSession
  session match {
    case Success(sess) => {
    try {
      (new BarCalculator(sess)).run
    } catch {
      case ex: Throwable => ex.printStackTrace()
    }
     finally {
      sess.close()
     }
    }
    case Failure(f) => f.printStackTrace()
  }
}
