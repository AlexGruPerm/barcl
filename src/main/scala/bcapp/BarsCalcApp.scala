package bcapp

import bcpackage.BarCalculator
import dbpackage.DBConnector
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Main application for run bars calculation.
  * As example of using "barcl".
  */
object BarsCalcApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "193.124.112.90"
  /**
  * Moving here from Session to DBSession DELIT.
  * */
  val session = (new DBConnector(node,"cassandra")).getDBSession
  session match {
    case Success(sess) => {
    try {
      /** DELIT.
        * Here we need rewrite code to make possibilities of using Oracle session, Postgres session and etc.
        * Send into BarCalculator unified object FE: sess as DBSession - that contain information about type of DB.
        */
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
