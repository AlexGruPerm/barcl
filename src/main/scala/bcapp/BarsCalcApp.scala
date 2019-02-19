package bcapp

import bcpackage.BarCalculator
import casspackage.CassConnect
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Main application for run bars calculation.
  * As example of using "barcl".
  */
class BarsCalcApp {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "193.124.112.90"
  val session : Try[Session] = (new CassConnect).getCassSession(node)
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
