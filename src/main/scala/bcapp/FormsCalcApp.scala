package bcapp

import bcpackage.FormsBuilder
import org.slf4j.LoggerFactory

object FormsCalcApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "10.241.5.234"
  val dbType: String = "cassandra"
  val readBySecs: Long = 60 * 60 * 12
  try {
    (new FormsBuilder(node)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }
}
