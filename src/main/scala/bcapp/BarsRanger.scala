package bcapp

import bcpackage.BarRangeCalculator
import org.slf4j.LoggerFactory

object BarsRanger extends App {

  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "10.241.5.234"
  val dbType: String = "cassandra"

  try {
    (new BarRangeCalculator(node)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }

}
