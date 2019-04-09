package bcapp

import bcpackage.BarRangeCalculator
import org.slf4j.LoggerFactory

object BarsRanger extends App {

  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "10.241.5.234"
  val dbType: String = "cassandra"
  val logOpenExit : Seq[Double] = Seq(0.219/*, 0.437, 0.873*/) //Fut analyze search percents, price go up or down

  try {
    (new BarRangeCalculator(node,logOpenExit)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }

}
