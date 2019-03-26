package bcapp

import bcpackage.BarRangeCalculator
import org.slf4j.LoggerFactory

object BarsRanger extends App {

  val logger = LoggerFactory.getLogger(getClass.getName)
  val node: String = "10.241.5.234"
  val dbType: String = "cassandra"
  val seqLogDiff : Seq[Int] = Seq(5,10,15) //Fut analyze search percents, price go up or down on 5,10.. precents.

  try {
    (new BarRangeCalculator(node,seqLogDiff)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }

}
