package bcapp

import bcpackage.FormsBuilder
import org.slf4j.LoggerFactory

object FormsCalcApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  logger.debug("PROPERTY log4j.properties = "+getClass.getClassLoader().getResource("log4j.properties"))


  val node: String = "10.241.5.234"
  val dbType: String = "cassandra"
  /**
    * Fut analyze search percents, price go up or down
  */
  val prcntsDiv : Seq[Double] = Seq(0.219, 0.437, 0.873)

  /**
    * For 3600 bars deep of Form is 6 hours. For bws=30 sec - Form size = 3 min.
  */
  val formDeepKoef :Int = 6

  /**
    *  Next bar with ts_end <= (ts_end (CurrBar) + intervalNewGroupKoeff * BWS) going in this group, else next group.
  */
  val intervalNewGroupKoeff :Int = 3

  try {
    (new FormsBuilder(node,prcntsDiv,formDeepKoef,intervalNewGroupKoeff)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }
}
