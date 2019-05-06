package bcapp

import bcpackage.FormsBuilder
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.collection.breakOut

object FormsCalcApp extends App {
  val config :Config = ConfigFactory.load(s"resources/application.conf")
  val logger = LoggerFactory.getLogger(getClass.getName)

  val dbType :String = "cassandra"
  val node :String =  config.getString(dbType+".connection.address")

  /**
    * Fut analyze search percents, price go up or down
  */
  val prcntsDiv :Seq[Double] = config.getDoubleList("futanalyze.logco").asScala.map(_.doubleValue)(breakOut)

  /**
    * For 3600 bars deep of Form is 6 hours. For bws=30 sec - Form size = 3 min.
  */
  val formDeepKoef :Int = 6 //rewrite it on seq on Int-s from config file.

  /**
    *  Next bar with ts_end <= (ts_end (CurrBar) + intervalNewGroupKoeff * BWS) going in this group, else next group.
  */
  val intervalNewGroupKoeff :Int = 2 //3

  try {
    (new FormsBuilder(node,prcntsDiv,formDeepKoef,intervalNewGroupKoeff)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }
}
