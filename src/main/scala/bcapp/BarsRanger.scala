package bcapp

import bcpackage.BarRangeCalculator
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.collection.breakOut

/*
import collection.JavaConverters._
import scala.collection.breakOut
*/

object BarsRanger extends App {
  val config :Config = ConfigFactory.load(s"application.conf")
  val logger = LoggerFactory.getLogger(getClass.getName)

  /**
    * val dbType: String = "cassandra"
    * val node: String = config.getString(dbType+".src.ip")
    * val readBySecs: Long = 60 * 60 * config.getInt(dbType+".readByHours")
  */
  val dbType: String = "cassandra"
  val node: String =  config.getString(dbType+".src.ip")
  val logOpenExit : Seq[Double] = config.getDoubleList("futanalyze.logco").asScala.map(_.doubleValue)(breakOut)

  try {
    (new BarRangeCalculator(node,logOpenExit)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }

}
