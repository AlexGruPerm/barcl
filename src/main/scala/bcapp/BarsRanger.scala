package bcapp

import bcpackage.BarRangeCalculator
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}

import collection.JavaConverters._
import scala.collection.breakOut

/*
import collection.JavaConverters._
import scala.collection.breakOut
*/

object BarsRanger extends App {
  val config :Config = ConfigFactory.load(s"resources/application.conf")
  val logger = LoggerFactory.getLogger(getClass.getName)

  val dbType: String = "cassandra"
  val node: String =  config.getString(dbType+".connection.address")
  val logOpenExit : Seq[Double] = config.getDoubleList("futanalyze.logco").asScala.map(_.doubleValue)(breakOut)

  try {
    (new BarRangeCalculator(node,logOpenExit)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }

}
