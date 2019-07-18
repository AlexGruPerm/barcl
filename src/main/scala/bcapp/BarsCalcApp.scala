package bcapp

import java.io
import java.io.File

import bcpackage.BarCalculatorTickersBws
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

//import scala.reflect.io.File

/*
package bcapp
import bcpackage.BarCalculator
import org.slf4j.LoggerFactory
*/

/**
  * Main application for run bars calculation.
  * As example of using "barcl".
  * readBySecs - read ticks by parts in seconds - 1 day by default.
  */

//todo: add config with cassandra ip.
object BarsCalcApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)

  val config :Config = try {
    if (args.length == 0) {
      logger.info("There is no external config file.")
      ConfigFactory.load()
    } else {
      val configFilename :String = System.getProperty("user.dir")+File.separator+args(0)
      logger.info("There is external config file, path="+configFilename)
      val fileConfig :Config = ConfigFactory.parseFile(new io.File(configFilename))
      ConfigFactory.load(fileConfig)
    }
  } catch {
    case e:Exception =>
      logger.error("ConfigFactory.load - cause:"+e.getCause+" msg:"+e.getMessage)
      throw e
  }

  /**
    * & <-- verifies both operands
    * && <-- stops evaluating if the first operand evaluates to false since the result will be false
    */

    /**
      * val startWriteBatchTime = System.nanoTime
      * nanoToMillis(System.nanoTime()-startWriteBatchTime)
    */
  val dbType: String = "cassandra"
  val node: String = config.getString(dbType+".src.ip")
  val readBySecs: Long = 60 * 60 * config.getInt(dbType+".readByHours")
  try {
    (new BarCalculatorTickersBws(node, dbType, readBySecs)).run
  } catch {
    case ex: Throwable => ex.printStackTrace()
  }
}


