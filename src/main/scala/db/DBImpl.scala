package db

import com.datastax.driver.core.{Cluster, Session}
import com.datastax.driver.core.exceptions.NoHostAvailableException
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

import bcstruct.{CalcProperties, CalcProperty}
import com.datastax.driver.core.Row

import scala.collection.JavaConverters._

/**
  *
  */
abstract class DBImpl(nodeAddress :String,dbType :String) {

  val supportedDb = Seq("cassandra","oracle"/*,"postgres"*/)

  def isClosed : Boolean
  def close()

  def getAllCalcProperties : CalcProperties

}







class DBCass(nodeAddress :String,dbType :String) extends DBImpl(nodeAddress :String,dbType :String)  {
  val logger = LoggerFactory.getLogger(getClass.getName)

  private def exCaseGetSession(e: Throwable) = {
    val errMsg :String = "Any kind of exception :"+e.getMessage
    logger.error(errMsg)
    Failure(new AssertionError(errMsg))
  }

  private def exNoHostAvail(e: NoHostAvailableException) = {
    val errMsg :String = s"Host for connection is not available ["+nodeAddress+"] for [$dbType]"
    logger.error(errMsg)
    throw e
  }

   private val TrySession : Try[Session] = {
    require(supportedDb.contains(dbType),"Not supported database type.")
    logger.debug(s"getSession - Try open connection to $dbType.")
        try {
          val sessInternal = Cluster.builder().addContactPoint(nodeAddress).build().connect()
          logger.debug(s"($getClass).getSession - Connection opened for [$dbType].")
          Success(sessInternal)
        } catch {
          case exHostAvail: NoHostAvailableException => exNoHostAvail(exHostAvail)
          case e: Throwable => exCaseGetSession(e)
        }
  }

  def getTrySession = TrySession

  val session = TrySession match {
    case Success(s) => s
  }

  def isClosed : Boolean = session.isClosed

  def close() = session.close()


  /**
    * Meta information, which tickers and which seconds deeps we need read for calculation.
    * For ex:
    *  ticker_id, deep_sec
    *  1          30
    *  1          60
    *  2          30
    *  ...
    *  means that we want calculate bars for ticker_id=1 for widths 30 and 60 seconds,
    *  and for ticker_id=2 just 30 seconds.
    */
  private val bndBCalcProps = session.prepare(""" select * from mts_meta.bars_property """).bind()
  private val bndBars3600 = session.prepare(""" select * from mts_bars.td_bars_3600 """).bind()


  def getAllCalcProperties= {
    require(!session.isClosed)

    val bndBCalcPropsDataSet = session.execute(bndBCalcProps).all().iterator.asScala

    val rowToCalcProperty = (row : Row) =>
      new CalcProperty(
        row.getInt("ticker_id"),
        row.getInt("bar_width_sec"),
        row.getInt("is_enabled")
      )

    CalcProperties(bndBCalcPropsDataSet
      .toSeq.map(rowToCalcProperty)
      .sortBy(sr => sr.tickerId)(Ordering[Int])
      .sortBy(sr => sr.barDeepSec)(Ordering[Int]))
  }

}


object DBCass {
  def apply(nodeAddress: String, dbType :String) = {
    new DBCass(nodeAddress,dbType)
  }
}