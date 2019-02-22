package db

import com.datastax.driver.core.{Cluster, Session}
import com.datastax.driver.core.exceptions.NoHostAvailableException
import org.slf4j.LoggerFactory
import java.util.Date

import scala.util.{Failure, Success, Try}
import bcstruct.{CalcProperties, CalcProperty, LastBar, LastBars}
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
 // def getLastBars(cProperties :CalcProperties) :LastBars

}







class DBCass(nodeAddress :String,dbType :String) extends DBImpl(nodeAddress :String,dbType :String) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  private def exCaseGetSession(e: Throwable) = {
    val errMsg: String = "Any kind of exception :" + e.getMessage
    logger.error(errMsg)
    Failure(new AssertionError(errMsg))
  }

  private def exNoHostAvail(e: NoHostAvailableException) = {
    val errMsg: String = s"Host for connection is not available [" + nodeAddress + "] for [$dbType]"
    logger.error(errMsg)
    throw e
  }

  private val TrySession: Try[Session] = {
    require(supportedDb.contains(dbType), "Not supported database type.")
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

  def isClosed: Boolean = session.isClosed

  def close() = session.close()

  /**
    * Retrieve all calc properties, look at CF mts_meta.bars_property
    *
    * @return
    */
  def getAllCalcProperties: CalcProperties = {
    require(!session.isClosed)

    val bndBCalcProps = session.prepare(""" select * from mts_meta.bars_property """).bind()

    val bndLastBar = session.prepare(
      """ select ddate,ts_end
                    from mts_bars.last_bars
                   where ticker_id     = :tickerId and
                         bar_width_sec = :barDeepSec
                   limit 1 """).bind()

    val bndBCalcPropsDataSet = session.execute(bndBCalcProps).all().iterator.asScala

    /*
    val rowToLastBar = (row: Row) =>
      new LastBar(
        new Date(row.getDate("ddate").getMillisSinceEpoch),
        row.getLong("ts_end")
      )
    */

    val rowToCalcProperty = (rowCP: Row) => {

      val lb : Option[LastBar] =  (session.execute(bndLastBar
          .setInt("tickerId", rowCP.getInt("ticker_id"))
          .setInt("barDeepSec", rowCP.getInt("bar_width_sec"))
      ).all().iterator.asScala.toSeq map (row => {
        new LastBar(
          new Date(row.getDate("ddate").getMillisSinceEpoch),
          row.getLong("ts_end")
      )})).headOption

      new CalcProperty(
        rowCP.getInt("ticker_id"),
        rowCP.getInt("bar_width_sec"),
        rowCP.getInt("is_enabled"),
        lb match {case Some(bar) => Some(bar.dDate) case _ => None},
        lb match {case Some(bar) => Some(bar.tsEnd) case _ => None}
        //(lb match case Some(lb) => lb.dDate)

      )
    }

    CalcProperties(bndBCalcPropsDataSet
      .toSeq.map(rowToCalcProperty)
      /*
      .sortBy(sr => sr.tickerId)(Ordering[Int])
      .sortBy(sr => sr.barDeepSec)(Ordering[Int])
      */
    )
  }

  /**
    * Retrieve all last bars from mts_bars.last_bars for each key: (ticker_id,bar_width_sec)
    *
    * @param cProperties all properties that need calculation.
    */
  /*
  def getLastBars(cProperties: CalcProperties): LastBars = {
    require(!session.isClosed)

    val rowToLastBar = (row: Row, tickerId: Int, barDeepSec: Int) =>
      new LastBar(
        tickerId,
        barDeepSec,
        new Date(row.getDate("ddate").getMillisSinceEpoch),
        row.getLong("ts_end")
      )

    val bndLastBar = session.prepare(
      """ select ddate,ts_end
                    from mts_bars.last_bars
                   where ticker_id     = :tickerId and
                         bar_width_sec = :barDeepSec
                   limit 1 """).bind()

    LastBars(
      cProperties.cProps filter (cp => cp.isEnabled == 1)
        flatMap { cp =>
        session.execute(bndLastBar
          .setInt("tickerId", cp.tickerId)
          .setInt("barDeepSec", cp.barDeepSec))
          .all().iterator.asScala.toSeq map (row => rowToLastBar(row, cp.tickerId, cp.barDeepSec))
      }
    )
  }
*/

}

object DBCass {
  def apply(nodeAddress: String, dbType :String) = {
    new DBCass(nodeAddress,dbType)
  }
}