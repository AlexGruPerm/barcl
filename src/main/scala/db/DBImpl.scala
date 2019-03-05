package db

import bcstruct._
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.{Cluster, LocalDate, Row, Session}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  *
  */
abstract class DBImpl(nodeAddress :String,dbType :String) {

  val supportedDb = Seq("cassandra","oracle"/*,"postgres"*/)

  def isClosed : Boolean
  def close()

  def getAllCalcProperties : CalcProperties
  def getTicksByInterval(tickerID :Int, tsBegin :Long, tsEnd :Long) : (seqTicksObj,Long)
  def getCalculatedBars(tickerId :Int, seqTicks :Seq[Tick], barDeepSec :Long) :Seq[Bar]

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

    /**
      * Last Tick ddate - Because:
      *  	PRIMARY KEY (ticker_id, ddate)
      * ) WITH CLUSTERING ORDER BY ( ddate DESC )
      */
    val bndLastTickDdate = session.prepare(""" select ddate from mts_src.ticks_count_days where ticker_id = :tickerId limit 1; """).bind()

    /**
      * Last Tick ts - Because:
      *   PRIMARY KEY (( ticker_id, ddate ), ts, db_tsunx)
      * ) WITH CLUSTERING ORDER BY ( ts DESC, db_tsunx DESC )
    */
    val bndLastTickTs = session.prepare(""" select db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate limit 1 """).bind()

    /**
      * ddate of First tick
    */
    val bndFirstTickDdate = session.prepare("""  select min(ddate) as ddate from mts_src.ticks_count_days where ticker_id = :tickerId; """).bind()

    /**
      * ts of first tick.
    */
    val bndFirstTickTs = session.prepare(""" select min(db_tsunx) as db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate """).bind()

    /**
      * Func wide description:
    */
    val rowToCalcProperty = (rowCP: Row) => {

      /**
        * Last Bar
        */
      val lb : Option[LastBar] =  (session.execute(bndLastBar
          .setInt("tickerId", rowCP.getInt("ticker_id"))
          .setInt("barDeepSec", rowCP.getInt("bar_width_sec"))
      ).all().iterator.asScala.toSeq map (row => {
        new LastBar(
          row.getDate("ddate"),
          row.getLong("ts_end")
      )})).headOption

      /**
        * Last Tick only ddate
        */
      val ltDdate : Option[LocalDate] = (session.execute(bndLastTickDdate
        .setInt("tickerId", rowCP.getInt("ticker_id"))
      ).all().iterator.asScala.toSeq map (row => row.getDate("ddate"))
        ).headOption

      val ltTS :Option[Long] = ltDdate
      match {
        case Some(ltd) => {
          (session.execute(bndLastTickTs
            .setInt("tickerId", rowCP.getInt("ticker_id"))
            .setDate("pDdate",ltd)
          ).all().iterator.asScala.toSeq map (row => row.getLong("db_tsunx"))
            ).headOption
        }
        case None => None
      }

      def firstTickTS :Option[Long] = {
        (session.execute(bndFirstTickDdate
          .setInt("tickerId", rowCP.getInt("ticker_id"))
        ).all().iterator.asScala.toSeq map (row => row.getDate("ddate")) filter(ld => ld != null)   ).headOption
        match {
          case Some(firstDdate) => {
            (session.execute(bndFirstTickTs
              .setInt("tickerId", rowCP.getInt("ticker_id"))
              .setDate("pDdate",  firstDdate)
            ).all().iterator.asScala.toSeq map (row => row.getLong("db_tsunx"))
              ).headOption
          }
          case None => None
        }
      }

      new CalcProperty(
        rowCP.getInt("ticker_id"),
        rowCP.getInt("bar_width_sec"),
        rowCP.getInt("is_enabled"),
        lb match {case Some(bar) => Some(bar.dDate) case _ => None},
        lb match {case Some(bar) => Some(bar.tsEnd) case _ => None},
        ltDdate,
        ltTS,
        lb match {case Some(bar) => Some(bar.tsEnd) case _ => firstTickTS}
      )
    }

    CalcProperties(bndBCalcPropsDataSet
      .toSeq.map(rowToCalcProperty)
      .sortBy(sr => sr.tickerId)(Ordering[Int])
      .sortBy(sr => sr.barDeepSec)(Ordering[Int])
    )
  }

  val bndTicksByTsInterval = session.prepare(
    """ select ticker_id,ddate,db_tsunx,ask,bid
            from mts_src.ticks
           where ticker_id = :tickerId and
                 db_tsunx >= :dbTsunxBegin and
                 db_tsunx <= :dbTsunxEnd
           allow filtering; """).bind()

  val rowToSeqTicks = (rowT: Row, tickerID :Int) => {
    new Tick(
      tickerID,//rowT.getInt("ticker_id"),
      rowT.getDate("ddate"),
      rowT.getLong("db_tsunx"),
      rowT.getDouble("ask"),
      rowT.getDouble("bid")
    )
  }

  /**
    * Read and return seq of ticks for this ticker_id and interval by ts: tsBegin - tsEnd (unix timestamp)
  */
  def getTicksByInterval(tickerID :Int, tsBegin :Long, tsEnd :Long)  = {
    val t1 = System.currentTimeMillis
    val sqt = seqTicksObj(session.execute(bndTicksByTsInterval
      .setInt("tickerId", tickerID)
      .setLong("dbTsunxBegin", tsBegin)
      .setLong("dbTsunxEnd", tsEnd)
    ).all().iterator.asScala.toSeq.map(r => rowToSeqTicks(r,tickerID)).sortBy(t => t.db_tsunx)
    )
    val t2 = System.currentTimeMillis
    (sqt,(t2-t1))
  }


  /**
    * Calculate seq of Bars from seq of Ticks.
    * @param seqTicks - seq of Ticks were read from DB in prev step.
    * @return
    */
  def getCalculatedBars(tickerId :Int, seqTicks :Seq[Tick], barDeepSec :Long) :Seq[Bar] ={

    val barsSides = seqTicks.head.db_tsunx.to(seqTicks.last.db_tsunx).by(barDeepSec)

    logger.info("- barsSides.size="+barsSides.size)
    logger.info("from : "+seqTicks.head.db_tsunx)
    logger.info("to   : "+seqTicks.last.db_tsunx)

    val seqBarSides = barsSides.zipWithIndex.map(elm => (elm._1,elm._2))
    logger.info("seqBarSides.size= "+seqBarSides.size)

    val seqBar2Sides = for(i <- 0 to seqBarSides.size-1) yield {
      if (i < seqBarSides.last._2)
        (seqBarSides(i)._1, seqBarSides(i+1)._1, seqBarSides(i)._2+1)
      else
        (seqBarSides(i)._1, seqBarSides(i)._1, seqBarSides(i)._2+1)
    }


    def getGroupThisElement(elm : Long)= {
      seqBar2Sides.find(bs => ((bs._1 <= elm && bs._2 > elm) && (bs._2 - bs._1) == barDeepSec)).map(x => x._3).getOrElse(0)
    }

    val seqSeqTicks = seqTicks.groupBy(elm => getGroupThisElement(elm.db_tsunx)).filter(seqT => seqT._1!=0 ).toSeq.sortBy(gr => gr._1)
    //Not last group where can be less then bar_width_sec
    val seqBarsCalced = for (seqTicksOneBar <- seqSeqTicks if seqTicksOneBar._1 != 0 ) yield {
      //logger.debug("          6. GROUP ID - seqTicksOneBar._1 = "+seqTicksOneBar._1)
      new Bar(
        p_ticker_id     = tickerId,
        p_bar_width_sec = (barDeepSec/1000L).toInt,
        barTicks        = seqTicksOneBar._2
      )
    }
    seqBarsCalced

  }



}

object DBCass {
  def apply(nodeAddress: String, dbType :String) = {
    new DBCass(nodeAddress,dbType)
  }
}