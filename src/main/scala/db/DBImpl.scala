package db

import bcstruct._
import com.datastax.driver.core
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
  def getTicksByInterval(cp :CalcProperty, tsBegin :Long, tsEnd :Long) : (seqTicksObj,Long)
  def getCalculatedBars(tickerId :Int, seqTicks :Seq[Tick], barDeepSec :Long) :Seq[Bar]
  def saveBars(seqBarsCalced :Seq[Bar])
  //def saveEmptyBar(pTickerId :Int,barDeepSec :Int, tsBegin :Long, tsEnd :Long)

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

  val bndBCalcProps = session.prepare(""" select * from mts_meta.bars_property """).bind()

  val bndLastBar = session.prepare(
    """   select ddate_begin,ddate_end,ts_begin,ts_end
                    from mts_bars.last_bars
                   where ticker_id     = :tickerId and
                         bar_width_sec = :barDeepSec
                   limit 1 """).bind()




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

  val bndTicksByTsInterval = session.prepare(
    """ select ticker_id,ddate,db_tsunx,ask,bid
            from mts_src.ticks
           where ticker_id = :tickerId and
                 db_tsunx >= :dbTsunxBegin and
                 db_tsunx <= :dbTsunxEnd
           allow filtering; """).bind()

  /**
    * Save one bar.
    */
  val bndSaveBar = session.prepare(
    """
        insert into mts_bars.bars(
        	  ticker_id,
        	  ddate,
        	  bar_width_sec,
            ts_begin,
            ts_end,
            o,
            h,
            l,
            c,
            h_body,
            h_shad,
            btype,
            ticks_cnt,
            disp,
            log_co
            )
        values(
        	  :p_ticker_id,
        	  :p_ddate,
        	  :p_bar_width_sec,
            :p_ts_begin,
            :p_ts_end,
            :p_o,
            :p_h,
            :p_l,
            :p_c,
            :p_h_body,
            :p_h_shad,
            :p_btype,
            :p_ticks_cnt,
            :p_disp,
            :p_log_co
            ); """).bind()

  val bndLastBars = session.prepare(
    """
        insert into mts_bars.last_bars(
        	   ticker_id,
        	   bar_width_sec,
             ddate_begin,
             ddate_end,
             ts_begin,
             ts_end
            )
        values(
        	  :p_ticker_id,
        	  :p_bar_width_sec,
            :p_ddate_begin,
            :p_ddate_end,
            :p_ts_begin,
            :p_ts_end
            ); """).bind()



  /**
    * Retrieve all calc properties, look at CF mts_meta.bars_property
    *
    * @return
    */
  def getAllCalcProperties: CalcProperties = {
    require(!session.isClosed)

    val bndBCalcPropsDataSet = session.execute(bndBCalcProps).all().iterator.asScala

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
          row.getDate("ddate_begin"),
          row.getLong("ts_begin"),
          row.getDate("ddate_end"),
          row.getLong("ts_end")
      )})).headOption
      //.sortBy(lb => lb.tsEnd)(Ordering[Long].reverse)

      logger.debug(">>>>>>>>>> lb.getOrElse(0L)="+lb.getOrElse(0L))

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
        lb match {case Some(bar) => Some(bar.dDateBegin) case _ => None},
        lb match {case Some(bar) => Some(bar.tsBegin) case _ => None},
        lb match {case Some(bar) => Some(bar.dDateEnd) case _ => None},
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
  def getTicksByInterval(cp :CalcProperty, tsBegin :Long, tsEnd :Long)  = {
    val t1 = System.currentTimeMillis
    val sqt = seqTicksObj(session.execute(bndTicksByTsInterval
      .setInt("tickerId", cp.tickerId)
      .setLong("dbTsunxBegin", tsBegin)
      .setLong("dbTsunxEnd", tsEnd)
    ).all().iterator.asScala.toSeq.map(r => rowToSeqTicks(r,cp.tickerId)).sortBy(t => t.db_tsunx)
    )
    val t2 = System.currentTimeMillis
    (sqt,(t2-t1))
    logger.debug("Inside getTicksByInterval. Read interval "+tsBegin+" - "+ tsEnd +" diration="+(t2-t1)+" Ticks Size="+sqt.sqTicks.size)
    (sqt,(t2-t1))
  }



  /**
    * Calculate seq of Bars from seq of Ticks.
    * @param seqTicks - seq of Ticks were read from DB in prev step.
    * @return
    */
  def getCalculatedBars(tickerId :Int, seqTicks :Seq[Tick], barDeepSec :Long) :Seq[Bar] = {

    val barsSides = seqTicks.head.db_tsunx.to(seqTicks.last.db_tsunx).by(barDeepSec)

    logger.debug("- barsSides.size=" + barsSides.size)
    logger.debug("from : " + seqTicks.head.db_tsunx)
    logger.debug("to   : " + seqTicks.last.db_tsunx)

    val seqBarSides = barsSides.zipWithIndex.map(elm => (elm._1, elm._2))
    logger.debug("seqBarSides.size= " + seqBarSides.size)

    val seqBar2Sides = for (i <- 0 to seqBarSides.size - 1) yield {
      if (i < seqBarSides.last._2)
        (seqBarSides(i)._1, seqBarSides(i + 1)._1, seqBarSides(i)._2 + 1)
      else
        (seqBarSides(i)._1, seqBarSides(i)._1, seqBarSides(i)._2 + 1)
    }

    def getGroupThisElement(elm: Long) = {
      seqBar2Sides.find(bs => ((bs._1 <= elm && bs._2 > elm) && (bs._2 - bs._1) == barDeepSec)).map(x => x._3).getOrElse(0)
    }

    val seqSeqTicks: Seq[(Int, Seq[Tick])] = seqTicks.groupBy(elm => getGroupThisElement(elm.db_tsunx)).filter(seqT => seqT._1 != 0).toSeq.sortBy(gr => gr._1)
    seqSeqTicks.filter(sb => sb._1 != 0).map(elm => new Bar(tickerId, (barDeepSec / 1000L).toInt, elm._2))
  }



  def saveBars(seqBarsCalced :Seq[Bar]) = {
     require(seqBarsCalced.nonEmpty,"Seq of Bars for saving is Empty.")

      for (b <- seqBarsCalced) {
        session.execute(bndSaveBar
          .setInt("p_ticker_id", b.ticker_id)
          .setDate("p_ddate",  core.LocalDate.fromMillisSinceEpoch(b.ddate)) //*1000
          .setInt("p_bar_width_sec",b.bar_width_sec)
          .setLong("p_ts_begin", b.ts_begin)
          .setLong("p_ts_end", b.ts_end)
          .setDouble("p_o",b.o)
          .setDouble("p_h",b.h)
          .setDouble("p_l",b.l)
          .setDouble("p_c",b.c)
          .setDouble("p_h_body",b.h_body)
          .setDouble("p_h_shad",b.h_shad)
          .setString("p_btype",b.btype)
          .setInt("p_ticks_cnt",b.ticks_cnt)
          .setDouble("p_disp",b.disp)
          .setDouble("p_log_co",b.log_co))
      }

      logger.debug("Saved "+seqBarsCalced.size+" bars into mts_bars.bars. Next save last_bar")

      //UPSERT LAST BAR FROM CALCED BARS seqBarsCalced
      val maxTsEndFromCalcedBars = seqBarsCalced.map(bfs => bfs.ts_end).max

      logger.debug("maxTsEndFromCalcedBars="+maxTsEndFromCalcedBars)

      val lastBarFromBars = seqBarsCalced.filter(b => b.ts_end == maxTsEndFromCalcedBars).head

      logger.debug("LAST BAR = " + lastBarFromBars)

      session.execute(bndLastBars
        .setInt("p_ticker_id", lastBarFromBars.ticker_id)
        .setInt("p_bar_width_sec", lastBarFromBars.bar_width_sec)
        .setDate("p_ddate_begin", core.LocalDate.fromMillisSinceEpoch(lastBarFromBars.ts_begin))
        .setDate("p_ddate_end", core.LocalDate.fromMillisSinceEpoch(lastBarFromBars.ts_end))
        .setLong("p_ts_begin", lastBarFromBars.ts_begin)
        .setLong("p_ts_end", lastBarFromBars.ts_end)
      )
  }

/*
  def saveEmptyBar(pTickerId :Int,barDeepSec :Int, tsBegin :Long, tsEnd :Long) = {

      session.execute(bndSaveBar
        .setInt("p_ticker_id",pTickerId)
        .setDate("p_ddate",  core.LocalDate.fromMillisSinceEpoch(tsEnd))
        .setInt("p_bar_width_sec",barDeepSec)
        .setLong("p_ts_begin", tsBegin)
        .setLong("p_ts_end", tsEnd)
        .setDouble("p_o",0)
        .setDouble("p_h",0)
        .setDouble("p_l",0)
        .setDouble("p_c",0)
        .setDouble("p_h_body",0)
        .setDouble("p_h_shad",0)
        .setString("p_btype", "n")
        .setInt("p_ticks_cnt",0)
        .setDouble("p_disp",0)
        .setDouble("p_log_co",0))


    //UPSERT LAST BAR FROM CALCED BARS seqBarsCalced
    session.execute(bndLastBars
      .setInt("p_ticker_id", pTickerId)
      .setInt("p_bar_width_sec", barDeepSec)
      .setDate("p_ddate", core.LocalDate.fromMillisSinceEpoch(tsEnd))
      .setLong("p_ts_end", tsEnd)
    )
  }
*/

}

object DBCass {
  def apply(nodeAddress: String, dbType :String) = {
    new DBCass(nodeAddress,dbType)
  }
}