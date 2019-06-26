package db

import bcstruct.{barsForFutAnalyze, barsFutAnalyzeRes, barsMeta, barsResToSaveDB, _}
import com.datastax.driver.core
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core._
//import com.madhukaraphatak.sizeof.SizeEstimator
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

  def getFirstTicksMeta : Seq[FirstTickMeta]

  def getAllCalcProperties(fTicksMeta :Seq[FirstTickMeta]) : CalcProperties
  def getTicksByInterval(cp :CalcProperty, tsBegin :Long, tsEnd :Long) : (seqTicksObj,Long)
  def getCalculatedBars(tickerId :Int, seqTicks :Seq[Tick], barDeepSec :Long) :Seq[Bar]
  def saveBars(seqBarsCalced :Seq[Bar])
  //For Bar range calculator
  def getAllBarsHistMeta : Seq[barsMeta]
  def getLastBarFaTSEnd(tickerID :Int, bar_width_sec :Int)  :Option[LocalDate]
  def getAllCalcedBars(seqB :Seq[barsMeta]) :Seq[barsForFutAnalyze]
  def makeAnalyze(seqB :Seq[barsForFutAnalyze],p: Double) :Seq[barsFutAnalyzeRes]
  def saveBarsFutAnal(seqFA :Seq[barsResToSaveDB])
  // For FormsBuilder
  def getAllBarsFAMeta : Seq[barsFaMeta]
  def getMinDdateBFroms(tickerId :Int, barWidthSec :Int, prcntsDiv : Seq[Double], formDeepKoef :Int, resType :String) :Option[(LocalDate,Long)]
  def getAllFaBars(seqB :Seq[barsFaMeta], minDdate :Option[LocalDate], minTs :Option[Long]) :Seq[barsResToSaveDB]
  def filterFABars(seqB :Seq[barsResToSaveDB], intervalNewGroupKoeff :Int) :Seq[(Int,barsResToSaveDB)]
  def getTicksForForm(tickerID :Int, tsBegin :Long, tsEnd :Long, ddateEnd : LocalDate) :Seq[tinyTick]
  def getAllTicksForForms(tickerID :Int, firstBarOfLastBars :barsResToSaveDB, lastBarOfLastBars :barsResToSaveDB) :Seq[tinyTick]
  def saveForms(seqForms : Seq[bForm])
  //def getFaBarsFiltered(seqBars :Seq[barsFaData],resType :String,futureInterval :Double,groupIntervalSec :Int) :Seq[barsFaData]

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

      val ClusterConfig = sessInternal.getCluster.getConfiguration
      ClusterConfig.getSocketOptions.setConnectTimeoutMillis(180000)
      ClusterConfig.getSocketOptions.setReadTimeoutMillis(120000)
      ClusterConfig.getSocketOptions.setKeepAlive(true)
      ClusterConfig.getPoolingOptions.setIdleTimeoutSeconds(600) // 5 minutes.
      val protocolVersion = sessInternal.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion

      val poolingOptions = sessInternal.getCluster.getConfiguration.getPoolingOptions
      /*
      poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, connectionsPerHost)
      poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, connectionsPerHost)*/


      logger.debug(s"($getClass).getSession - Connection opened for [$dbType]. protocolVersion="+protocolVersion)

      logger.debug("CoreConnectionsPerHost(REMOTE) = "+poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE))
      logger.debug("HeartbeatIntervalSeconds = "+poolingOptions.getHeartbeatIntervalSeconds)
      logger.debug("IdleTimeoutSeconds = "+poolingOptions.getIdleTimeoutSeconds)


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

  val bndBCalcProps = session.prepare(""" select * from mts_meta.bars_property; """).bind()

  /*
  val lastBarMaxDdate = session.prepare(
    """ select max(ddate) as ddate from mts_bars.bars where ticker_id=:tickerId and bar_width_sec=:barDeepSec allow filtering; """).bind()
*/
  //optimization 26.06.2019
  val lastBarMaxDdate = session.prepare(
    """ select ddate from mts_bars.bars_bws_dates where ticker_id=:tickerId and bar_width_sec=:barDeepSec; """).bind()


  val bndLastBar = session.prepare(
    """  select ts_end from mts_bars.bars where ticker_id=:tickerId and bar_width_sec=:barDeepSec and ddate=:maxDdate limit 1;  """).bind()//allow filtering;


  /**
    * Last Tick ddate - Because:
    * PRIMARY KEY (ticker_id, ddate)
    * ) WITH CLUSTERING ORDER BY ( ddate DESC )
    */
  val bndLastTickDdate = session.prepare(""" select ddate from mts_src.ticks_count_days where ticker_id = :tickerId limit 1; """).bind()


  /**
    * Last Tick ts - Because:
    * PRIMARY KEY (( ticker_id, ddate ), ts, db_tsunx)
    * ) WITH CLUSTERING ORDER BY ( ts DESC, db_tsunx DESC )
    */
  val bndLastTickTs = session.prepare(""" select db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate limit 1 """).bind()

  /**
    * ddate of First tick
    */
  val bndFirstTickDdate = session.prepare("""  select min(ddate) as ddate from mts_src.ticks_count_days where ticker_id = :tickerId; """).bind()


  val bndTickers = session.prepare(""" select ticker_id from mts_meta.tickers; """).bind

  val bndFirstTicksDdateAll = session.prepare(""" select ticker_id, min(ddate) as ddate from mts_src.ticks_count_days group by ticker_id; """).bind()

  /**
    * ts of first tick.
    */
  //Optimized 26.06.2019 extremely values.
  //val bndFirstTickTs = session.prepare(""" select min(db_tsunx) as db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate """).bind()
  val bndFirstTickTs = session.prepare(""" select db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate order by ts,db_tsunx limit 1 """).bind()


  val bndTicksByTsInterval = session.prepare(
    """ select ddate,db_tsunx,ask,bid
            from mts_src.ticks
           where ticker_id = :tickerId and
                 ddate >= :pMinDate and
                 ddate <= :pMaxDate and
                 db_tsunx >= :dbTsunxBegin and
                 db_tsunx <= :dbTsunxEnd
           allow filtering; """).setIdempotent(true).bind()

  val bndTicksByTsIntervalONEDate = session.prepare(
    """ select db_tsunx,ask,bid
            from mts_src.ticks
           where ticker_id = :tickerId and
                 ddate     = :pDate and
                 db_tsunx >= :dbTsunxBegin and
                 db_tsunx <= :dbTsunxEnd
           allow filtering; """).setIdempotent(true).bind()


  val bndSaveBarDdatesMeta = session.prepare(" insert into mts_bars.bars_bws_dates(ticker_id,ddate,bar_width_sec) values(:tickerId,:pDdate,:bws); ").setIdempotent(true).bind()

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
            ); """).setIdempotent(true).bind()

  /*
  val bndBarsHistMeta = session.prepare(
    """ select distinct ticker_id,bar_width_sec,ddate from mts_bars.bars allow filtering; """).setIdempotent(true).bind()
*/
  val bndBarsHistMeta = session.prepare(
    """ select ticker_id,bar_width_sec,ddate from mts_bars.bars_bws_dates; """).bind()//.setIdempotent(true)

  //todo: may be more optimal will be remove ordering in app level from db. IN db data stored ordered by ts_end DESC.
  val bndBarsHistData = session.prepare(
    """ select ts_begin,ts_end,o,h,l,c
          from mts_bars.bars
         where ticker_id     = :p_ticker_id and
               bar_width_sec = :p_bar_width_sec and
               ddate         = :p_ddate
         order by ts_end; """).bind()//         allow filtering

  val bndSaveFa = session.prepare(
    """ insert into mts_bars.bars_fa(
      ticker_id,
      ddate,
      bar_width_sec,
      ts_end,
      c,
      log_oe,
      ts_end_res,
      dursec_res,
      ddate_res,
      c_res,
      res_type)
   values(
       :p_ticker_id,
       :p_ddate,
       :p_bar_width_sec,
       :p_ts_end,
       :p_c,
       :p_log_oe,
       :p_ts_end_res,
       :p_dursec_res,
       :p_ddate_res,
       :p_c_res,
       :p_res_type
       ) """).bind()
  /*
  val bndSaveFa =session.prepare(
    """ insert into mts_bars.bars_fa(ticker_id,    ddate,    bar_width_sec,    ts_end,     prcnt,    res_type,    res )
                                      values(:p_ticker_id, :p_ddate, :p_bar_width_sec, :p_ts_end, :p_prcnt, :p_res_type, :p_res) """)
  */

  val bndBarsFAMeta = session.prepare(
    """ select distinct ticker_id,ddate,bar_width_sec from mts_bars.bars_fa; """).bind()

  val bndBarsFormsMaxDdate = session.prepare(
    """ select max(ddate)  as ddate,
                       max(ts_end) as ts_end
                 from mts_bars.bars_forms
                where ticker_id     = :p_ticker_id and
                      bar_width_sec = :p_bar_width_sec and
                      formdeepkoef  = :p_formdeepkoef and
                      log_oe        = :p_log_oe and
                      res_type      = :p_res_type
                allow filtering;
       """).bind()

  val bndBarsFaLastNN = session.prepare(
    """ select max(ddate) as ddate
                                                      from mts_bars.bars_fa
                                                     where ticker_id     = :p_ticker_id and
                                                           bar_width_sec = :p_bar_width_sec
                                                     allow filtering; """).bind()

  val bndBarsFaData = session.prepare(
    """ select
                      ts_end,
                      c,
                      log_oe,
                      ts_end_res,
                      dursec_res,
                      ddate_res,
                      c_res,
                      res_type
                 from mts_bars.bars_fa
                where ticker_id     = :p_ticker_id and
                      bar_width_sec = :p_bar_width_sec and
                      ddate         = :p_ddate
                allow filtering; """).bind()


  val bndBarsFaDataRestTsEnd = session.prepare(
    """ select
                       ts_end,
                       c,
                       log_oe,
                       ts_end_res,
                       dursec_res,
                       ddate_res,
                       c_res,
                       res_type
                 from mts_bars.bars_fa
                where ticker_id     = :p_ticker_id and
                      bar_width_sec = :p_bar_width_sec and
                      ddate         = :p_ddate and
                      ts_end        > :p_ts_end
                allow filtering; """).bind()


  /**
    * may be here make double read, first determine ddate BEGIN by b_tsunx >= and second read with 2 ddates.
    */
  val bndTinyTicks = session.prepare(
    """ select db_tsunx,ask,bid
          from mts_src.ticks
         where ticker_id = :p_ticker_id and
               db_tsunx >= :p_ts_begin and
               db_tsunx <= :p_ts_end and
               ddate <= :p_ddate
         allow filtering """
  ).bind()


  val bndDdatesTicksByInter = session.prepare(
    """
      select distinct ticker_id,ddate
        from mts_src.ticks
       where ticker_id  = :p_ticker_id and
                 ddate <= :p_ddate_max and
                 ddate >= :p_ddate_min
        allow filtering
    """).bind()

  val bndAllTicksByDdate = session.prepare(
    """
      select db_tsunx,ask,bid
        from mts_src.ticks
       where ticker_id = :p_ticker_id and
             ddate     = :p_ddate
    """).bind()

  val bndSaveForms = session.prepare(
    """
       insert into mts_bars.bars_forms(
         	                             ticker_id,
                                       bar_width_sec,
      	                               ddate,
                                       ts_begin,
                                       ts_end,
                                   	   log_oe,
      	                               res_type,
                                       formDeepKoef,
                                       FormProps)
       values(
              :p_ticker_id,
              :p_bar_width_sec,
              :p_ddate,
              :p_ts_begin,
              :p_ts_end,
              :p_log_oe,
              :p_res_type,
              :p_formDeepKoef,
              :p_FormProps
             )
    """)




  def getFirstTs(tickerId :Int, thisDdate :LocalDate) :Long =
    session.execute(bndFirstTickTs.setInt("tickerId",tickerId).setDate("pDdate",thisDdate))
      .one().getLong("db_tsunx")



  def getFirstTicksMeta : Seq[FirstTickMeta] ={
    require(!session.isClosed)

    val tickersIds :Seq[Int] = session.execute(bndTickers)
      .all().iterator.asScala.map(row => row.getInt("ticker_id")).toList

    val (tMeta :Seq[(Int,LocalDate,Long)]) =
      session.execute(bndFirstTicksDdateAll)
      .all().iterator.asScala
      .map(row => (row.getInt("ticker_id"),row.getDate("ddate")))
      .map(elm => (elm._1,elm._2,getFirstTs(elm._1,elm._2))).toSeq

    tickersIds.map(thisTickerId =>
      FirstTickMeta(
        thisTickerId,
        tMeta.find(t => t._1 == thisTickerId).map(tf => tf._2),
        tMeta.find(t => t._1 == thisTickerId).map(tf => tf._3)
     )
    )

  }



  /**
    * Retrieve all calc properties, look at CF mts_meta.bars_property
    *
    * @return
    */
  def getAllCalcProperties(fTicksMeta :Seq[FirstTickMeta]): CalcProperties = {
    require(!session.isClosed)

    val bndBCalcPropsDataSet = session.execute(bndBCalcProps).all().iterator.asScala

    /**
      * Func wide description:
      */
    val rowToCalcProperty = (rowCP: Row) => {

      /**
        * Last bar max ddate
        */
      val maxDdate = session.execute(lastBarMaxDdate
        .setInt("tickerId", rowCP.getInt("ticker_id"))
        .setInt("barDeepSec", rowCP.getInt("bar_width_sec"))
      ).all().iterator.asScala.toSeq.map(row => {
        row.getDate("ddate")
      }).headOption //.head

      //select ddate from mts_bars.bars_bws_dates where ticker_id=:tickerId and bar_width_sec=:barDeepSec;

      /**
        * Last Bar
        */
      val lb: Option[LastBar] = (session.execute(bndLastBar
        .setInt("tickerId", rowCP.getInt("ticker_id"))
        .setInt("barDeepSec", rowCP.getInt("bar_width_sec"))
        .setDate("maxDdate",
          maxDdate match {
            case Some(lDate: LocalDate) => lDate
            case _ => core.LocalDate.fromMillisSinceEpoch(0)
        })
      ).all().iterator.asScala.toSeq map (row => {
        LastBar(
          row.getLong("ts_end")
        )
      })).headOption

      logger.debug(">>>>>>>>>> lb.getOrElse(0L)=" + lb.getOrElse(0L))

      /**
        * Last Tick only ddate
        */
      val ltDdate: Option[LocalDate] = (session.execute(bndLastTickDdate
        .setInt("tickerId", rowCP.getInt("ticker_id"))
      ).all().iterator.asScala.toSeq map (row => row.getDate("ddate"))
        ).headOption

      val ltTS: Option[Long] = ltDdate
      match {
        case Some(ltd) => {
          (session.execute(bndLastTickTs
            .setInt("tickerId", rowCP.getInt("ticker_id"))
            .setDate("pDdate", ltd)
          ).all().iterator.asScala.toSeq map (row => row.getLong("db_tsunx"))
            ).headOption
        }
        case None => None
      }

      /*
      --removed 26.056.2019 for optimization purpose.

      def firstTickTS: Option[Long] =
          session.execute(bndFirstTickDdate
          .setInt("tickerId", rowCP.getInt("ticker_id")))
          .all().iterator.asScala.toSeq map (row => row.getDate("ddate")) find (ld => ld != null)
        match {
          case Some(firstDdate) => {
            (session.execute(bndFirstTickTs
              .setInt("tickerId", rowCP.getInt("ticker_id"))
              .setDate("pDdate", firstDdate)
            ).all().iterator.asScala.toSeq map (row => row.getLong("db_tsunx"))
              ).headOption
          }
          case None => None
        }
      */

      val thisRowTickerID :Int = rowCP.getInt("ticker_id")

       CalcProperty(
         thisRowTickerID,
         rowCP.getInt("bar_width_sec"),
         rowCP.getInt("is_enabled"),
         lb match {
           case Some(bar) => Some(bar.tsEnd)
           case _ => None
         },
         ltDdate,
         ltTS,
         lb match {
           case Some(bar) => Some(bar.tsEnd)
           case _ => fTicksMeta.find(_.tickerId == thisRowTickerID) match {
             case Some(foundedFirstTicksMeta) => foundedFirstTicksMeta.firstTs
             case _ => None
           }
         }
       )

    }

    CalcProperties(bndBCalcPropsDataSet
      .toSeq.map(rowToCalcProperty)
      //.sortBy(sr => (sr.tickerId, sr.barDeepSec)) // todo: opt#2 maybe remove sorts at all.
    )
  }


  val rowToSeqTicks = (rowT: Row, tickerID: Int) => {
     Tick(
      tickerID,
      rowT.getDate("ddate"),
      rowT.getLong("db_tsunx"),
      rowT.getDouble("ask"),
      rowT.getDouble("bid")
    )
  }

  val rowToSeqTicksWDate = (rowT: Row, tickerID: Int, pDate :LocalDate) => {
    Tick(
      tickerID,
      pDate,
      rowT.getLong("db_tsunx"),
      rowT.getDouble("ask"),
      rowT.getDouble("bid")
    )
  }

  val rowToBarMeta = (row: Row) => {
     barsMeta(
      row.getInt("ticker_id"),
      row.getInt("bar_width_sec"),
      row.getDate("ddate"))
  }

  val rowToBarData = (row: Row, tickerID: Int, barWidthSec: Int, dDate: LocalDate) => {
     barsForFutAnalyze(
      tickerID,
      barWidthSec,
      dDate,
      row.getLong("ts_begin"),
      row.getLong("ts_end"),
      row.getDouble("o"),
      row.getDouble("h"),
      row.getDouble("l"),
      row.getDouble("c")
    )
  }

  val rowToBarFAMeta = (row: Row) => {
     barsFaMeta(
      row.getInt("ticker_id"),
      row.getInt("bar_width_sec"),
      row.getDate("ddate"),
      0L
    )
  }

  val rowToBarFAData = (row: Row, tickerID: Int, barWidthSec: Int, dDate: LocalDate) => {
     barsResToSaveDB(
      tickerID,
      dDate,
      barWidthSec,
      row.getLong("ts_end"),
      row.getDouble("c"),
      row.getDouble("log_oe"),
      row.getLong("ts_end_res"),
      row.getInt("dursec_res"),
      row.getDate("ddate_res"),
      row.getDouble("c_res"),
      row.getString("res_type")
    )
  }

  val rowToTinyTick = (row: Row) => {
     tinyTick(
      row.getLong("db_tsunx"),
      row.getDouble("ask"),
      row.getDouble("bid")
    )
  }

  /**
    * Read and return seq of ticks for this ticker_id and interval by ts: tsBegin - tsEnd (unix timestamp)
    */
  //todo :opt#3 Maybe use 2 queries, when ddate_begin=ddate-end Use = in query, or >= and <=
  def getTicksByInterval(cp: CalcProperty, tsBegin: Long, tsEnd: Long) = {
    val t1 = System.currentTimeMillis

    val pDate :LocalDate = LocalDate.fromMillisSinceEpoch(tsBegin)

    val sqt = if (LocalDate.fromMillisSinceEpoch(tsBegin) == LocalDate.fromMillisSinceEpoch(tsEnd)) {
      seqTicksObj(session.execute(bndTicksByTsIntervalONEDate
        .setInt("tickerId", cp.tickerId)
        .setDate("pDate", pDate)
        .setLong("dbTsunxBegin", tsBegin)
        .setLong("dbTsunxEnd", tsEnd)
      ).all().iterator.asScala.toSeq.map(r => rowToSeqTicksWDate(r, cp.tickerId, pDate)).sortBy(t => t.db_tsunx))
    } else {
      seqTicksObj(session.execute(bndTicksByTsInterval
        .setInt("tickerId", cp.tickerId)
        .setDate("pMinDate", LocalDate.fromMillisSinceEpoch(tsBegin))
        .setDate("pMaxDate", LocalDate.fromMillisSinceEpoch(tsEnd))
        .setLong("dbTsunxBegin", tsBegin)
        .setLong("dbTsunxEnd", tsEnd)
      ).all().iterator.asScala.toSeq.map(r => rowToSeqTicks(r, cp.tickerId)).sortBy(t => t.db_tsunx))
    }





    val t2 = System.currentTimeMillis
    logger.debug("Inside getTicksByInterval. Read interval " + tsBegin + " - " + tsEnd + " diration=" + (t2 - t1) + " Ticks Size=" + sqt.sqTicks.size)
    (sqt, t2 - t1)
  }


  /*
  def debugTsPoints(tsPoints :Seq[Long],seqTicks: Seq[Tick]) ={
    logger.debug("- barsSides.size=" + tsPoints.size)
    logger.debug("from : " + seqTicks.head.db_tsunx)
    logger.debug("to   : " + seqTicks.last.db_tsunx)
  }
  */


  /**
    * Calculate seq of Bars from seq of Ticks.
    */
  def getCalculatedBars(tickerId: Int, seqTicks: Seq[Tick], barDeepSec: Long): Seq[Bar] = {
    val seqBarSides :Seq[TsPoint] = seqTicks.head.db_tsunx.to(seqTicks.last.db_tsunx).by(barDeepSec)
                                    .zipWithIndex
                                    .map(TsPoint.create)

    val seqBarRanges :Seq[TsIntervalGrp] = seqBarSides.init.zip(seqBarSides.tail)
      .map(TsIntervalGrp.create)

    def getGroupThisElement(elm: Long) :Int =
      seqBarRanges.find(bs => bs.tsBegin <= elm && bs.tsEnd > elm)
        .map(_.groupNumber).getOrElse(0)

      seqTicks.groupBy(elm => getGroupThisElement(elm.db_tsunx))
      .filter(_._1 != 0).toSeq.sortBy(_._1)
      .map(elm => new Bar(tickerId, (barDeepSec/1000L).toInt, elm._2))
  }


  def saveBars(seqBarsCalced: Seq[Bar]) :Unit = {
    require(seqBarsCalced.nonEmpty, "Seq of Bars for saving is Empty.")

    for (b <- seqBarsCalced) {
      session.execute(bndSaveBar
        .setInt("p_ticker_id", b.ticker_id)
        .setDate("p_ddate", core.LocalDate.fromMillisSinceEpoch(b.ddate)) //*1000
        .setInt("p_bar_width_sec", b.bar_width_sec)
        .setLong("p_ts_begin", b.ts_begin)
        .setLong("p_ts_end", b.ts_end)
        .setDouble("p_o", b.o)
        .setDouble("p_h", b.h)
        .setDouble("p_l", b.l)
        .setDouble("p_c", b.c)
        .setDouble("p_h_body", b.h_body)
        .setDouble("p_h_shad", b.h_shad)
        .setString("p_btype", b.btype)
        .setInt("p_ticks_cnt", b.ticks_cnt)
        .setDouble("p_disp", b.disp)
        .setDouble("p_log_co", b.log_co))
    }

    val (tickerId :Int, bws :Int, maxDdate :LocalDate) = (
      seqBarsCalced.head.ticker_id,
      seqBarsCalced.head.bar_width_sec,
      core.LocalDate.fromMillisSinceEpoch(seqBarsCalced.maxBy(_.ddate).ddate))

    session.execute(bndSaveBarDdatesMeta
      .setInt("tickerId",tickerId)
      .setDate("pDdate",maxDdate)
        .setInt("bws",bws)
    )

  }


  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.getDaysSinceEpoch)

  /**
    * Read and return all information about calced bars.
    * distinct ticker_id, bar_width_sec, ddate from mts_bars.bars
    *
    */
  def getAllBarsHistMeta: Seq[barsMeta] = {
    session.execute(bndBarsHistMeta).all().iterator.asScala.toSeq.map(r => rowToBarMeta(r))
      //.filter(r => Seq(20).contains(r.tickerId) /*&& Seq(30/*300,600,1800,3600*/).contains(r.barWidthSec)*/) //-------------------------------------------------------------- !!!!!!!!!!!!!!!!!!
      .sortBy(sr => (sr.tickerId, sr.barWidthSec, sr.dDate))
    //read here ts_end for each pairs:sr.tickerId,sr.barWidthSec for running Iterations in loop.
  }


  def getLastBarFaTSEnd(tickerID: Int, bar_width_sec: Int): Option[LocalDate] = {
    Option(session.execute(bndBarsFaLastNN
      .setInt("p_ticker_id", tickerID)
      .setInt("p_bar_width_sec", bar_width_sec))
      .one().getDate("ddate"))
  }


  /**
    * Read all bars from mts_bars.bars by input filtered seqB (contains same tickerID, bar_width_sec and differnet ddates)
    * Read date by date for key: ticker+bws
    *
    **/
  def getAllCalcedBars(seqB: Seq[barsMeta]): Seq[barsForFutAnalyze] = {
    seqB.flatMap(sb => session.execute(bndBarsHistData
      .setInt("p_ticker_id", sb.tickerId)
      .setInt("p_bar_width_sec", sb.barWidthSec)
      .setDate("p_ddate", sb.dDate))
      .all()
      .iterator.asScala.toSeq.map(r => rowToBarData(r, sb.tickerId, sb.barWidthSec, sb.dDate))
      .sortBy(sr => (sr.tickerId, sr.barWidthSec, sr.dDate, sr.ts_end))
    )
  }

  /**
    *
    * Make future analyze, for each bar from seqB look in futuer and determin is it exist conditions.
    * Calculate for each bar and for each seqPrcnts (typical values : 5,10,15 percents).
    *
    */
  def makeAnalyze(seqB: Seq[barsForFutAnalyze], p: Double): Seq[barsFutAnalyzeRes] = {
    for (
      currBarWithIndex <- seqB.zipWithIndex;
      currBar = currBarWithIndex._1;
      searchSeq = seqB.drop(currBarWithIndex._2+1)
  ) yield {
    val pUp :Double = (Math.exp( Math.log(currBar.c) + p)* 10000).round / 10000.toDouble
    val pDw :Double = (Math.exp( Math.log(currBar.c) - p)* 10000).round / 10000.toDouble

    searchSeq.find(srcElm => (pUp >= srcElm.minOHLC && pUp <= srcElm.maxOHLC) ||
                      (pDw >= srcElm.minOHLC && pDw <= srcElm.maxOHLC) ||
                      (pUp <= srcElm.maxOHLC && pDw >= srcElm.minOHLC)
      ) match {
      case Some(fBar) => {
        if (pUp >= fBar.minOHLC && pUp <= fBar.maxOHLC) barsFutAnalyzeRes(currBar,Some(fBar),p,"mx")
        else if (pDw >= fBar.minOHLC && pDw <= fBar.maxOHLC) barsFutAnalyzeRes(currBar,Some(fBar),p,"mn")
        else if (pUp <= fBar.maxOHLC && pDw >= fBar.minOHLC) barsFutAnalyzeRes(currBar,Some(fBar),p,"bt")
         else barsFutAnalyzeRes(currBar,None,p,"nn")
      }
      case None => barsFutAnalyzeRes(currBar,None,p,"nn")
    }
    }
  }



  /**
    * Save results of Future analyze into DB mts_bars.bars_fa
    */
  def saveBarsFutAnal(seqFA :Seq[barsResToSaveDB]) :Unit = {
     for(t <- seqFA) {
       session.execute(bndSaveFa
         .setInt("p_ticker_id", t.tickerId)
         .setDate("p_ddate", t.dDate)
         .setInt("p_bar_width_sec", t.barWidthSec)
         .setLong("p_ts_end", t.ts_end)
         .setDouble("p_c", t.c)
         .setDouble("p_log_oe", t.log_oe)
         .setLong("p_ts_end_res", t.ts_end_res)
         .setInt("p_dursec_res", t.dursec_res)
         .setDate("p_ddate_res", t.ddate_res)
         .setDouble("p_c_res", t.c_res)
         .setString("p_res_type", t.res_type)
       )
     }

    /*
        val partsSeqBarFa = seqFA.grouped(20)
    for(thisPartOfSeq <- partsSeqBarFa) {
      var batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
      thisPartOfSeq.foreach {
        t =>
          batch.add(bndSaveFa.bind()
            .setInt("p_ticker_id", t.tickerId)
            .setDate("p_ddate", t.dDate)
            .setInt("p_bar_width_sec",t.barWidthSec)
            .setLong("p_ts_end", t.ts_end)
            .setDouble("p_c",t.c)
            .setDouble("p_log_oe",t.log_oe)
            .setLong("p_ts_end_res", t.ts_end_res)
            .setInt("p_dursec_res",t.dursec_res)
            .setDate("p_ddate_res",t.ddate_res)
            .setDouble("p_c_res",t.c_res)
            .setString("p_res_type", t.res_type)
          )
      }
      session.execute(batch)
    }
    */
  }
  /** ------------------------------------------------------- */

  def getAllBarsFAMeta : Seq[barsFaMeta] ={
    session.execute(bndBarsFAMeta).all().iterator.asScala.toSeq.map(r => rowToBarFAMeta(r))
       //.filter(r =>  r.tickerId == 18 /*&& Seq(30).contains(r.barWidthSec)*/) //-------------------------------------------------------------- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      .sortBy(sr => (sr.tickerId,sr.barWidthSec,sr.dDate))
  }


  def getDdateTsEndFromRow(row :Row) : Option[(LocalDate,Long)] = {
    Option(row.getDate("ddate")) match {
      case Some(ld) => Some((ld,row.getLong("ts_end")))
      case None => None
    }
  }


  /**
    * Read max ddate from mts_bars.bars_forms for each key ticker_id, bar_width_sec, formdeepkoef, prcnt
    * and return minimal one.
  */
  def getMinDdateBFroms(tickerId :Int, barWidthSec :Int, prcntsDiv : Seq[Double], formDeepKoef :Int, resType :String) :Option[(LocalDate,Long)] ={
    prcntsDiv.map(
      pr => getDdateTsEndFromRow(session.execute(bndBarsFormsMaxDdate
        .setInt("p_ticker_id", tickerId)
        .setInt("p_bar_width_sec", barWidthSec)
        .setInt("p_formdeepkoef", formDeepKoef)
        .setDouble("p_log_oe", pr)
        .setString("p_res_type", resType)
      ).one())
    ).collect {case Some(d) => d}.toList
    match {
      case List() => None
      case nel: List[(LocalDate, Long)] => Option(nel.minBy(_._2))
    }
  }



  /**
    *
    * Read all data for each ddata by key: ticker_id and barWidthSec OR
    * read just part begin from (LocalDate,Long) if something already calculated in bars_forms.
    * We don't need to sort by sr.tickerId, sr.barWidthSec because this function calls for single tickerId + barWidthSec
    * look at :
    * val allFABars = dbInst.getAllFaBars(barsFam.filter(r => r.tickerId == tickerId &&
    *                                                    r.barWidthSec == barWidthSec) .... )
  */
  def getAllFaBars(seqB :Seq[barsFaMeta], minDdate :Option[LocalDate], minTs :Option[Long]) :Seq[barsResToSaveDB] = {
    (minDdate,minTs) match {
      case (Some(minDdate),Some(tsEndBegin)) =>
        seqB.withFilter(b => b.dDate.getMillisSinceEpoch >= minDdate.getMillisSinceEpoch)
          .flatMap(sb => session.execute(bndBarsFaDataRestTsEnd
            .setInt("p_ticker_id", sb.tickerId)
            .setInt("p_bar_width_sec", sb.barWidthSec)
            .setDate("p_ddate", sb.dDate)
            .setLong("p_ts_end", tsEndBegin))
            .all().iterator.asScala.toSeq.map(r => rowToBarFAData(r, sb.tickerId, sb.barWidthSec, sb.dDate))
            .sortBy(_.ts_end))
      case _ =>
        seqB.flatMap(sb => session.execute(bndBarsFaData
          .setInt("p_ticker_id", sb.tickerId)
          .setInt("p_bar_width_sec", sb.barWidthSec)
          .setDate("p_ddate", sb.dDate))
          .all().iterator.asScala.toSeq.map(r => rowToBarFAData(r, sb.tickerId, sb.barWidthSec, sb.dDate))
          .sortBy(_.ts_end))
    }
  }



  /**
    * Filter source seq of FABars and group it groups with group number,
    * step between groups is related by groupIntervalSec (seconds)
    * Do it in recursive style.
    *
    * FE (step = 2): 1,2,3,6,7,8,11,12,15
    * converted into
    * (1,(1,2,3)) (2,(6,7,8)) (3,(11,12))  (4,(15))
    *
    */
  def filterFABars(seqB :Seq[barsResToSaveDB], intervalNewGroupKoeff :Int) : Seq[(Int,barsResToSaveDB)] = {
    val groupIntervalSec = seqB.head.barWidthSec * intervalNewGroupKoeff
    val acc_bar = seqB.head

    /**
      * Grouping sequences of bars. Excluding neighboring bars.
    */
    val r = seqB.tail.foldLeft(List((1,acc_bar))) ((acc :List[(Int,barsResToSaveDB)],elm :barsResToSaveDB) =>
      if ((elm.ts_end - acc.head._2.ts_end)/1000L < groupIntervalSec)
        ((acc.head._1, elm) :: acc)
      else
        ((acc.head._1+1, elm) :: acc)
    ).reverse

    r.groupBy(elm => elm._1).map(
      s => (s._1,s._2.filter(
        e => e._2.ts_end == (s._2.map(
          b => b._2.ts_end).max)
      ))
    ).toSeq.map(elm => elm._2).flatten.sortBy(e => e._2.ts_end)

  }
  /** ---------------------------------------------------------------------------------------- */






  /**
    * read all ticks for this Form ts interval. For deep micro structure analyze.
  */
  // !!! remove it as useless !!!
  def getTicksForForm(tickerID :Int, tsBegin :Long, tsEnd :Long, ddateEnd : LocalDate) :Seq[tinyTick] = {
    session.execute(bndTinyTicks
      .setInt("p_ticker_id", tickerID)
      .setLong("p_ts_begin",tsBegin)
      .setLong("p_ts_end",tsEnd)
      .setDate("p_ddate",ddateEnd))
      .all()
      .iterator.asScala.toSeq.map(r => rowToTinyTick(r/*, sb.tickerId, sb.barWidthSec, sb.dDate*/))
      .sortBy(sr => (sr.db_tsunx))
  }

  /**
    * Read all ticks for this tickerID and ts interval, additional less than ddateMax
    */
  def getAllTicksForForms(tickerID :Int, firstBarOfLastBars :barsResToSaveDB, lastBarOfLastBars :barsResToSaveDB) :Seq[tinyTick] ={
    //val tsMin    :Long = firstBarOfLastBars.ts_end
    //val tsMax    :Long = lastBarOfLastBars.ts_end
    //val ddateMin :LocalDate = firstBarOfLastBars.dDate
    //val ddateMax :LocalDate = lastBarOfLastBars.dDate

    val (tsMin :Long, ddateMin :LocalDate) = (firstBarOfLastBars.ts_end, firstBarOfLastBars.dDate)
    val (tsMax :Long, ddateMax :LocalDate) = (lastBarOfLastBars.ts_end, lastBarOfLastBars.dDate)

    logger.debug("1. INTERNAL READ TICKS (" + (tickerID) + ") (ddateMin,ddateMax) = ( "+ddateMin+" , "+ddateMax+" )   (tsMin,tsMax) = ( "+tsMin+" , "+tsMax+" )")

    session.execute(bndDdatesTicksByInter
        .setInt( "p_ticker_id",tickerID)
        .setDate("p_ddate_max",ddateMax)
        .setDate("p_ddate_min",ddateMin))
      .all()
      .iterator.asScala.toSeq
      .map(r => (r.getInt("ticker_id"),r.getDate("ddate"))).sortBy(e => e._2)
      .collect {
      case (tickerID :Int, ddate :LocalDate) =>
          val seqTickOneDDate :Seq[tinyTick] =
            session.execute(bndAllTicksByDdate
              .setInt("p_ticker_id",tickerID)
              .setDate("p_ddate",ddate))
              .all().iterator.asScala.toSeq
              .map(r => new tinyTick(
                r.getLong("db_tsunx"),
                r.getDouble("ask"),
                r.getDouble("bid"))
              )
          logger.debug("  2. INTERNAL READ TICKS (" + (tickerID,ddate) + ") ROWS = "+seqTickOneDDate.size+" SIZE = "+
            /*SizeEstimator.estimate(seqTickOneDDate)/1024L/1024L+*/" Mb.")
        seqTickOneDDate.sortBy(e => e.db_tsunx)
    }.flatten.filter(elm => elm.db_tsunx >= tsMin && elm.db_tsunx <= tsMax)
  }
  /** --------------------------------------------------------------------------------------- */

  /**
    * Save all calculated forms of bars into DB.
    * Save separated by partition key, because batch inserts.
  */
  def saveForms(seqForms : Seq[bForm]) = {
    seqForms.map(f => f.dDate).distinct.toList.collect {
      case  thisDdate =>
        val parts = seqForms.filter(tf => tf.dDate == thisDdate).grouped(100)//other limit 65535 for tiny rows.
        for(thisPartOfSeq <- parts) {
          var batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
          thisPartOfSeq.foreach {
            t =>
              batch.add(bndSaveForms.bind()
                .setInt("p_ticker_id", t.tickerId)
                .setInt("p_bar_width_sec",t.barWidthSec)
                .setDate("p_ddate", t.dDate)
                .setLong("p_ts_begin", t.TsBegin)
                .setLong("p_ts_end", t.TsEnd)
                .setDouble("p_log_oe",t.log_oe)
                .setString("p_res_type",t.resType)
                .setInt("p_formDeepKoef", t.formDeepKoef)
                .setMap("p_FormProps", t.FormProps.asJava)
              )
          }
          session.execute(batch)
        }
    }
  }
  /** --------------------------------------------------------------------------------------- */

}

object DBCass {
  def apply(nodeAddress: String, dbType :String) = {
    new DBCass(nodeAddress,dbType)
  }
}

