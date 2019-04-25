package db

import bcstruct.{barsForFutAnalyze, barsMeta, barsResToSaveDB, _}
import com.datastax.driver.core
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core._
import com.madhukaraphatak.sizeof.SizeEstimator
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
  //For Bar range calculator
  def getAllBarsHistMeta : Seq[barsMeta]
  def getLastBarFaTSEnd(tickerID :Int, bar_width_sec :Int)  :Option[LocalDate]
  def getAllCalcedBars(seqB :Seq[barsMeta]) :Seq[barsForFutAnalyze]
  def makeAnalyze(seqB :Seq[barsForFutAnalyze],p: Double) :Seq[barsFutAnalyzeRes]
  def saveBarsFutAnal(seqFA :Seq[barsResToSaveDB])
  // For FormsBuilder
  def getAllBarsFAMeta : Seq[barsFaMeta]
  def getMinDdateBFroms(tickerId :Int, barWidthSec :Int, prcntsDiv : Seq[Double], formDeepKoef :Int, resType :String) :Option[(LocalDate,Long)]
  def getAllFaBars(seqB :Seq[barsFaMeta],minDdateTsFromBForms :Option[(LocalDate,Long)]) :Seq[barsResToSaveDB]
  def filterFABars(seqB :Seq[barsResToSaveDB], intervalNewGroupKoeff :Int) :Seq[(Int,barsResToSaveDB)]
  def getTicksForForm(tickerID :Int, tsBegin :Long, tsEnd :Long, ddateEnd : LocalDate) :Seq[tinyTick]
  def getAllTicksForForms(tickerID :Int, tsMin :Long, tsMax :Long, ddateMin : LocalDate, ddateMax : LocalDate) :Seq[tinyTick]
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
      //ClusterConfig.getSocketOptions.setConnectTimeoutMillis(60000)
      ClusterConfig.getSocketOptions.setReadTimeoutMillis(60000)
      ClusterConfig.getPoolingOptions.setIdleTimeoutSeconds(300) // 5 minutes.
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

  val lastBarMaxDdate = session.prepare(
    """ select max(ddate) as ddate from mts_bars.bars where ticker_id=:tickerId and bar_width_sec=:barDeepSec allow filtering; """).bind()

  val bndLastBar = session.prepare(
    """  select ts_end from mts_bars.bars where ticker_id=:tickerId and bar_width_sec=:barDeepSec and ddate=:maxDdate limit 1 allow filtering;  """).bind()


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

  /**
    * ts of first tick.
    */
  val bndFirstTickTs = session.prepare(""" select min(db_tsunx) as db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate """).bind()

  //ticker_id,ddate,db_tsunx,ask,bid
  val bndTicksByTsInterval = session.prepare(
    """ select ddate,db_tsunx,ask,bid
            from mts_src.ticks
           where ticker_id = :tickerId and
                 ddate >= :pMinDate and
                 ddate <= :pMaxDate and
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

  val bndBarsHistMeta = session.prepare(
    """ select distinct ticker_id,bar_width_sec,ddate from mts_bars.bars allow filtering; """).bind()

  val bndBarsHistData = session.prepare(
    """ select ts_begin,ts_end,o,h,l,c
          from mts_bars.bars
         where ticker_id     = :p_ticker_id and
               bar_width_sec = :p_bar_width_sec and
               ddate         = :p_ddate
         order by ts_end
         allow filtering; """).bind()

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
       ) """)
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
        * Last bar max ddate
        */
      val maxDdate = session.execute(lastBarMaxDdate
        .setInt("tickerId", rowCP.getInt("ticker_id"))
        .setInt("barDeepSec", rowCP.getInt("bar_width_sec"))
      ).all().iterator.asScala.toSeq.map(row => {
        row.getDate("ddate")
      }).head

      /**
        * Last Bar
        */
      val lb: Option[LastBar] = (session.execute(bndLastBar
        .setInt("tickerId", rowCP.getInt("ticker_id"))
        .setInt("barDeepSec", rowCP.getInt("bar_width_sec"))
        .setDate("maxDdate", (maxDdate match {
          case null => core.LocalDate.fromMillisSinceEpoch(0)
          case lDate: LocalDate => lDate
        }))
      ).all().iterator.asScala.toSeq map (row => {
        new LastBar(
          row.getLong("ts_end")
        )
      })).headOption
      //.sortBy(lb => lb.tsEnd)(Ordering[Long].reverse)

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

      def firstTickTS: Option[Long] = {
        (session.execute(bndFirstTickDdate
          .setInt("tickerId", rowCP.getInt("ticker_id"))
        ).all().iterator.asScala.toSeq map (row => row.getDate("ddate")) filter (ld => ld != null)).headOption
        match {
          case Some(firstDdate) => {
            //logger.debug("----------------- firstDdate inside firstTickTS  is = "+firstDdate)
            (session.execute(bndFirstTickTs
              .setInt("tickerId", rowCP.getInt("ticker_id"))
              .setDate("pDdate", firstDdate)
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
        //lb match {case Some(bar) => Some(bar.dDateBegin) case _ => None},
        //lb match {case Some(bar) => Some(bar.tsBegin) case _ => None},
        //lb match {case Some(bar) => Some(bar.dDateEnd) case _ => None},
        lb match { case Some(bar) => Some(bar.tsEnd) case _ => None },
        ltDdate,
        ltTS,
        lb match { case Some(bar) => Some(bar.tsEnd) case _ => firstTickTS }
      )
    }

    CalcProperties(bndBCalcPropsDataSet
      .toSeq.map(rowToCalcProperty)
      .sortBy(sr => (sr.tickerId, sr.barDeepSec)) //(Ordering[Int])
      //.sortBy(sr => sr.barDeepSec)(Ordering[Int])
    )
  }


  val rowToSeqTicks = (rowT: Row, tickerID: Int) => {
    new Tick(
      tickerID,
      rowT.getDate("ddate"),
      rowT.getLong("db_tsunx"),
      rowT.getDouble("ask"),
      rowT.getDouble("bid")
    )
  }

  val rowToBarMeta = (row: Row) => {
    new barsMeta(
      row.getInt("ticker_id"),
      row.getInt("bar_width_sec"),
      row.getDate("ddate"))
  }

  val rowToBarData = (row: Row, tickerID: Int, barWidthSec: Int, dDate: LocalDate) => {
    new barsForFutAnalyze(
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
    new barsFaMeta(
      row.getInt("ticker_id"),
      row.getInt("bar_width_sec"),
      row.getDate("ddate"),
      0L
    )
  }

  val rowToBarFAData = (row: Row, tickerID: Int, barWidthSec: Int, dDate: LocalDate) => {
    new barsResToSaveDB(
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
    new tinyTick(
      row.getLong("db_tsunx"),
      row.getDouble("ask"),
      row.getDouble("bid")
    )
  }

  /**
    * Read and return seq of ticks for this ticker_id and interval by ts: tsBegin - tsEnd (unix timestamp)
    */
  def getTicksByInterval(cp: CalcProperty, tsBegin: Long, tsEnd: Long) = {
    val t1 = System.currentTimeMillis
    val sqt = seqTicksObj(session.execute(bndTicksByTsInterval
      .setInt("tickerId", cp.tickerId)
      .setDate("pMinDate", core.LocalDate.fromMillisSinceEpoch(tsBegin))
      .setDate("pMaxDate", core.LocalDate.fromMillisSinceEpoch(tsEnd))
      .setLong("dbTsunxBegin", tsBegin)
      .setLong("dbTsunxEnd", tsEnd)
    ).all().iterator.asScala.toSeq.map(r => rowToSeqTicks(r, cp.tickerId)).sortBy(t => t.db_tsunx)
    )
    val t2 = System.currentTimeMillis
    (sqt, (t2 - t1))
    logger.debug("Inside getTicksByInterval. Read interval " + tsBegin + " - " + tsEnd + " diration=" + (t2 - t1) + " Ticks Size=" + sqt.sqTicks.size)
    (sqt, (t2 - t1))
  }


  /**
    * Calculate seq of Bars from seq of Ticks.
    *
    * @param seqTicks - seq of Ticks were read from DB in prev step.
    * @return
    */
  def getCalculatedBars(tickerId: Int, seqTicks: Seq[Tick], barDeepSec: Long): Seq[Bar] = {

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
    seqSeqTicks.filter(sb => sb._1 != 0).map(elm =>
      new Bar(
        tickerId,
        (barDeepSec / 1000L).toInt,
        elm._2
      )
    )
  }


  def saveBars(seqBarsCalced: Seq[Bar]) = {
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
  }


  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.getDaysSinceEpoch)

  /**
    * Read and return all information about calced bars.
    * distinct ticker_id, bar_width_sec, ddate from mts_bars.bars
    *
    */
  def getAllBarsHistMeta: Seq[barsMeta] = {
    session.execute(bndBarsHistMeta).all().iterator.asScala.toSeq.map(r => rowToBarMeta(r))
      .filter(r => Seq(1).contains(r.tickerId) && Seq(30/*300,600,1800,3600*/).contains(r.barWidthSec)) //-------------------------------------------------------------- !!!!!!!!!!!!!!!!!!
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
    //logger.debug(" >>> inside makeAnalyze for p="+p+" and seqB.SIZE="+seqB.size)
    //val t1 = System.currentTimeMillis

    def fCheckCritMax(currBar: barsForFutAnalyze, srcElm: barsForFutAnalyze): Boolean ={
      val pUp :Double = (Math.exp( Math.log(currBar.c) + p)* 10000).round / 10000.toDouble
      pUp >= srcElm.minOHLC && pUp <= srcElm.maxOHLC
  }

  def fCheckCritMin(currBar: barsForFutAnalyze, srcElm: barsForFutAnalyze): Boolean ={
    val pDw :Double = (Math.exp( Math.log(currBar.c) - p)* 10000).round / 10000.toDouble
    pDw >= srcElm.minOHLC && pDw <= srcElm.maxOHLC
  }

    def fCheckCritBoth (currBar :barsForFutAnalyze,  srcElm:barsForFutAnalyze) :Boolean ={
      val pUp :Double = (Math.exp( Math.log(currBar.c) + p)* 10000).round / 10000.toDouble
      val pDw :Double = (Math.exp( Math.log(currBar.c) - p)* 10000).round / 10000.toDouble
      pUp <= srcElm.maxOHLC && pDw >= srcElm.minOHLC
    }


  val r = for (
    currBar <- seqB;
    /**
      Optimization: drop is faster
    searchSeq = seqB.filter(srcElm => srcElm.ts_end > currBar.ts_end)
     OR
    */
    idxDrop = seqB.indexOf(currBar);
    searchSeq = seqB.drop(idxDrop+1)
  ) yield {

    val fbMax :Long = searchSeq
      .find(srcElm => fCheckCritMax(currBar,srcElm))//.headOption
    match {
      case Some(foundedBar) => foundedBar.ts_end
      case None => 0L
    }

    val fbMin :Long =
      (fbMax match {
        case 0L =>
          searchSeq
            .find(srcElm => fCheckCritMin(currBar,srcElm))
        case x:Long =>
          searchSeq.filter(srcElm => (srcElm.ts_end > currBar.ts_end && srcElm.ts_end < x))
            .find(srcElm => fCheckCritMin(currBar,srcElm))
      })
      match {
        case Some(foundedBar) => foundedBar.ts_end
        case None => 0L
      }

    val fbBoth :Long =
      ((fbMin,fbMax) match {
        case (0L, 0L)             => searchSeq
          .find(srcElm => fCheckCritBoth(currBar,srcElm))
        case (mn :Long, 0L)       =>searchSeq
          .find(srcElm => fCheckCritBoth(currBar,srcElm))
        case (0L, mx :Long)       =>searchSeq
          .find(srcElm => fCheckCritBoth(currBar,srcElm))
        case (mn :Long, mx :Long) =>searchSeq
          .find(srcElm => fCheckCritBoth(currBar,srcElm))
      })
      match {
        case Some(foundedBar) => foundedBar.ts_end
        case None => 0L
    }

    val aFoundedAll :Seq[(String,Long)] = Seq(("mx",fbMax),("mn",fbMin),("bt",fbBoth)).filter(e => e._2!=0L)

    val aFoundedAllMin :Long = aFoundedAll.map(elm => elm._2).reduceOption(_ min _).getOrElse(0L)

    val aFounded :Option[(String,Long)] = aFoundedAll.find(e => e._2 == aFoundedAllMin)

    aFounded match {
      case Some(bar) =>
        new barsFutAnalyzeRes(
          currBar, searchSeq.find(b => b.ts_end == bar._2),
          p, bar._1
        )
      case None =>
        new barsFutAnalyzeRes(
          currBar, None, p , "nn"
        )
    }
  }
    //val t2 = System.currentTimeMillis
    //logger.debug(" <<< inside makeAnalyze  Result r.size="+r.size+" analyze duration = "+(t2 - t1) + " msecs.")
    r
  }




  /**
    * Save results of Future analyze into DB mts_bars.bars_fa
    */
  def saveBarsFutAnal(seqFA :Seq[barsResToSaveDB]) = {

    val partsSeqBarFa = seqFA.grouped(50)//other limit 65535 for tiny rows.

    for(thisPartOfSeq <- partsSeqBarFa) {
      //logger.debug("INSIDE [saveBarsFutAnal] SIZE OF thisPartOfSeq = "+thisPartOfSeq.size)
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
  }
  /** ------------------------------------------------------- */

  def getAllBarsFAMeta : Seq[barsFaMeta] ={
    session.execute(bndBarsFAMeta).all().iterator.asScala.toSeq.map(r => rowToBarFAMeta(r))
      //.filter(r =>  r.tickerId==7 /*&& r.barWidthSec==60*/) //-------------------------------------------------------------- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      .sortBy(sr => (sr.tickerId,sr.barWidthSec,sr.dDate))
    //read here ts_end for each pairs:sr.tickerId,sr.barWidthSec for running Iterations in loop.
  }

  /**
    * Read max ddate from mts_bars.bars_forms for each key ticker_id, bar_width_sec, formdeepkoef, prcnt
    * and return minimal one.
  */
  def getMinDdateBFroms(tickerId :Int, barWidthSec :Int, prcntsDiv : Seq[Double], formDeepKoef :Int, resType :String) :Option[(LocalDate,Long)] ={
   val seqDdates :Seq[Option[(LocalDate,Long)]] = prcntsDiv.map(
     pr => {
       logger.debug("INSIDE LOOP: tickerId="+tickerId+" barWidthSec="+barWidthSec+" formDeepKoef="+formDeepKoef+" log_oe = "+pr+" resType = ["+resType+"]")
       session.execute(bndBarsFormsMaxDdate
         .setInt("p_ticker_id", tickerId)
         .setInt("p_bar_width_sec", barWidthSec)
         .setInt("p_formdeepkoef", formDeepKoef)
         .setDouble("p_log_oe", pr)
         .setString("p_res_type",resType))
         //.one()
         .all().iterator.asScala.toSeq.map(row => (row.getDate("ddate"), row.getLong("ts_end"))).headOption
     }
   )

    logger.debug("(1) seqDdates = "+seqDdates.toString())

    val seqNNDdates :Seq[(LocalDate,Long)] = seqDdates.collect{ case Some(d) => d }
    logger.debug("(2) seqNNDdates = "+seqNNDdates.toString())

    if (seqNNDdates.isEmpty)
     None
    else
      Option(seqNNDdates.filter(elm => elm._2 == seqNNDdates.map(s => s._2).min ).head)
  }


  /**
    *
    * Read all data for each ddata by key: ticker_id and bws
    * OR
    * read just part begining from (LocalDate,Long) if something already calced in bars_forms.
    *
  */
  def getAllFaBars(seqB :Seq[barsFaMeta],minDdateTsFromBForms :Option[(LocalDate,Long)]) :Seq[barsResToSaveDB] = {

    val minDdateTs : (Option[LocalDate],Long) =  minDdateTsFromBForms match {
      case Some(minDdateTs) => (Option(minDdateTs._1),minDdateTs._2)
      case None => (Option(null),0L)
    }

    minDdateTs match {
      case (Some(minDdate),tsEndBegin : Long) =>
        seqB.filter(b => (b.dDate.getMillisSinceEpoch >= minDdate.getMillisSinceEpoch))
          .flatMap(sb => session.execute(bndBarsFaDataRestTsEnd
            .setInt("p_ticker_id", sb.tickerId)
            .setInt("p_bar_width_sec", sb.barWidthSec)
            .setDate("p_ddate", sb.dDate)
            .setLong("p_ts_end", minDdateTs._2))
            .all()
            .iterator.asScala.toSeq.map(r => rowToBarFAData(r, sb.tickerId, sb.barWidthSec, sb.dDate))
            .sortBy(sr => (sr.tickerId, sr.barWidthSec, sr.dDate, sr.ts_end)))
      case _ =>
        seqB.flatMap(sb => session.execute(bndBarsFaData
          .setInt("p_ticker_id", sb.tickerId)
          .setInt("p_bar_width_sec", sb.barWidthSec)
          .setDate("p_ddate", sb.dDate))
          .all()
          .iterator.asScala.toSeq.map(r => rowToBarFAData(r, sb.tickerId, sb.barWidthSec, sb.dDate))
          .sortBy(sr => (sr.tickerId, sr.barWidthSec, sr.dDate, sr.ts_end)))
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
  def getAllTicksForForms(tickerID :Int, tsMin :Long, tsMax :Long, ddateMin : LocalDate, ddateMax : LocalDate) :Seq[tinyTick] ={
   // ticker_id,ddate
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
          logger.debug("INTERNAL READ TICKS (" + (tickerID,ddate) + ") ROWS = "+seqTickOneDDate.size+" SIZE = "+ SizeEstimator.estimate(seqTickOneDDate)/1024L/1024L+" Mb.")
        seqTickOneDDate.sortBy(e => e.db_tsunx)
    }.flatten.filter(elm => elm.db_tsunx >= tsMin && elm.db_tsunx <= tsMax)
  }
  /** --------------------------------------------------------------------------------------- */

  /**
    * Save all calculated forms of bars into DB.
  */
  def saveForms(seqForms : Seq[bForm]) = {
    /*
    for (f <- seqForms.take(10)) {
      logger.debug("  FORM :  "+f.tickerId + " ts("+f.TsBegin+","+f.TsEnd+") ticksCnt="+f.FormProps.get("ticksCnt"))
    }
    */
    //logger.debug("INSIDE[1] [saveBarsFutAnal] SIZE OF seqForms = "+seqForms.size)
    val parts = seqForms.grouped(100)//other limit 65535 for tiny rows.

    for(thisPartOfSeq <- parts) {
      //logger.debug("INSIDE[2] [saveBarsFutAnal] SIZE OF thisPartOfSeq = "+thisPartOfSeq.size)
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
  /** --------------------------------------------------------------------------------------- */

}

object DBCass {
  def apply(nodeAddress: String, dbType :String) = {
    new DBCass(nodeAddress,dbType)
  }
}

