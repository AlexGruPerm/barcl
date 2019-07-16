package bcpackage

import java.util.concurrent.Executors

import bcstruct._
import bcstruct.seqTicksWithReadDuration
import com.datastax.driver.core
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, _}
/*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
*/

/**
  * The Main class that contains all logic for bar calculation.
  *
  */
class BarCalculatorTickersBws(nodeAddress :String, dbType :String, readBySecs :Long) {
  val logger = LoggerFactory.getLogger(getClass.getName)
  logger.info("readBySecs="+readBySecs)

  def logCalcProp(cp :CalcProperty) ={
    logger.info("Calc property: -------------------------------------------------------------------------------------")
    logger.info(" TICKER_ID=" + cp.tickerId + " DEEPSEC=" + cp.barDeepSec + " IS_ENABLED=["+cp.isEnabled+"]" + "] LASTBAR_TSEND=[" + cp.tsEndLastBar + "] LASTTICK_DDATE=" +
      cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick+ " cp FIRSTTICK="+cp.tsFirstTicks)

    logger.info(" First tick TS = "+cp.tsFirstTicks)
    logger.info(" Interval from last bar TS and last tick TS  =    "+cp.diffLastTickTSBarTS+"   sec." + " AVERAGE = "+
      Math.round(cp.diffLastTickTSBarTS/(60*60*24))+" days.")
  }

  def intervalSecondsDouble(sqTicks :Seq[Tick]) :Double =
    (sqTicks.last.db_tsunx.toDouble - sqTicks.head.db_tsunx.toDouble) / 1000

  def calcIteration(dbInst :DBImpl,fTicksMeta :FirstTickMeta, bws :Int) :Int = {
    logger.info("ITERATION FOR tickerId="+fTicksMeta.tickerId+" and BWS=["+bws+"]")
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties(fTicksMeta,bws)


    //todo: Make type definition and return this type instead of (seqTicksObj,Long)
    def readTicksRecurs(readFromTs :Long, readToTs :Long, cp :CalcProperty) :seqTicksWithReadDuration = {
      val (seqTicks, readMsec) = dbInst.getTicksByInterval(cp, readFromTs, readToTs)
      //read from Friday last ticks and there is nth. to read and we have data in next days.
      if (seqTicks.sqTicks.isEmpty && cp.tsLastTick.getOrElse(0L) > readToTs){
        readTicksRecurs(readFromTs, readToTs + readBySecs * 1000L, cp)
      }
      else if (seqTicks.sqTicks.isEmpty && cp.tsLastTick.getOrElse(0L) <= readToTs){
        (seqTicks, readMsec)
      }
      else if (cp.tsLastTick.getOrElse(0L) > readFromTs && cp.tsLastTick.getOrElse(0L) < readToTs) {
        (seqTicks, readMsec)
      }
      else if (seqTicks.sqTicks.nonEmpty && intervalSecondsDouble(seqTicks.sqTicks) < cp.barDeepSec.toDouble &&
        cp.tsLastTick.getOrElse(0L) > readToTs) {
        readTicksRecurs(readFromTs, readToTs + readBySecs*1000L, cp)
      }
      else {
        (seqTicks, readMsec)
      }
    }

    //allCalcProps.cProps.foreach{cp =>

      val cp = allCalcProps.cProps.head
      logCalcProp(cp)

      val currReadInterval :(Long,Long) = (cp.beginFrom,
        Seq(cp.beginFrom+readBySecs*1000L, cp.tsLastTick.getOrElse(cp.beginFrom)).min)

      //todo: rewrite on function with one line output
      logger.info(" In this iteration will read interval (PLAN) FROM: "+ currReadInterval._1+
        " ("+ core.LocalDate.fromMillisSinceEpoch(currReadInterval._1) +")")
      logger.info("                                      (PLAN)   TO: "+ currReadInterval._2+
        " ("+ core.LocalDate.fromMillisSinceEpoch(currReadInterval._2) +")")

      val (seqTicks,readMsec) :seqTicksWithReadDuration =
        try {
          readTicksRecurs(currReadInterval._1, currReadInterval._2,cp)
        } catch {
          case ex :com.datastax.driver.core.exceptions.OperationTimedOutException  =>
            logger.error("-1- ex when call readTicksRecurs ["+ex.getMessage+"] ["+ex.getCause+"]")
            (seqTicksObj(Nil),0L)
          case e :Throwable =>
            logger.error("-2- EXCEPTION when call readTicksRecurs ["+e.getMessage+"] ["+e.getCause+"]")
            (seqTicksObj(Nil),0L)
        }

      logger.info("readed recursively ticks SIZE="+seqTicks.sqTicks.size+" ticks WIDTH sec.="+
        (seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L)

    /*
      val sleepAfterThisIteration :Int =
        if (seqTicks.sqTicks.nonEmpty /*&& (seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L > cp.barDeepSec*/ ) {
          val bars: Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec * 1000L)

          logger.info("LAST ["+cp.tickerId+"] (FROM CALCULATED THIS ITERATION) BAR TS_END="+bars.last.ts_end+
            " SUMMARY LAST TICK IN SRC ="+cp.tsLastTick.getOrElse(0L)+ " DIFF = "+
            (cp.tsLastTick.getOrElse(0L)-bars.last.ts_end)/1000L
          )
          logger.info("!!!!  Seq ticks interval = "+(seqTicks.sqTicks.last.db_tsunx -
            seqTicks.sqTicks.head.db_tsunx)/1000L+" seconds.")
          logger.info("!!!!  cp.barDeepSec = "+cp.barDeepSec+" seconds.")

          if (bars.nonEmpty &&
              ((seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L) >= cp.barDeepSec) {
            dbInst.saveBars(bars)
            logger.info("Saved ["+bars.size+"] bars.")

            if (bars.last.ddateFromTick == seqTicks.sqTicks.last.dDate &&
               (cp.tsLastTick.getOrElse(0L).toInt - bars.last.ts_end.toInt)/1000.toFloat < cp.barDeepSec
               ) {
               ((cp.barDeepSec-(cp.tsLastTick.getOrElse(0L) - bars.last.ts_end)/1000L)*1000L).toInt
             } else {
               0
             }
          }
          else {
            logger.info("bars is Empty no save")
            0
          }
        }
        else if (seqTicks.sqTicks.nonEmpty && (seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L < cp.barDeepSec) {
          logger.info("Sleep not enougth ticks. INTERVAL="+(cp.barDeepSec - (seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L).toInt)
          (cp.barDeepSec - (seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L).toInt*1000 //because sec. ms.
        }
        else 0 //seqTicks.sqTicks IS EMPTY, not sleeping at all.
    */

    /**
      * seqTicks.sqTicks - ticks read by last readTicksRecurs,
      * it's can be interval from previous time or current online date.
      * Also it can be ticks interval seconds not enough to calculate at least one bar OR
      * interval to calculate N bars plus additional small part - in this case we will sleep.
      *
    */
    val sleepAfterThisIteration :Int =
      if (seqTicks.sqTicks.nonEmpty) {
        val bars: Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec * 1000L)
        logger.info(" DEBUG_1 bars.size="+bars.size)
        /**
          * At least one bar calculated and ready to save.
        */
        if (bars.nonEmpty) {
          logger.info(" DEBUG_2 bars.nonEmpty")
          logger.info("[" + bars.size + "] bars send into dbInst.saveBars(bars) ")
          dbInst.saveBars(bars)
          /**
            * Sleep or not
          */
          if (
              bars.last.ddateFromTick == cp.dDateLastTick.getOrElse(bars.last.ddateFromTick) &&
              (cp.tsLastTick.getOrElse(0L) > 0L) &&
              (cp.tsLastTick.getOrElse(0L) - bars.last.ts_end)/1000L < cp.barDeepSec
          ) {
            val sleepInterval :Int = (bars.last.ts_end + cp.barDeepSec*1000L).toInt -
                                     (cp.tsLastTick.getOrElse(0L) - bars.last.ts_end).toInt
            logger.info("WILL SLEEP sleepInterval = ["+sleepInterval+"] ms.")
            Seq(sleepInterval,0).max
          } else {
           0
          }
        } else {
          logger.info(" DEBUG_2 bars.isEmpty")
          /**
            * We read ticks (sqTicks.nonEmpty) but it's not enough to calculate at least one bar
            */
          if (
              (seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L < cp.barDeepSec &&
                cp.dDateLastTick.getOrElse(0L) != 0L &&
                cp.dDateLastTick.getOrElse(0L) == seqTicks.sqTicks.last.dDate //total last tick ddate = this Rea last ticks ddate
          ){
            val sleepInterval :Int = cp.barDeepSec*1000 - (seqTicks.sqTicks.last.db_tsunx - cp.tsEndLastBar.getOrElse(0L)).toInt
              /*
              (bars.last.ts_end + cp.barDeepSec*1000L).toInt -
              (cp.tsLastTick.getOrElse(0L) - bars.last.ts_end).toInt
            */
            logger.info(" DEBUG_3 WILL SLEEP sleepInterval = ["+sleepInterval+"] ms.")
            Seq(sleepInterval,0).max

          } else {
            0
          }
        }
      } else {
        0
        /*
        if (1==1) {
          333
        } else {
          444
        }
        */
      }

    logger.info("RETURN sleepAfterThisIteration="+sleepAfterThisIteration)
    sleepAfterThisIteration
  }






  def run = {
    val dbInst: DBImpl = new DBCass(nodeAddress, dbType)
    require(!dbInst.isClosed, s"Session to [$dbType] is closed.")
    val fTicksMeta :Seq[FirstTickMeta] = dbInst.getFirstTicksMeta//.filter(t => Seq(1).contains(t.tickerId))
    val parsProperties :Seq[BarCalcProperty] = dbInst.getAllBarsProperties

    val countOfThread :Int = Seq((fTicksMeta.size * parsProperties.size + 3),1).max
    logger.info("countOfThread="+countOfThread)

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(countOfThread))

    def taskCalcBars(tm :FirstTickMeta, bws :Int): Future[Unit] =
      Future {
        blocking {
          logger.info("BEGIN taskCalcBars FOR [" + tm.tickerId + "]")
          val t1 = System.currentTimeMillis
          val sleepAfterThisIterationMs: Int = calcIteration(dbInst, tm, bws)
          val t2 = System.currentTimeMillis
          logger.info("Duration of taskCalcBars.run() - " + (t2 - t1) + " msecs. FOR=" + tm.tickerId + " bws=" + bws)
          logger.info("This THREAD will sleep [" + (sleepAfterThisIterationMs / 1000L) + "] seconds.")
          if (sleepAfterThisIterationMs != 0)
            Thread.sleep(sleepAfterThisIterationMs)
        }
      }(ec)

    def loopCalcBars(tm :FirstTickMeta,bws :Int): Future[Unit] = taskCalcBars(tm,bws).flatMap(_ => loopCalcBars(tm,bws))

    def infiniteLoop(): Seq[Future[Unit]] = {
      fTicksMeta.flatMap {
        tm => logger.info(">>> " + tm)
          parsProperties.filter(bp => bp.tickerId == tm.tickerId).sortBy(bp => bp.bws)(Ordering[Int]).reverse
            .map{
              bp => logger.info("   >>> "+bp)
              Future.sequence(List(loopCalcBars(tm, bp.bws))).map(_ => ())
            }
      }
    }

    Await.ready(Future.sequence(infiniteLoop), Duration.Inf)
  }

}
