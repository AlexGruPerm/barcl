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
import java.util.concurrent.Executors
import bcstruct._
import bcstruct.seqTicksWithReadDuration
import com.datastax.driver.core
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, _}
*/

/**
  * The Main class that contains all logic for bar calculation.
  *
  */
class BarCalculatorTickersBws(nodeAddress :String, dbType :String, readBySecs :Long) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def logCalcProp(cp :CalcProperty) :Unit = {
      logger.info("Calc property: -------------------------------------------------------------------------------")
      logger.info("[" + cp.tickerId + "][" + cp.barDeepSec + "] LASTBAR_TSEND=[" + cp.tsEndLastBar +
        "] LASTTICK_DDATE=" + cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick + " cp FIRSTTICK=" + cp.tsFirstTicks)
      //logger.info(" First tick TS = " + cp.tsFirstTicks)
      logger.info(" Interval from last bar TS and last tick TS  =    " + cp.diffLastTickTSBarTS + "   sec." +
        " AVERAGE = " + Math.round(cp.diffLastTickTSBarTS / (60 * 60 * 24)) + " days.")
  }

  def logReadInterval(currReadInterval: (Long, Long)) :Unit ={
    logger.info(" In this iteration will read interval (PLAN) FROM: " + currReadInterval._1 +
      " (" + core.LocalDate.fromMillisSinceEpoch(currReadInterval._1) + ")")
    logger.info("                                      (PLAN)   TO: " + currReadInterval._2 +
      " (" + core.LocalDate.fromMillisSinceEpoch(currReadInterval._2) + ")")
  }

  def intervalSecondsDouble(sqTicks :Seq[Tick]) :Double =
    (sqTicks.last.db_tsunx.toDouble - sqTicks.head.db_tsunx.toDouble) / 1000


  /** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
  def readTicksRecurs(dbInst :DBImpl, readFromTs: Long, readToTs: Long, cp: CalcProperty): seqTicksWithReadDuration = {

    val (seqTicks, readMsec) = dbInst.getTicksByIntervalShort(cp.tickerId, readFromTs, readToTs, cp.barDeepSec)
    /*
      if (cp.barDeepSec <= 600 &&
                                   cp.diffLastTickTSBarTS <= cp.barDeepSec
    ) {
      logger.info("getTicksByIntervalShort for "+cp.tickerId+"-"+cp.barDeepSec)
      dbInst.getTicksByIntervalShort(cp.tickerId, readFromTs, readToTs, cp.barDeepSec)
    } else {
      logger.info("getTicksByIntervalLong for "+cp.tickerId+"-"+cp.barDeepSec)
      dbInst.getTicksByIntervalLong(cp.tickerId, readFromTs, readToTs, cp.barDeepSec)
    }
    */

    if (seqTicks.sqTicks.isEmpty && cp.tsLastTick.getOrElse(0L) > readToTs)
      readTicksRecurs(dbInst, readFromTs, readToTs + readBySecs * 1000L, cp)

    else if (seqTicks.sqTicks.isEmpty && cp.tsLastTick.getOrElse(0L) <= readToTs)
      (seqTicks, readMsec)

    else if (cp.tsLastTick.getOrElse(0L) > readFromTs && cp.tsLastTick.getOrElse(0L) < readToTs)
      (seqTicks, readMsec)

    else if (seqTicks.sqTicks.nonEmpty && intervalSecondsDouble(seqTicks.sqTicks) < cp.barDeepSec.toDouble &&
      cp.tsLastTick.getOrElse(0L) > readToTs)
      readTicksRecurs(dbInst, readFromTs, readToTs + readBySecs * 1000L, cp)

    else
      (seqTicks, readMsec)

  }
  /** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */


  def calcIteration(dbInst :DBImpl,fTicksMeta :FirstTickMeta, bws :Int) :Int = {
    logger.info("calcIteration ["+ fTicksMeta.tickerId + "][" + bws + "]")
    //dbInst.checkConnectCount(fTicksMeta.tickerId,bws)//todo: maybe remove at all.
    val allCalcProps: CalcProperties = dbInst.getAllCalcProperties(fTicksMeta, bws)

    val cp = allCalcProps.cProps.head
    logCalcProp(cp)
    val currReadInterval: (Long, Long) = (cp.beginFrom,
      Seq(cp.beginFrom + readBySecs * 1000L, cp.tsLastTick.getOrElse(cp.beginFrom)).min)
    logReadInterval(currReadInterval)

      val (seqTicks,readMs) :seqTicksWithReadDuration =
        try {
          readTicksRecurs(dbInst, currReadInterval._1, currReadInterval._2,cp)
        } catch {
          case e :com.datastax.driver.core.exceptions.OperationTimedOutException  =>
            logger.error("EXCEPTION: (1) OperationTimedOutException (readTicksRecurs) ["+e.getMessage+"] ["+e.getCause+"]")
            (seqTicksObj(Nil),0L)
          case e :Throwable =>
            logger.error("EXCEPTION: (2) Throwable (readTicksRecurs) ["+e.getMessage+"] ["+e.getCause+"] FOR ["+cp.tickerId+"]["+cp.barDeepSec+"]")
            (seqTicksObj(Nil),0L)
        }

    logger.info("readTicksRecurs ["+ fTicksMeta.tickerId + "][" + bws + "] size ["+seqTicks.sqTicks.size+"]"+
      " width ["+(seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L+"] sec. durat :"+readMs+" ms. "
    )


    /**
      * seqTicks.sqTicks - ticks read by last readTicksRecurs,
      * it's can be interval from previous time or current online date.
      * Also it can be ticks interval seconds not enough to calculate at least one bar OR
      * interval to calculate N bars plus additional small part - in this case we will sleep.
      *
    */
      //todo: compare conditions and make refactoring of branches 1 and 2.
    val sleepAfterThisIteration :Int =
      if (seqTicks.sqTicks.nonEmpty) {
        val bars: Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec * 1000L)
        logger.info("["+ fTicksMeta.tickerId + "][" + bws + "] calculated bars.size =" +bars.size)
        /**
          * At least one bar calculated and ready to save.
        */
        if (bars.nonEmpty) {
          dbInst.saveBars(bars)
          /**
            * Sleep or not
          */
          //todo: branch #1
          if (
              bars.last.ddateFromTick == cp.dDateLastTick.getOrElse(bars.last.ddateFromTick) &&
              (cp.tsLastTick.getOrElse(0L) > 0L) &&
              (cp.tsLastTick.getOrElse(0L) - bars.last.ts_end)/1000L < cp.barDeepSec
          ) {
            val sleepInterval :Int = (((bars.last.ts_end + cp.barDeepSec*1000L) - cp.tsLastTick.getOrElse(0L))/1000L).toInt
            Seq(sleepInterval,1000).max
          } else
           3000
        } else {
          logger.info("Bars.IsEmpty")
          /**
            * We read ticks (sqTicks.nonEmpty) but it's not enough to calculate at least one bar
            */
          //todo: branch #2
          if ((seqTicks.sqTicks.last.db_tsunx - seqTicks.sqTicks.head.db_tsunx)/1000L < cp.barDeepSec &&
                cp.dDateLastTick.getOrElse(0L) != 0L &&
                cp.dDateLastTick.getOrElse(0L) == seqTicks.sqTicks.last.dDate //total last tick ddate = this Rea last ticks ddate
          ){
            val sleepInterval :Int = cp.barDeepSec*1000 - (seqTicks.sqTicks.last.db_tsunx - cp.tsEndLastBar.getOrElse(0L)).toInt
            Seq(sleepInterval,3000).max
          } else
            3000
        }
      } else
        3000
    sleepAfterThisIteration
  }





  def run = {
    val dbInst: DBImpl = new DBCass(nodeAddress, dbType)
    require(!dbInst.isClosed, s"Session to [$dbType] is closed.")
    val barsProperties :Seq[BarCalcProperty] = dbInst.getAllBarsProperties
    val fTicksMeta :Seq[FirstTickMeta] = dbInst.getFirstTicksMeta
      .filter(ftm => barsProperties.map(_.tickerId).contains(ftm.tickerId))
    val countOfThread :Int = Seq((fTicksMeta.size * barsProperties.size + 3),1).max
    //(Executors.newFixedThreadPool(countOfThread))
    implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

    def taskCalcBars(tm :FirstTickMeta, bws :Int): Future[Int/*Unit*/] =
      Future {
        blocking {
          val t1 = System.currentTimeMillis
          val sleepAfterThisIterationMs: Int = calcIteration(dbInst, tm, bws)
          val t2 = System.currentTimeMillis
            logger.info("Duration of taskCalcBars.run ["+tm.tickerId+"]["+bws+"] = " + (t2 - t1) + " ms. "+
            "sleep = "+ sleepAfterThisIterationMs +" ms.")
          sleepAfterThisIterationMs
          /*
          if (sleepAfterThisIterationMs != 0)
            Thread.sleep(sleepAfterThisIterationMs)
          */
        }}(ec)

    def loopCalcBars(tm :FirstTickMeta,bws :Int): Future[Unit] = //taskCalcBars(tm,bws).flatMap(_ => loopCalcBars(tm,bws))
      taskCalcBars(tm,bws).flatMap{sleepInterval =>
        Thread.sleep(sleepInterval)
        loopCalcBars(tm,bws)
      }

    def infiniteLoop(): Seq[Future[Unit]] = {
      val eachTickerBws :Seq[(FirstTickMeta,Int)] =
        fTicksMeta.flatMap { tm =>
          barsProperties
            .filter(bp => bp.tickerId == tm.tickerId)
            .map(bp => (tm,bp.bws))
        }

      scala.util.Random.shuffle(eachTickerBws).map {
        Thread.sleep(1000)
        thisElm => Future.sequence(List(loopCalcBars(thisElm._1, thisElm._2))).map(_ => ())
      }
    }

    Await.ready(Future.sequence(infiniteLoop), Duration.Inf)
  }

}
