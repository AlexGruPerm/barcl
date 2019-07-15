package bcpackage

import bcstruct._
import bcstruct.seqTicksWithReadDuration

import com.datastax.driver.core
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory


import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
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
    // From 15.07.2019 return only one CalcProperty for input fTicksMeta.ticker_id and BWS
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties(fTicksMeta,bws)
    logger.info(" !!! all the time must be EQUAL = 1  Size of all bar calculator properties is "+allCalcProps.cProps.size)

    /**
      * & <-- verifies both operands
      * && <-- stops evaluating if the first operand evaluates to false since the result will be false
    */
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
      logger.info(" In this iteration will read interval (PLAN) FROM: "+ currReadInterval._1+" ("+ core.LocalDate.fromMillisSinceEpoch(currReadInterval._1) +")")
      logger.info("                                      (PLAN)   TO: "+ currReadInterval._2+" ("+ core.LocalDate.fromMillisSinceEpoch(currReadInterval._2) +")")

      val (seqTicks,readMsec) :seqTicksWithReadDuration =
        try {
          readTicksRecurs(currReadInterval._1, currReadInterval._2,cp)
        } catch {
          case ex :com.datastax.driver.core.exceptions.OperationTimedOutException  => logger.error("-1- ex when call readTicksRecurs ["+ex.getMessage+"] ["+ex.getCause+"]")
            (seqTicksObj(Nil),0L)
         //todo: Fix it, commented 10.07.2019
          // case e :Throwable => logger.error("-2- EXCEPTION when call readTicksRecurs ["+e.getMessage+"] ["+e.getCause+"]")
         //   (seqTicksObj(Nil),0L)
        }

      logger.info("readed recursively ticks SIZE="+seqTicks.sqTicks.size)
      //todo: remove it !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      //seqTicks.sqTicks.foreach(t => logger.info(t.toString))

      val sleepAfterThisIteration :Int =
        if (seqTicks.sqTicks.nonEmpty) {
         //don't do attampts of bar calculation if it's not enough source data.

          val bars: Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec * 1000L)

          logger.info("seqTicks.sqTicks.head  ddate = " +seqTicks.sqTicks.head.dDate+ " db_tsunx = "+ seqTicks.sqTicks.head.db_tsunx)
          logger.info("seqTicks.sqTicks.last  ddate = " +seqTicks.sqTicks.last.dDate+ " db_tsunx = "+ seqTicks.sqTicks.last.db_tsunx)
          logger.info("head - last interval in seconds  = " +(seqTicks.sqTicks.head.db_tsunx - seqTicks.sqTicks.last.db_tsunx)/1000L )
          logger.info("bars.head.ts_end = " + bars.head.ts_end)
          logger.info("bars.last.ts_end = " + bars.last.ts_end)
          logger.info("bars.last.ddateFromTick = " + bars.last.ddateFromTick)

          if (bars.nonEmpty) {
            dbInst.saveBars(bars)
            0
            /*
            if (lastbar.ddate = last_tick.ddate) {
               cp.barDeepSec * 1000L
             } else {
               0
             }
            */
          }
          else {
            logger.info("bars is Empty no save")
            0
          }

        }
        else 0 //seqTicks.sqTicks IS EMPTY, not sleeping at all.

    sleepAfterThisIteration
  }



  def run = {
    val dbInst: DBImpl = new DBCass(nodeAddress, dbType)
    require(!dbInst.isClosed, s"Session to [$dbType] is closed.")
    val fTicksMeta :Seq[FirstTickMeta] = dbInst.getFirstTicksMeta
    val parsProperties :Seq[BarCalcProperty] = dbInst.getAllBarsProperties


    def taskCalcBars(tm :FirstTickMeta, bws :Int): Future[Unit] = Future {
      val t1 = System.currentTimeMillis
      val sleepAfterThisIterationMs :Int = calcIteration(dbInst, tm, bws)
      val t2 = System.currentTimeMillis
      logger.info("Duration of taskCalcBars.run() - " + (t2 - t1) + " msecs. FOR="+tm.tickerId+" bws="+bws)
      logger.info("Now will sleep ["+(sleepAfterThisIterationMs/1000L)+"] seconds.")
      if (sleepAfterThisIterationMs!=0)
       Thread.sleep(sleepAfterThisIterationMs)
  }

    def loopCalcBars(tm :FirstTickMeta,bws :Int): Future[Unit] = taskCalcBars(tm,bws).flatMap(_ => loopCalcBars(tm,bws))

    def infiniteLoop(): Seq[Future[Unit]] =
      fTicksMeta
        .flatMap(tm => parsProperties
          .filter(bp => bp.tickerId == tm.tickerId)
          .sortBy(bp => bp.bws)
          .map(bp => Future.sequence(List(loopCalcBars(tm,bp.bws))).map(_ => ()))
      )

    Await.ready(Future.sequence(infiniteLoop), Duration.Inf)
  }


}
