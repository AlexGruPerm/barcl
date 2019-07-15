package bcpackage

import bcstruct._
import bcstruct.seqTicksWithReadDuration
import com.datastax.driver.core
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory
/*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
*/

/**
  * The Main class that contains all logic for bar calculation.
  *
  */
class BarCalculator(nodeAddress :String, dbType :String, readBySecs :Long) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  logger.info("readBySecs=" + readBySecs)

  def logCalcProp(cp: CalcProperty) = {
    logger.info("Calc property: -------------------------------------------------------------------------------------")
    logger.info(" TICKER_ID=" + cp.tickerId + " DEEPSEC=" + cp.barDeepSec + " IS_ENABLED=[" + cp.isEnabled + "]" + "] LASTBAR_TSEND=[" + cp.tsEndLastBar + "] LASTTICK_DDATE=" +
      cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick + " cp FIRSTTICK=" + cp.tsFirstTicks)

    logger.info(" First tick TS = " + cp.tsFirstTicks)
    logger.info(" Interval from last bar TS and last tick TS  =    " + cp.diffLastTickTSBarTS + "   sec." + " AVERAGE = " +
      Math.round(cp.diffLastTickTSBarTS / (60 * 60 * 24)) + " days.")
  }

  def intervalSecondsDouble(sqTicks: Seq[Tick]): Double = {
    //logger.info("sqTicks.last.db_tsunx="+sqTicks.last.db_tsunx)
    //logger.info("sqTicks.head.db_tsunx="+sqTicks.head.db_tsunx)
    (sqTicks.last.db_tsunx.toDouble - sqTicks.head.db_tsunx.toDouble) / 1000
  }

  def calcIteration(dbInst: DBImpl, fTicksMeta: Seq[FirstTickMeta]): Unit = {
    val allCalcProps: CalcProperties = dbInst.getAllCalcProperties(fTicksMeta)
    logger.info(" Size of all bar calculator properties is " + allCalcProps.cProps.size)

    /**
      * & <-- verifies both operands
      * && <-- stops evaluating if the first operand evaluates to false since the result will be false
      */
    //todo: Make type definition and return this type instead of (seqTicksObj,Long)
    def readTicksRecurs(readFromTs: Long, readToTs: Long, cp: CalcProperty): seqTicksWithReadDuration = {
      val (seqTicks, readMsec) = dbInst.getTicksByInterval(cp, readFromTs, readToTs)
      //read from Friday last ticks and there is nth. to read and we have data in next days.
      if (seqTicks.sqTicks.isEmpty && cp.tsLastTick.getOrElse(0L) > readToTs) {
        readTicksRecurs(readFromTs, readToTs + readBySecs * 1000L, cp)
      }
      else if (seqTicks.sqTicks.isEmpty && cp.tsLastTick.getOrElse(0L) <= readToTs) {
        (seqTicks, readMsec)
      }
      else if (cp.tsLastTick.getOrElse(0L) > readFromTs && cp.tsLastTick.getOrElse(0L) < readToTs) {
        (seqTicks, readMsec)
      }
      else if (seqTicks.sqTicks.nonEmpty && intervalSecondsDouble(seqTicks.sqTicks) < cp.barDeepSec.toDouble &&
        cp.tsLastTick.getOrElse(0L) > readToTs) {
        readTicksRecurs(readFromTs, readToTs + readBySecs * 1000L, cp)
      }
      else {
        (seqTicks, readMsec)
      }
    }


    allCalcProps.cProps.foreach { cp =>
      logCalcProp(cp)

      val currReadInterval: (Long, Long) = (cp.beginFrom,
        Seq(cp.beginFrom + readBySecs * 1000L, cp.tsLastTick.getOrElse(cp.beginFrom)).min)

      //todo: rewrite on function with one line output
      logger.info(" In this iteration will read interval (PLAN) FROM: " + currReadInterval._1 + " (" + core.LocalDate.fromMillisSinceEpoch(currReadInterval._1) + ")")
      logger.info("                                      (PLAN)   TO: " + currReadInterval._2 + " (" + core.LocalDate.fromMillisSinceEpoch(currReadInterval._2) + ")")

      val (seqTicks, readMsec): seqTicksWithReadDuration =
        try {
          readTicksRecurs(currReadInterval._1, currReadInterval._2, cp)
        } catch {
          case ex: com.datastax.driver.core.exceptions.OperationTimedOutException => logger.error("-1- ex when call readTicksRecurs [" + ex.getMessage + "] [" + ex.getCause + "]")
            (seqTicksObj(Nil), 0L)
          //todo: Fix it, commented 10.07.2019
          // case e :Throwable => logger.error("-2- EXCEPTION when call readTicksRecurs ["+e.getMessage+"] ["+e.getCause+"]")
          //   (seqTicksObj(Nil),0L)
        }

      logger.info("readed recursively ticks SIZE=" + seqTicks.sqTicks.size)
      //todo: remove it !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      //seqTicks.sqTicks.foreach(t => logger.info(t.toString))


      if (seqTicks.sqTicks.nonEmpty) {
        val bars: Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec * 1000L)
        if (bars.nonEmpty) {
          //logger.info("bars Non Empty save it. bars.size="+bars.size)
          //logger.info("begin: " + bars.head.ts_begin)
          //logger.info("end:   " + bars.head.ts_end)
          dbInst.saveBars(bars)
        }
        else logger.info("bars is Empty no save")
      }

    }
  }


  def run = {
    /**
      * dbSess hides DB query execution logic and converting data sets into seq of scala objects.
      * Lets us get necessary structures of data.
      */
    val dbInst: DBImpl = dbType match {
      case
        "cassandra" => new DBCass(nodeAddress, dbType)
      // "oracle"    => new DbOra(nodeAddress,dbType)
    }
    require(!dbInst.isClosed, s"Session to [$dbType] is closed.")
    logger.info(s"Session to [$dbType] is opened. Continue.")

    val fTicksMeta: Seq[FirstTickMeta] = dbInst.getFirstTicksMeta

    logger.info("fTicksMeta=[" + fTicksMeta + "]")

    while (true) {
      val t1 = System.currentTimeMillis
      calcIteration(dbInst, fTicksMeta)
      val t2 = System.currentTimeMillis
      logger.info("Duration of ITERATION barCalc.run() - " + (t2 - t1) + " msecs.")
    }

  }

}
