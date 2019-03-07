package bcpackage

import bcstruct.{Bar, CalcProperties, CalcProperty, seqTicksObj}
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * The Main class that contains all logic for bar calculation.
  *
  */
class BarCalculator(nodeAddress :String, dbType :String, readBySecs :Long) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def logCalcProp(cp :CalcProperty) ={
    logger.debug("Calc property: -------------------------------------------------------------------------------------")
    logger.debug(" TICKER_ID=" + cp.tickerId + " DEEPSEC=" + cp.barDeepSec + " IS_ENABLED=["+cp.isEnabled+"]" +
      "  LASTBAR_DDATE=[" + cp.dDateBeginLastBar + "] LASTBAR_TSEND=[" + cp.tsEndLastBar + "] LASTTICK_DDATE=" +
      cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick)
    logger.debug(" First tick TS = "+cp.tsFirstTicks)
    logger.debug(" Interval from last bar TS and last tick TS  =    "+cp.diffLastTickTSBarTS+"   sec." + " AVERAGE = "+
      Math.round(cp.diffLastTickTSBarTS/(60*60*24))+" days.")
  }

  def calcIteration(dbInst :DBImpl) ={
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties
    logger.debug(" Size of all bar calculator properties is "+allCalcProps.cProps.size)

    for(cp <- allCalcProps.cProps) {
      logCalcProp(cp)

      val currReadInterval :(Long,Long) = (cp.beginFrom,
        Seq(cp.beginFrom+readBySecs*1000L,
          cp.tsLastTick match {
            case Some(ts) => ts
            case _ => cp.beginFrom
          }).min)

      logger.debug(s" In this iteration will read interval (PLAN) $currReadInterval for deepSecs="+cp.barDeepSec)
      //val (seqTicks,readMsec) = dbInst.getTicksByInterval(cp, currReadInterval._1, currReadInterval._2)

      def readTicksRecurs(readFromTs :Long, readToTs :Long) :(seqTicksObj,Long) ={
        val (seqTicks,readMsec) = dbInst.getTicksByInterval(cp, readFromTs, readToTs)
        if (seqTicks.sqTicks.size==0)
        (seqTicks,readMsec)
        else if (
          cp.tsLastTick.getOrElse(0L) > readFromTs && cp.tsLastTick.getOrElse(0L) <  readToTs
        )
        (seqTicks,readMsec)
        else if (
          seqTicks.sqTicks.size < cp.barDeepSec && cp.tsLastTick.getOrElse(0L) > readToTs
        )
          readTicksRecurs(readFromTs, readToTs + (readToTs-readFromTs))
        else
          (seqTicks,readMsec)
      }

      val (seqTicks,readMsec) = readTicksRecurs(currReadInterval._1, currReadInterval._2) // dbInst.getTicksByInterval(cp, currReadInterval._1, currReadInterval._2)

      logger.debug("Duration of read ticks seq = "+ readMsec + " msecs. Read ["+seqTicks.sqTicks.size+"] ticks.")

      if (seqTicks.sqTicks.size != 0) {
        val bars :Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec*1000L)
        logger.debug(" bars.size="+bars.size)
        if (bars.size > 0)
          dbInst.saveBars(bars)
      }

      /*
      if (seqTicks.sqTicks.size==0 && cp.tsLastTick.getOrElse(0L) > currReadInterval._2) {
        dbInst.saveEmptyBar(cp.tickerId ,cp.barDeepSec, currReadInterval._1, currReadInterval._2)
      } else {
        /**
          * Which interval in seconds was read
          * */
        val factReadSeqTicksLnSecs = (seqTicks.sqTicks.last.db_tsunx/1000L - seqTicks.sqTicks.head.db_tsunx/1000L)
        logger.debug("Ticks data interval in seconds = "+factReadSeqTicksLnSecs+" seconds. Where LastTS = "+seqTicks.sqTicks.last.db_tsunx)
        if (cp.tsLastTick.getOrElse(0L) > seqTicks.sqTicks.last.db_tsunx &&
            factReadSeqTicksLnSecs < cp.barDeepSec &&
            cp.tsLastTick.getOrElse(0L) < currReadInterval._2) {
            logger.debug(" - tsLastTick inside planning read interval - DO NOTHING, WAIT NEW TICKS -")
        } else if (cp.tsLastTick.getOrElse(0L) > seqTicks.sqTicks.last.db_tsunx &&
            factReadSeqTicksLnSecs < cp.barDeepSec &&
            cp.tsLastTick.getOrElse(0L) > currReadInterval._2) {
          dbInst.saveEmptyBar(cp.tickerId ,cp.barDeepSec, currReadInterval._1, currReadInterval._2)
        } else if (cp.tsLastTick.getOrElse(0L) > seqTicks.sqTicks.last.db_tsunx &&
          factReadSeqTicksLnSecs > cp.barDeepSec &&
          cp.tsLastTick.getOrElse(0L) > currReadInterval._2){
            val bars :Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec*1000L)
            logger.debug(" bars.size="+bars.size)
          if (bars.size>0)
            dbInst.saveBars(bars)
          else
            dbInst.saveEmptyBar(cp.tickerId ,cp.barDeepSec, currReadInterval._1, currReadInterval._2)
        }
      }
      */



      logger.debug(" ")
      logger.debug("----------------------------------------------------------------------------------------------------")
    }
  }



  def run = {
    /**
      * dbSess hides DB query execution logic and converting data sets into seq of scala objects.
      * Lets us get necessary structures of data.
      */
    val dbInst :DBImpl = dbType match {case
                                        "cassandra" => new DBCass(nodeAddress,dbType)
                                       // "oracle"    => new DbOra(nodeAddress,dbType)
                                      }
    require(!dbInst.isClosed,s"Session to [$dbType] is closed.")
    logger.debug(s"Session to [$dbType] is opened. Continue.")

    def taskCalcBars(): Future[Unit] = Future {
      val t1 = System.currentTimeMillis
      calcIteration(dbInst)
      val t2 = System.currentTimeMillis
      logger.info("Duration of barCalc.calc() - "+(t2 - t1) + " msecs.")
      Thread.sleep(3000)
    }

    def loopCalcBars(): Future[Unit] = {
      taskCalcBars.flatMap(_ => loopCalcBars())
    }

    def infiniteLoop(): Future[Unit] = {
      Future.sequence(List(loopCalcBars())).map(_ => ())
    }

    Await.ready(infiniteLoop(), Duration.Inf)

  }

}
