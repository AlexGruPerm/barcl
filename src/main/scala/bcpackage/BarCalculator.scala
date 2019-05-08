package bcpackage

import bcstruct.{Bar, CalcProperties, CalcProperty, seqTicksObj}
import com.datastax.driver.core
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
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

  def logCalcProp(cp :CalcProperty) ={
    logger.info("Calc property: -------------------------------------------------------------------------------------")
    logger.info(" TICKER_ID=" + cp.tickerId + " DEEPSEC=" + cp.barDeepSec + " IS_ENABLED=["+cp.isEnabled+"]" +
      /*"  LASTBAR_DDATE=[" + cp.dDateBeginLastBar +*/ "] LASTBAR_TSEND=[" + cp.tsEndLastBar + "] LASTTICK_DDATE=" +
      cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick+ " cp FIRSTTICK="+cp.tsFirstTicks)

    logger.info(" First tick TS = "+cp.tsFirstTicks)
    logger.info(" Interval from last bar TS and last tick TS  =    "+cp.diffLastTickTSBarTS+"   sec." + " AVERAGE = "+
      Math.round(cp.diffLastTickTSBarTS/(60*60*24))+" days.")
  }

  def calcIteration(dbInst :DBImpl) ={
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties
    logger.info(" Size of all bar calculator properties is "+allCalcProps.cProps.size)

    for(cp <- allCalcProps.cProps/*.filter(c => c.barDeepSec==30)*/) {
      logCalcProp(cp)

      val currReadInterval :(Long,Long) = (cp.beginFrom,
        Seq(cp.beginFrom+readBySecs*1000L,
          cp.tsLastTick match {
            case Some(ts) => ts
            case _ => cp.beginFrom
          }).min)

      logger.info(" In this iteration will read interval (PLAN) FROM: "+ currReadInterval._1+" ("+ core.LocalDate.fromMillisSinceEpoch(currReadInterval._1) +")")
      logger.info("                                      (PLAN)   TO: "+ currReadInterval._2+" ("+ core.LocalDate.fromMillisSinceEpoch(currReadInterval._2) +")")

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

      val (seqTicks,readMsec) = readTicksRecurs(currReadInterval._1, currReadInterval._2) 

      logger.info("Duration of read ticks seq = "+ readMsec + " msecs. Read ["+seqTicks.sqTicks.size+"] ticks.")

      if (seqTicks.sqTicks.size != 0) {
        val bars :Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec*1000L)
        logger.info(" bars.size="+bars.size)
        if (bars.size > 0)
          dbInst.saveBars(bars)
      }

      logger.info(" ")
      logger.info("----------------------------------------------------------------------------------------------------")
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
    logger.info(s"Session to [$dbType] is opened. Continue.")

    def taskCalcBars(): Future[Unit] = Future {
      val t1 = System.currentTimeMillis
      calcIteration(dbInst)
      val t2 = System.currentTimeMillis
      logger.info("Duration of barCalc.run() - "+(t2 - t1) + " msecs.")
      Thread.sleep(1500)
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
