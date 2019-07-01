package bcpackage

import bcstruct.bcstruct.seqTicksWithReadDuration
import bcstruct._
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

  def logCalcProp(cp :CalcProperty) ={
    logger.info("Calc property: -------------------------------------------------------------------------------------")
    logger.info(" TICKER_ID=" + cp.tickerId + " DEEPSEC=" + cp.barDeepSec + " IS_ENABLED=["+cp.isEnabled+"]" + "] LASTBAR_TSEND=[" + cp.tsEndLastBar + "] LASTTICK_DDATE=" +
      cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick+ " cp FIRSTTICK="+cp.tsFirstTicks)

    logger.info(" First tick TS = "+cp.tsFirstTicks)
    logger.info(" Interval from last bar TS and last tick TS  =    "+cp.diffLastTickTSBarTS+"   sec." + " AVERAGE = "+
      Math.round(cp.diffLastTickTSBarTS/(60*60*24))+" days.")
  }

  def calcIteration(dbInst :DBImpl,fTicksMeta :Seq[FirstTickMeta]) :Unit = {
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties(fTicksMeta)
    logger.info(" Size of all bar calculator properties is "+allCalcProps.cProps.size)

    /**
      * & <-- verifies both operands
      * && <-- stops evaluating if the first operand evaluates to false since the result will be false
    */
    //todo: Make type definition and return this type instead of (seqTicksObj,Long)
    def readTicksRecurs(readFromTs :Long, readToTs :Long, cp :CalcProperty) :seqTicksWithReadDuration = {
      val (seqTicks, readMsec) = dbInst.getTicksByInterval(cp, readFromTs, readToTs)
  /*
      logger.info(" > Inside readTicksRecurs readFromTs=" + readFromTs + " readToTs=" + readToTs +
        " seqTicks.size=" + seqTicks.sqTicks.size+" cp.tsLastTick.getOrElse(0L)="+cp.tsLastTick.getOrElse(0L)+
      " cp.barDeepSec="+cp.barDeepSec)
      logger.info("   > head-last sec. ="+(seqTicks.sqTicks.last.db_tsunx/1000L - seqTicks.sqTicks.head.db_tsunx/1000L))
*/
      if (seqTicks.sqTicks.isEmpty)
        (seqTicks, readMsec)

      else if (cp.tsLastTick.getOrElse(0L) > readFromTs && cp.tsLastTick.getOrElse(0L) < readToTs) {
        //logger.info(" -1- readTicksRecurs")
        (seqTicks, readMsec)
      }

      else if ( /*seqTicks.sqTicks.size*/(seqTicks.sqTicks.last.db_tsunx/1000L - seqTicks.sqTicks.head.db_tsunx/1000L) < cp.barDeepSec && cp.tsLastTick.getOrElse(0L) > readToTs) {
        //logger.info(" -2- readTicksRecurs")
        readTicksRecurs(readFromTs, readToTs + (readToTs - readFromTs), cp)
      }

      else {
        //logger.info(" -3- readTicksRecurs")
      (seqTicks, readMsec)
      }
    }

    /*
        def readTicksRecurs(readFromTs :Long, readToTs :Long, cp :CalcProperty) :seqTicksWithReadDuration = {
      val (seqTicks,readMsec) = dbInst.getTicksByInterval(cp, readFromTs, readToTs)
      if (seqTicks.sqTicks.isEmpty)
        (seqTicks,readMsec)
      else if (cp.tsLastTick.getOrElse(0L) > readFromTs && cp.tsLastTick.getOrElse(0L) <  readToTs)
        (seqTicks,readMsec)
      else if (seqTicks.sqTicks.size < cp.barDeepSec && cp.tsLastTick.getOrElse(0L) > readToTs)
        readTicksRecurs(readFromTs, readToTs + (readToTs-readFromTs), cp)
      else
        (seqTicks,readMsec)
    }
    */


    allCalcProps.cProps.foreach{cp =>
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
          case e :Throwable => logger.error("-2- ex when call readTicksRecurs ["+e.getMessage+"] ["+e.getCause+"]")
            (seqTicksObj(Nil),0L)
        }

      logger.info("readed recursively ticks SIZE="+seqTicks.sqTicks.size)

      if (seqTicks.sqTicks.nonEmpty) {
        val bars :Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec*1000L)
        if (bars.nonEmpty)
          dbInst.saveBars(bars)
        else logger.info("bars is Empty no save")
      }

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
    /*
    val t1 = System.currentTimeMillis
    calcIteration(dbInst)
    val t2 = System.currentTimeMillis
    logger.info("Duration of barCalc.run() - "+(t2 - t1) + " msecs.")
    */

    //todo: get here firstTickTS for each ticker and send inside calcIteration, to eliminate unnecessary reads.
    val fTicksMeta :Seq[FirstTickMeta] = dbInst.getFirstTicksMeta


    while(true){
      val t1 = System.currentTimeMillis
      calcIteration(dbInst,fTicksMeta)
      val t2 = System.currentTimeMillis
      logger.info("Duration of ITERATION barCalc.run() - "+(t2 - t1) + " msecs.")
    }

    /*
    def taskCalcBars(): Future[Unit] = Future {
      val t1 = System.currentTimeMillis
      calcIteration(dbInst)
      val t2 = System.currentTimeMillis
      logger.info("Duration of barCalc.run() - "+(t2 - t1) + " msecs.")
      Thread.sleep(20000) //todo: get from config
    }
    def loopCalcBars(): Future[Unit] = {
      taskCalcBars.flatMap(_ => loopCalcBars())
    }
    def infiniteLoop(): Future[Unit] = {
      Future.sequence(List(loopCalcBars())).map(_ => ())
    }
    Await.ready(infiniteLoop(), Duration.Inf)
*/


  }


}
