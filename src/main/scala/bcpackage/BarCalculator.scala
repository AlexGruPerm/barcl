package bcpackage

import bcstruct.{Bar, CalcProperties}
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

  def calcIteration(dbInst :DBImpl) ={
    /**
      * Here we have object dbInst that generally not related with type of DB, it's just has necessary methods
      * from DBImpl.
      * */
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties
    logger.debug(" Size of all bar calculator properties is "+allCalcProps.cProps.size)


    for(cp <- allCalcProps.cProps) {
      logger.debug("Calc property: -------------------------------------------------------------------------------------")
      logger.debug(" TICKER_ID=" + cp.tickerId + " DEEPSEC=" + cp.barDeepSec + " IS_ENABLED=["+cp.isEnabled+"]" +
        "  LASTBAR_DDATE=[" + cp.dDateLastBar + "] LASTBAR_TSEND=[" + cp.tsEndLastBar + "] LASTTICK_DDATE=" +
        cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick)
      logger.debug(" First tick TS = "+cp.tsFirstTicks)
      logger.debug(" Interval from last bar TS and last tick TS  =    "+cp.diffLastTickTSBarTS+"   sec." + " AVERAGE = "+
        Math.round(cp.diffLastTickTSBarTS/(60*60*24))+" days.")

      val currReadInterval :(Long,Long) = (cp.beginFrom,
        Seq(cp.beginFrom+readBySecs*1000L,
          cp.tsLastTick match {
            case Some(ts) => ts
            case _ => cp.beginFrom
          }).min)

      logger.debug(s" In this iteration will read interval $currReadInterval for deepSecs="+cp.barDeepSec)
      val (seqTicks,readMsec) = dbInst.getTicksByInterval(cp.tickerId, currReadInterval._1, currReadInterval._2)
      logger.debug("Duration of read ticks seq = "+ readMsec + " msecs. Read ["+seqTicks.sqTicks.size+"] ticks.")

      val bars :Seq[Bar] = dbInst.getCalculatedBars(cp.tickerId, seqTicks.sqTicks, cp.barDeepSec*1000L)
      logger.debug(" bars.size="+bars.size)

      dbInst.saveBars(bars)
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
