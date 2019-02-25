package bcpackage

import bcstruct.CalcProperties
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

/**
  * The Main class that contains all logic for bar calculation.
  *
  */
class BarCalculator(nodeAddress :String, dbType :String, readBySecs :Long) {
  val logger = LoggerFactory.getLogger(getClass.getName)

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
    /**
      * Here we have object dbInst that generally not related with type of DB, it's just has necessary methods
      * from DBImpl.
      * */
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties

    logger.debug(" Size of all bar calculator properties is "+allCalcProps.cProps.size)

    logger.debug("Calc property: -------------------------------------------------------------------------------------")
    for(cp <- allCalcProps.cProps) {
      logger.debug(" TICKER_ID=" + cp.tickerId + " DEEPSEC=" + cp.barDeepSec + " IS_ENABLED=["+cp.isEnabled+"]" +
                   "  LASTBAR_DDATE=[" + cp.dDateLastBar + "] LASTBAR_TSEND=[" + cp.tsEndLastBar + "] LASTTICK_DDATE=" +
                  cp.dDateLastTick + " LASTTICK_TS=" + cp.tsLastTick)
      logger.debug(" First tick TS = "+cp.tsFirstTicks)
      logger.debug(" Interval from last bar TS and last tick TS  =    "+cp.diffLastTickTSBarTS+"   sec." + " AVERAGE = "+
        Math.round(cp.diffLastTickTSBarTS/(60*60*24))+" days.")
      val currReadInterval :(Long,Long) = (cp.beginFrom,cp.beginFrom+readBySecs*1000L)

      logger.debug(s" In this iteration will read interval $currReadInterval")

      logger.debug(" ")
    }
    logger.debug("----------------------------------------------------------------------------------------------------")

    /*Don't forget parallel (futures in loop) when read each tickers (ticks for bars calculation). */


    /* MAIN CODE HERE ...........*/

  }

}
