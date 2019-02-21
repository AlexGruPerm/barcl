package bcpackage

import bcstruct.CalcProperties
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

/**
  * The Main class that contains all logic for bar calculation.
  *
  */
class BarCalculator(nodeAddress :String, dbType :String) {
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
    logger.debug(s"Session to [dbType] is opened. Continue.")
    /**
      * Here we have object dbInst that generally not related with type of DB, it's just has necessary methods
      * from DBImpl.
      * */
    val allCalcProps :CalcProperties = dbInst.getAllCalcProperties
    logger.debug(" Size of all bar calculator properties is "+allCalcProps.cProps.size)

    for(tw <- allCalcProps.cProps) {
      logger.debug(tw.tickerId+" "+tw.barDeepSec)
    }

    /* MAIN CODE HERE ...........*/

  }

}
