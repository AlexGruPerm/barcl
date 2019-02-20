package bcpackage

import bcstruct.CalcProperties
import db.{DBCass}
import org.slf4j.LoggerFactory

/**
  * The Main class that contains all logic for bar calculation.
  * @param session - opened session to Cassandra, must be checked on Success.
  *
  */
class BarCalculator(nodeAddress :String, dbType :String) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def run = {
    /**
      * dbSess hides DB query execution logic and converting data sets into seq of scala objects.
      * Lets us get necessary structures of data.
      */
    val dbSess = new DBCass(nodeAddress,dbType)
    val allCalcProps :CalcProperties = dbSess.getAllCalcProperties
    logger.debug(" allCalcProps.cProps.size = "+allCalcProps.cProps.size)
  }

}
