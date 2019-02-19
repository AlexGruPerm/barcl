package bcpackage

import casspackage.{CassQueriesBinds, DataSetOperations}
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

/**
  * The Main class that contains all logic for bar calculation.
  * @param session - opened session to Cassandra, must be checked on Success.
  *
  */
class BarCalculator(session : Session) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def run = {
    /**
      * DataSetOperations hides Cassandra query execution logic and converting data sets into seq of scala objects.
      * Lets us get necessary structures of data.
      */
    val dsOper = new DataSetOperations(new CassQueriesBinds(session))

    val allCalcProps :CalcProperties = dsOper.getAllCalcProperties
    logger.debug(" allCalcProps.cProps.size = "+allCalcProps.cProps.size)
  }

}
