package bcpackage

import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

/**
  * The Main class that contains all logic for bar calculation.
  * @param session - opened session to Cassandra, must be check on Success.
  *
  */
class BarCalculator(session : Session) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def run = {
    val cassQBinds = new CassQueriesBinds(session)
  }

}
