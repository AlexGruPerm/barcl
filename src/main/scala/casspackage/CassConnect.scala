package casspackage

import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Class just for opening Cassandra session and store it.
  */
class CassConnect {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def getCassSession(node_address : String) : Try[Session] = {
    logger.debug("CassConnect.getCassSession - Try open connection to cassandra.")
    try {
      val session = Cluster.builder().addContactPoint(node_address).build().connect()
      logger.debug("CassConnect.getCassSession - Connection opened.")
      Success(session)
    } catch {
      case noHostAvlExc: NoHostAvailableException => {
        val errMsg :String = "Host for connection is not available ["+node_address+"]"
        logger.error(errMsg)
        Failure(new AssertionError(errMsg))
      }
      case e: Throwable => {
        val errMsg :String = "Any kind of exception :"+e.getMessage
        logger.error(errMsg)
        Failure(new AssertionError(errMsg))
      }
    }
  }

}
