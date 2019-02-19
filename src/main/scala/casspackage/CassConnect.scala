package casspackage

import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory

class CassConnect {

  object logger extends Serializable {
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
  }

  def getCassSession(node_address : String) : Option[Session] = {
    logger.log.debug("CassConnect.getCassSession - Try open connection to cassandra.")
    try {
      val session = Cluster.builder().addContactPoint(node_address).build().connect()
      Option(session)
    } catch {
      case noHostAvlExc: NoHostAvailableException => {
        logger.log.error("Host for connection is not available ["+node_address+"]")
        None
      }
      case _: Throwable => {
        logger.log.error("Any kind of exception.")
        None
      }
    }

  }

}
