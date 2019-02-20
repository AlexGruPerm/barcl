package dbpackage

import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Class just for opening DB (Cassandra,Oracle,Postgres) session and store it.
  */
class DBConnector(nodeAddress: String, dbType :String) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  val supportedDb = Seq("cassandra"/*,"oracle","postgres"*/)

  /**
    * Common function to get connection/session to necessary database
  */
  def getDBSession:Try[Session] ={
    require(supportedDb.contains(dbType),"Not supported database type.")

    logger.debug(s"getSession - Try open connection to $dbType.")

    dbType match {
      case cass if cass =="cassandra" => getCassSession
      case _ => Failure(new Throwable(s"This type of database not supported [$dbType]"))
    }
  }


  def getCassSession : Try[Session] =
    try {
      val session = Cluster.builder().addContactPoint(nodeAddress).build().connect()
      logger.debug(s"($getClass).getSession - Connection opened for [$dbType].")
      Success(session)
    } catch {
      case exHostAvail: NoHostAvailableException => exNoHostAvail(exHostAvail)
      case e: Throwable => exCaseGetSession(e)
    }

  def exCaseGetSession(e: Throwable) = {
    val errMsg :String = "Any kind of exception :"+e.getMessage
    logger.error(errMsg)
    Failure(new AssertionError(errMsg))
  }

  def exNoHostAvail(e: NoHostAvailableException) = {
    val errMsg :String = s"Host for connection is not available ["+nodeAddress+"] for [$dbType]"
    logger.error(errMsg)
    Failure(new AssertionError(errMsg))
  }

}



object DBConnector {
  def apply(nodeAddress: String, dbType :String) = {
    new DBConnector(nodeAddress,dbType)
  }
}