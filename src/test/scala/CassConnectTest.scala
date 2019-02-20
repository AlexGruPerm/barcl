import dbpackage.{DBConnector, QueriesBinds}
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

class CassConnectTest extends FunSuite {
  val logger = LoggerFactory.getLogger(getClass.getName)

  test("1. CassConnect.getSession Correct IP") {
    assert((new DBConnector("193.124.112.90","cassandra")).getDBSession.isSuccess)
  }

  test("2. CassConnect.getSession Incorrect IP") {
    assert((new DBConnector("193.124.112.9","cassandra")).getDBSession.isFailure)
  }

  test("3. QueriesBinds created with closed session should throw IllegalArgumentException") {
    val node: String = "193.124.112.90"
    val session = (new DBConnector("193.124.112.90","cassandra")).getDBSession
        session match {
          case Success(ss) => {
            ss.close()
            intercept[IllegalArgumentException] {
              new QueriesBinds(ss)
            }
          }
          case Failure(f) => {
            println("no test here")
          }
        }

    }

  test("4. QueriesBinds created with NULL as session should throw IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      new QueriesBinds(null)
    }
  }


}
