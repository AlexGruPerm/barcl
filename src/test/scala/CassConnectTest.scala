import com.datastax.driver.core.exceptions.NoHostAvailableException
import db.{DBCass}
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

class CassConnectTest extends FunSuite {
  val logger = LoggerFactory.getLogger(getClass.getName)


  test("1. CassConnect.getSession Correct IP") {
    assert(new DBCass("193.124.112.90","cassandra").getTrySession.isSuccess)
  }

  test("2. CassConnect.getSession Incorrect IP") {
    intercept[NoHostAvailableException] {
      new DBCass("193.124.112.9", "cassandra").getTrySession
    }
  }

  test("3. QueriesBinds created with closed session should throw IllegalArgumentException") {
    val dbCassImpl = new DBCass("193.124.112.90","cassandra")
    val session = dbCassImpl.getTrySession
        session match {
          case Success(ss) => {
            ss.close()
            intercept[IllegalArgumentException] {
              dbCassImpl.getAllCalcProperties
            }
          }
          case Failure(f) => {
            println("no test here")
          }
        }

    }

/*
  test("4. QueriesBinds created with NULL as session should throw IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      new QueriesBinds(null)
    }
  }
*/


}
