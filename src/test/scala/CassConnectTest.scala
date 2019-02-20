import dbpackage.{CassConnect, QueriesBinds}
import com.datastax.driver.core.Session
import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

class CassConnectTest extends FunSuite {

  test("1. CassConnect.getSession Correct IP") {
    assert((new CassConnect).getSession("193.124.112.90").isSuccess)
  }

  test("2. CassConnect.getSession Incorrect IP") {
    assert((new CassConnect).getSession("193.124.112.9").isFailure)
  }

  test("3. QueriesBinds created with closed session should throw IllegalArgumentException") {
    val node: String = "193.124.112.90"
    val session : Try[Session] = (new CassConnect).getSession(node)
    session match {
      case Success(sess) => {
        sess.close()
        intercept[IllegalArgumentException] {
          new QueriesBinds(sess)
        }
      }
      case Failure(f) => println("no test here")
    }
  }

  test("4. QueriesBinds created with NULL as session should throw IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      new QueriesBinds(null)
    }
  }

}
