import casspackage.{CassConnect, CassQueriesBinds}
import com.datastax.driver.core.Session
import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

class CassConnectTest extends FunSuite {

  test("1. CassConnect.getCassSession Correct IP") {
    assert((new CassConnect).getCassSession("193.124.112.90").isSuccess)
  }

  test("2. CassConnect.getCassSession Incorrect IP") {
    assert((new CassConnect).getCassSession("193.124.112.9").isFailure)
  }

  test("3. CassQueriesBinds created with closed session should throw IllegalArgumentException") {
    val node: String = "193.124.112.90"
    val session : Try[Session] = (new CassConnect).getCassSession(node)
    session match {
      case Success(sess) => {
        sess.close()
        intercept[IllegalArgumentException] {
          new CassQueriesBinds(sess)
        }
      }
      case Failure(f) => println("no test here")
    }
  }

  test("4. CassQueriesBinds created with NULL as session should throw IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      new CassQueriesBinds(null)
    }
  }

}
