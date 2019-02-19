package bcpackage

import com.datastax.driver.core.Session

/**
  * Class for encapsulate all queries and binds.
  * @param session - Opened session to Cassandra.
  */
class CassQueriesBinds(session : Session) {
  Option(session).orElse(throw new IllegalArgumentException("Null!"))
  require(session.isClosed==false)

  val sess = session

  val bndBars3600 = sess.prepare(""" select * from mts_bars.td_bars_3600 """).bind()

}
