package dbpackage

import com.datastax.driver.core.Session
import scala.collection.JavaConverters._

/**
  * Class for encapsulate all cql queries and binds for Cassandra.
  * @param session - Opened session to Cassandra.
  */
class QueriesBinds(session : AutoCloseable) {
  Option(session).orElse(throw new IllegalArgumentException("Null!"))
  //require(session.isClosed==false)
  //use pattern matching here to determine correct name of Class, Session, Connection...

  private val sess = session

  /**
    * Meta information, which tickers and which seconds deeps we need read for calculation.
    * For ex:
    *  ticker_id, deep_sec
    *  1          30
    *  1          60
    *  2          30
    *  ...
    *  means that we want calculate bars for ticker_id=1 for widths 30 and 60 seconds,
    *  and for ticker_id=2 just 30 seconds.
    */
  private val bndBCalcProps = sess.prepare(""" select * from mts_meta.bars_property """).bind()

  private val bndBars3600 = sess.prepare(""" select * from mts_bars.td_bars_3600 """).bind()

  def dsBCalcProperty = {
    sess.execute(bndBCalcProps).all().iterator.asScala
  }



}
