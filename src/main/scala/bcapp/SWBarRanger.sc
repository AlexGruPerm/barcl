import com.datastax.driver.core._
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy

val sess = Cluster.builder()
  .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 1 * 60 * 1000))
  .addContactPoint("10.241.5.234")
  .build()
  .connect()

val (tickerID,barWidthSec) = (1,30)

val queryMaxDateFa = sess.prepare(""" select max(ddate) as faLastDate
                                    |   from mts_bars.bars_fa
                                    |  where ticker_id     = :p_ticker_id and
                                    |        bar_width_sec = :p_bar_width_sec
                                    |  allow filtering """.stripMargin)

val queryMinDateBar = sess.prepare(""" select min(ddate) as mindate
                                     |   from mts_bars.bars_bws_dates
                                     |  where ticker_id     = :p_ticker_id and
                                     |        bar_width_sec = :p_bar_width_sec """.stripMargin)

def getFaLastDate(tickerID :Int,barWidthSec :Int) :Option[LocalDate] = Option(sess.execute(queryMaxDateFa.bind()
    .setInt("p_ticker_id", tickerID)
    .setInt("p_bar_width_sec", barWidthSec))
  .one().getDate("faLastDate"))

def getBarMinDate(tickerID :Int,barWidthSec :Int) :Option[LocalDate] = Option(sess.execute(queryMinDateBar.bind()
    .setInt("p_ticker_id", tickerID)
    .setInt("p_bar_width_sec", barWidthSec)).one().getDate("mindate"))

val faLastDate :Option[LocalDate] = getFaLastDate(tickerID,barWidthSec)
val barMinDate :Option[LocalDate] =getBarMinDate(tickerID,barWidthSec)

println(s"faLastDate = $faLastDate barMinDate = $barMinDate")

//def readBars

sess.close()
