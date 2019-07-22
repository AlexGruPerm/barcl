import com.datastax.driver.core.Cluster

val session = Cluster.builder().addContactPoint("84.201.147.105").build().connect()

val queryMD = session.prepare(
  """ select max(ddate) as ddate
               from mts_bars.bars_bws_dates
               where ticker_id=:tickerId and bar_width_sec=300 allow filtering;
         """)

(1 to 28).map(tickerID =>
  (tickerID,
    session.execute(queryMD.bind.setInt("tickerId", tickerID)).one.getDate("ddate"))
).foreach(td => println(td))

