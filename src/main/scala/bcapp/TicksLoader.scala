package bcapp

import com.datastax.driver.core.{BatchStatement, Cluster, LocalDate}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class TicksCountDays(ticker_id :Int,ddate :LocalDate,ticks_count :Long)

case class TicksCountTotal(ticker_id :Int,ticks_count :Long)

case class  Tick(
                  tickerId  :Int,
                  dDate     :LocalDate,
                  ts        :Long,
                  db_tsunx  :Long,
                  ask       :Double,
                  bid       :Double
                )

object TicksLoader extends App {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val nodeFrom: String = "" // remote Yandex cloud (sinle instance)
  val nodeTo: String = ""  // local 3 instance Cluster. For cluster:

  val sessFrom = Cluster.builder().addContactPoint(nodeFrom).build().connect()
  val sessTo = Cluster.builder().addContactPoint(nodeTo).build().connect()

  sessFrom.getCluster.getConfiguration.getSocketOptions.setReadTimeoutMillis(12000)
  sessTo.getCluster.getConfiguration.getSocketOptions.setConnectTimeoutMillis(6000)

  sessFrom.getCluster.getConfiguration.getPoolingOptions.setHeartbeatIntervalSeconds(90);
  sessTo.getCluster.getConfiguration.getPoolingOptions.setHeartbeatIntervalSeconds(90);

  val bndTicksCountDays = sessFrom.prepare(""" select ticker_id,ddate,ticks_count from mts_src.ticks_count_days """).bind()

  val bndTicksTotal = sessFrom.prepare(""" select * from mts_src.ticks_count_total """).bind()

  val bndTicksByTickerDdate = sessFrom.prepare(
    """ select * from mts_src.ticks
      where ticker_id=:tickerId and ddate=:dDate """).bind()

  val bndSaveTickWb = sessTo.prepare(""" insert into mts_src.ticks(ticker_id,ddate,ts,db_tsunx,ask,bid)
                                     	values(:p_ticker_id,:p_ddate,:p_ts,:p_db_tsunx,:p_ask,:p_bid) """)

  val bndSaveTicksByDay = sessTo.prepare(
    """ update mts_src.ticks_count_days
      set ticks_count=ticks_count+:p_ticks_count where ticker_id=:p_ticker_id and ddate=:p_ddate """).bind()

  val bndSaveTicksCntTotal = sessTo.prepare(
    """  update mts_src.ticks_count_total
      set ticks_count=ticks_count+:p_ticks_count where ticker_id=:p_ticker_id """).bind()

  val sqTicksCountTotal = sessFrom.execute(bndTicksTotal)
    .all().iterator.asScala.toSeq
    .map(r => TicksCountTotal(r.getInt("ticker_id"),r.getLong("ticks_count")))
    .sortBy(t => t.ticker_id)

  val sqTicksCountDays = sessFrom.execute(bndTicksCountDays)
    .all().iterator.asScala.toSeq
    .map(r => TicksCountDays(r.getInt("ticker_id"),r.getDate("ddate"),r.getLong("ticks_count")))
    .sortBy(t => (t.ticker_id,t.ddate.getMillisSinceEpoch)) //sort by ticker_id AND ddate

  for(elm <- sqTicksCountDays){
    val tickCntTotal = sqTicksCountTotal.filter(tct => tct.ticker_id == elm.ticker_id).head.ticks_count
    println(elm +"  TOTAL_TICKS_COUNT = "+tickCntTotal)

    val sqTicks = sessFrom.execute(bndTicksByTickerDdate
      .setInt("tickerId", elm.ticker_id)
      .setDate("dDate",elm.ddate)
    ).all().iterator.asScala.toSeq.map(r => Tick(
      r.getInt("ticker_id"),
      r.getDate("ddate"),
      r.getLong("ts"),
      r.getLong("db_tsunx"),
      r.getDouble("ask"),
      r.getDouble("bid"))
    ).sortBy(t => t.db_tsunx)
    println(" READ sqTicks.size="+sqTicks.size+" head.db_tsunx="+sqTicks.head.db_tsunx)

    val partSqTicks = sqTicks.grouped(65535)

    for(thisPartOfSeq <- partSqTicks) {
      var batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
      thisPartOfSeq.foreach {
        t =>
          batch.add(bndSaveTickWb.bind
            .setInt("p_ticker_id", t.tickerId)
            .setDate("p_ddate", t.dDate)
            .setLong("p_ts", t.ts)
            .setLong("p_db_tsunx", t.db_tsunx)
            .setDouble("p_ask", t.ask)
            .setDouble("p_bid", t.bid))
      }
      sessTo.execute(batch)
    }

    sessTo.execute(bndSaveTicksByDay
      .setInt("p_ticker_id", elm.ticker_id)
      .setDate("p_ddate", elm.ddate)
      .setLong("p_ticks_count",elm.ticks_count))

    println("   TICKS SAVED INTO Cluster.")
  }


  for(elm <- sqTicksCountDays.map(e => e.ticker_id).distinct) {
    val tickCntTotal = sqTicksCountTotal.filter(tct => tct.ticker_id == elm).head.ticks_count
    sessTo.execute(bndSaveTicksCntTotal
      .setInt("p_ticker_id", elm)
      .setLong("p_ticks_count",tickCntTotal))
  }

  sessFrom.close()
  sessTo.close()
}
