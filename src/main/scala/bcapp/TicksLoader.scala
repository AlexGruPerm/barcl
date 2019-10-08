package bcapp

import com.datastax.driver.core.exceptions.OperationTimedOutException
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

  logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  logger.info("                 ")
  logger.info("BEGIN TicksLoader")
  logger.info("                 ")
  logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  Thread.sleep(1000)


  val nodeFrom: String = "84.201.169.181" // remote MTS
  val nodeTo: String = "10.241.5.234"  // local 3 instance Cluster. For cluster:

  val sessFrom = Cluster.builder().addContactPoint(nodeFrom).build().connect()
  val sessTo = Cluster.builder().addContactPoint(nodeTo).build().connect()

  sessFrom.getCluster.getConfiguration.getSocketOptions.setReadTimeoutMillis(360000)
  sessFrom.getCluster.getConfiguration.getSocketOptions.setKeepAlive(true)

  sessTo.getCluster.getConfiguration.getSocketOptions.setConnectTimeoutMillis(600000)
  sessTo.getCluster.getConfiguration.getSocketOptions.setKeepAlive(true)

  sessFrom.getCluster.getConfiguration.getPoolingOptions.setHeartbeatIntervalSeconds(360);
  sessTo.getCluster.getConfiguration.getPoolingOptions.setHeartbeatIntervalSeconds(360);




 //todo: xxx
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
    .map(r => TicksCountTotal(r.getInt("ticker_id"),r.getLong("ticks_count")))/*.filter(elm => Seq(30,31,32,33,34,35,36,37,38).contains(elm.ticker_id))*/
    .sortBy(t => t.ticker_id)

  val sqTicksCountDays = sessFrom.execute(bndTicksCountDays)
    .all().iterator.asScala.toSeq
    .map(r => TicksCountDays(r.getInt("ticker_id"),r.getDate("ddate"),r.getLong("ticks_count")))
    .sortBy(t => (t.ticker_id,t.ddate.getMillisSinceEpoch)) //sort by ticker_id AND ddate

  for(elm <- sqTicksCountDays/*.filter(elm => Seq(30,31,32,33,34,35,36,37,38).contains(elm.ticker_id))*/){
    val tickCntTotal = sqTicksCountTotal.filter(tct => tct.ticker_id == elm.ticker_id).head.ticks_count
    logger.info(elm +"  TOTAL_TICKS_COUNT = "+tickCntTotal)

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
    logger.info(" READ sqTicks.size="+sqTicks.size+" head.db_tsunx="+sqTicks.head.db_tsunx)

    /**
      * https://stackoverflow.com/questions/21819035/write-timeout-thrown-by-cassandra-datastax-driver
      *
      * I faced the same problem once. I was using BatchStatement to write data in Cassnadra. My batch size was 10000.
      * After reducing this batch size, I didn't face the exception.
      * So, maybe you are trying to load to much data into Cassandra in a single request.
    */
    val partSqTicks = sqTicks.grouped(30000/*65535*/)

    logger.info(">>> begin BATCH for(thisPartOfSeq <- partSqTicks)")


    for(thisPartOfSeq <- partSqTicks) {
      var batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
      batch.setIdempotent(true)
      //RetryPolicy.RetryDecision.retry(ConsistencyLevel.ANY).
      //val policy :RetryPolicy = RetryP
      //batch.setRetryPolicy().

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
      logger.info("inside before execute batch.size()="+batch.size())
      sessTo.execute(batch)
      batch.clear()
    }
    logger.info("<<< end BATCH insert")

    logger.info("next begin bndSaveTicksByDay")


    try {
      sessTo.execute(bndSaveTicksByDay
        .setInt("p_ticker_id", elm.ticker_id)
        .setDate("p_ddate", elm.ddate)
        .setLong("p_ticks_count",elm.ticks_count))
    } catch {
      case ex: OperationTimedOutException =>
      {
        logger.info("EXCEPTION bndSaveTicksByDay = "+ex.getLocalizedMessage)
        ex.printStackTrace()
      }
    }



    logger.info("   TICKS SAVED INTO Cluster.")
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
