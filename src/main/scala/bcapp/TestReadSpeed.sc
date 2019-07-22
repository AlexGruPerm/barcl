import bcstruct.bcstruct.seqTicksWithReadDuration
import bcstruct.{Tick, seqTicksObj}
import com.datastax.driver.core.{Cluster, LocalDate, Row}

import scala.collection.JavaConverters._
/*
import bcstruct.bcstruct.seqTicksWithReadDuration
import bcstruct.seqTicksObj
import com.datastax.driver.core.{Cluster, LocalDate}
import scala.collection.JavaConverters._
*/

val session = Cluster.builder().addContactPoint("84.201.147.105").build().connect()

val ClusterConfig = session.getCluster.getConfiguration
ClusterConfig.getSocketOptions.setConnectTimeoutMillis(180000)
ClusterConfig.getSocketOptions.setReadTimeoutMillis(120000)
ClusterConfig.getSocketOptions.setKeepAlive(true)
ClusterConfig.getPoolingOptions.setIdleTimeoutSeconds(600)
val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion

val poolingOptions = session.getCluster.getConfiguration.getPoolingOptions

val bndTicksByTsIntervalONEDate = session.prepare(
  """ select db_tsunx,ask,bid
            from mts_src.ticks
           where ticker_id = :tickerId and
                 ddate     = :pDate and
                 db_tsunx >= :dbTsunxBegin and
                 db_tsunx <= :dbTsunxEnd
           order by  ts ASC, db_tsunx ASC
           allow filtering; """)

val rowToSeqTicksWDate = (rowT: Row, tickerID: Int, pDate :LocalDate) => {
  Tick(
    tickerID,
    pDate,
    rowT.getLong("db_tsunx"),
    rowT.getDouble("ask"),
    rowT.getDouble("bid")
  )
}

//todo: comment in main code.
val rowToSeqTicks = (rowT: Row, tickerID: Int) => {
  Tick(
    tickerID,
    rowT.getDate("ddate"),
    rowT.getLong("db_tsunx"),
    rowT.getDouble("ask"),
    rowT.getDouble("bid")
  )
}

def getTicksByInterval(tickerId: Int, tsBegin: Long, tsEnd: Long,bws :Int) :(seqTicksObj,Long) = {
  val t1 = System.currentTimeMillis
  (seqTicksObj(
         (LocalDate.fromMillisSinceEpoch(tsBegin).getDaysSinceEpoch to
          LocalDate.fromMillisSinceEpoch(tsEnd).getDaysSinceEpoch).flatMap {
          dayNum =>
            val d: LocalDate = LocalDate.fromDaysSinceEpoch(dayNum)
            session.execute(bndTicksByTsIntervalONEDate.bind
              .setInt("tickerId", tickerId)
              .setDate("pDate", d)
              .setLong("dbTsunxBegin", tsBegin)
              .setLong("dbTsunxEnd", tsEnd))
              .all().iterator.asScala.toSeq.map(r => rowToSeqTicksWDate(r, tickerId, d))
        }
      ), System.currentTimeMillis - t1)
}


val readBySecs :Long = 3*60*60

def intervalSecondsDouble(sqTicks :Seq[Tick]) :Double =
  (sqTicks.last.db_tsunx.toDouble - sqTicks.head.db_tsunx.toDouble) / 1000

/** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
def readTicksRecurs(readFromTs: Long, readToTs: Long): seqTicksWithReadDuration = {
  val (seqTicks, readMsec) = getTicksByInterval(1, readFromTs, readToTs, 30)

  if (seqTicks.sqTicks.isEmpty && 1563483602072L > readToTs)
    readTicksRecurs(readFromTs, readToTs + readBySecs * 1000L)

  else if (seqTicks.sqTicks.isEmpty && 1563483602072L <= readToTs)
    (seqTicks, readMsec)

  else if (1563483602072L > readFromTs && 1563483602072L < readToTs)
    (seqTicks, readMsec)

  else if (seqTicks.sqTicks.nonEmpty && intervalSecondsDouble(seqTicks.sqTicks) < 30.toDouble &&
    1563483602072L > readToTs)
    readTicksRecurs(readFromTs, readToTs + readBySecs * 1000L)

  else
    (seqTicks, readMsec)

}
/** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
  //cur 22 1563775726804L
  //max 19 1563569700092L

val res = readTicksRecurs(1563483602072L,1563569700092L)
println("TICKS SIZE = "+res._1.sqTicks.size+" duration = "+res._2+" ms.")
println("MIN HEAD TICK ="+res._1.sqTicks.head.db_tsunx+" ddate="+res._1.sqTicks.head.dDate)
println("MAX LAST TICK ="+res._1.sqTicks.last.db_tsunx+" ddate="+res._1.sqTicks.last.dDate)

/*
//avg durations.
val res = (1 to 10).map(_ =>
readTicksRecurs(1563483602072L,1563775726804L)._2
).sum/10
println("Old Code avg duration is = "+res)
*/

/*
New Code avg duration is = 6322
Old Code avg duration is = 9988 with .sortBy() in App
Old Code avg duration is =  without .sortBy() in App

*/

/*

TICKS SIZE = 84986 duration = 7157 ms.
res4: Unit = ()
MIN HEAD TICK =1563483602072 ddate=2019-07-19
res5: Unit = ()
MAX LAST TICK =1563775726804 ddate=2019-07-22
res6: Unit = ()

OLD CODE:
------------------------------------------------------------
TICKS SIZE = 84986 duration = 4535 ms.
TICKS SIZE = 84986 duration = 7575 ms.
TICKS SIZE = 84986 duration = 8636 ms.
TICKS SIZE = 84986 duration = 22616 ms.

res4: Unit = ()
MIN HEAD TICK =1563483602072 ddate=2019-07-19
res5: Unit = ()
MAX LAST TICK =1563775726804 ddate=2019-07-22
res6: Unit = ()

NEW CODE:
------------------------------------------------------------
TICKS SIZE = 84986 duration = 8570 ms.
TICKS SIZE = 84986 duration = 7942 ms.
TICKS SIZE = 84986 duration = 8290 ms.

res4: Unit = ()
MIN HEAD TICK =1563483602072 ddate=2019-07-19
res5: Unit = ()
MAX LAST TICK =1563775726804 ddate=2019-07-22
res6: Unit = ()

res4: Unit = ()
MIN HEAD TICK =1563569700092 ddate=2019-07-19
res5: Unit = ()
MAX LAST TICK =1563743102214 ddate=2019-07-22
res6: Unit = ()

res4: Unit = ()
MIN HEAD TICK =1563483602072 ddate=2019-07-19
res5: Unit = ()
MAX LAST TICK =1563775726804 ddate=2019-07-22
res6: Unit = ()
*/