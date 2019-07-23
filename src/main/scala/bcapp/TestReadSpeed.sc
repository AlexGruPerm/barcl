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

val bndTicksByTsIntervalONEDate = session.prepare(
  """ select db_tsunx,ask,bid
            from mts_src.ticks
           where ticker_id = :tickerId and
                 ddate     = :pDate and
                 db_tsunx >= :dbTsunxBegin and
                 db_tsunx <= :dbTsunxEnd
           order by  ts ASC, db_tsunx ASC
           allow filtering; """)

val bndTicksByTsIntervalONEDateV3 = session.prepare(
  """ select db_tsunx,ask,bid
                from mts_src.ticks
               where ticker_id = :tickerId and
                     ddate     = :pDate """)

val rowToSeqTicksWDate = (rowT: Row, tickerID: Int, pDate :LocalDate) => {
  Tick(
    tickerID,
    pDate,
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

def getTicksByIntervalV3(tickerId: Int, tsBegin: Long, tsEnd: Long,bws :Int) :(seqTicksObj,Long) = {
  val t1 = System.currentTimeMillis
  (seqTicksObj(
    (LocalDate.fromMillisSinceEpoch(tsBegin).getDaysSinceEpoch to
      LocalDate.fromMillisSinceEpoch(tsEnd).getDaysSinceEpoch).flatMap {
      dayNum =>
        val d: LocalDate = LocalDate.fromDaysSinceEpoch(dayNum)
        session.execute(bndTicksByTsIntervalONEDateV3.bind
          .setInt("tickerId", tickerId)
          .setDate("pDate", d)
        ).all().iterator.asScala.toSeq.map(r => rowToSeqTicksWDate(r, tickerId, d))
          .filter(elm => elm.db_tsunx >=tsBegin && elm.db_tsunx <= tsEnd)
          .sortBy(t => t.db_tsunx)
    }
  ), System.currentTimeMillis - t1)
}


val readBySecs :Long = 3*60*60

def intervalSecondsDouble(sqTicks :Seq[Tick]) :Double =
  (sqTicks.last.db_tsunx.toDouble - sqTicks.head.db_tsunx.toDouble) / 1000

/** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
def readTicksRecurs(readFromTs: Long, readToTs: Long): seqTicksWithReadDuration = {
  val (seqTicks, readMsec) = getTicksByIntervalV3(1, readFromTs, readToTs, 30)

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

val res = readTicksRecurs(1563483602072L,1563775726804L)
println("TICKS SIZE = "+res._1.sqTicks.size+" duration = "+res._2+" ms.")
println("MIN HEAD TICK ="+res._1.sqTicks.head.db_tsunx+" ddate="+res._1.sqTicks.head.dDate)
println("MAX LAST TICK ="+res._1.sqTicks.last.db_tsunx+" ddate="+res._1.sqTicks.last.dDate)

/*

------------------------------------------------------------
OLD (v2)
TICKS SIZE = 84986 duration = 7117 ms.

MIN HEAD TICK =1563483602072 ddate=2019-07-19
MAX LAST TICK =1563775726804 ddate=2019-07-22

TICKS SIZE = 84986 duration = 9952 ms.
MIN HEAD TICK =1563483602072 ddate=2019-07-19
MAX LAST TICK =1563775726804 ddate=2019-07-22

*/