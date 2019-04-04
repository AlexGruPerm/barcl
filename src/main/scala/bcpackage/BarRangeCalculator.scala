package bcpackage

import bcstruct.{barsForFutAnalyze, barsFutAnalyzeRes, barsMeta, barsResToSaveDB}
import com.datastax.driver.core.LocalDate
import com.madhukaraphatak.sizeof.SizeEstimator
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

class BarRangeCalculator(nodeAddress :String, prcntsDiv: Seq[Double]) {
  val logger = LoggerFactory.getLogger(getClass.getName)
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.getDaysSinceEpoch)

  def calcIteration(dbInst :DBImpl) = {
    val allBarsHistMeta :Seq[barsMeta] = dbInst.getAllBarsHistMeta
    allBarsHistMeta.map(bh => (bh.tickerId,bh.barWidthSec)).filter(elm => elm._1 >=27).distinct.foreach {
       {//remove this breaket
        case (tickerID: Int, barWidthSec: Int) =>
        val lastFaCalcedDdate: LocalDate = dbInst.getLastBarFaTSEnd(tickerID, barWidthSec) match {
          case Some(nnDdate) => nnDdate
          case None => allBarsHistMeta.filter(b => b.tickerId == tickerID && b.barWidthSec==barWidthSec).map(b => b.dDate).min
        }

        //logger.debug("CHECK TSEND >>> (" + tickerID + "," + barWidthSec + ")  lastFaCalcedTsEnd=[" + lastFaCalcedDdate + "]")
/*
          logger.debug("AllDDATES=["+
          allBarsHistMeta.filter(
            r =>
              r.tickerId == tickerID &&
                r.barWidthSec == barWidthSec).size
          +"] FILTEREDBY=["+
          allBarsHistMeta.filter(
            r =>
              r.tickerId == tickerID &&
                r.barWidthSec == barWidthSec &&
                r.dDate.getDaysSinceEpoch >= lastFaCalcedDdate.getDaysSinceEpoch).size
          +"]")
*/

        val allBars: Seq[barsForFutAnalyze] = dbInst.getAllCalcedBars(allBarsHistMeta.filter(
          r =>
            r.tickerId == tickerID &&
            r.barWidthSec == barWidthSec &&
            r.dDate.getDaysSinceEpoch >= lastFaCalcedDdate.getDaysSinceEpoch))

        logger.debug("allBars BY [" + tickerID + "," + barWidthSec + "] SIZE = " + allBars.size + "  (" + allBars.head.ts_end + " - " + allBars.last.ts_end + ") " +
          " (" + allBars.head.dDate + " - " + allBars.last.dDate + ") SIZE=" + SizeEstimator.estimate(dbInst) + " bytes.")

        val prcntsDivSize = prcntsDiv.size
        val t1FAnal = System.currentTimeMillis
        val futAnalRes: Seq[barsFutAnalyzeRes] = prcntsDiv.flatMap(p => dbInst.makeAnalyze(allBars, p))
        val t2FAnal = System.currentTimeMillis
        logger.debug("After analyze RES.size = " + futAnalRes.size + " Duration " + (t2FAnal - t1FAnal) + " msecs.")

        val t1FS = System.currentTimeMillis

        /**
          * Old algo when it was converted into columns of type Map.
          **/
        /*
        val resFSave :Seq[barsResToSaveDB] = futAnalRes.sortBy(t => t.srcBar.ts_end).sliding(prcntsDivSize, prcntsDivSize)
          .filter(elm => elm.size == prcntsDivSize)
          .map(intList => new barsResToSaveDB(intList)).to[collection.immutable.Seq]
        */
        val resFSave: Seq[barsResToSaveDB] = futAnalRes.sortBy(t => t.srcBar.ts_end)
          .map(r => new barsResToSaveDB(
            r.srcBar.tickerId,
            r.srcBar.barWidthSec,
            r.srcBar.dDate,
            r.srcBar.ts_end,
            r.p,
            r.resType,
            Map(
              "ts_end" -> (r.resAnal match {
                case Some(ri) => ri.ts_end.toString
                case None => ""
              }),
              "durSec" -> (r.resAnal match {
                case Some(ri) => Math.round((/*  (ri.ts_end + ri.ts_begin)/2L  */ ri.ts_end - r.srcBar.ts_end) / 1000L).toString
                case None => "0"
              })
            )
          ))

        val t2FS = System.currentTimeMillis
        logger.info("Duration of gathering resFSave - " + (t2FS - t1FS) + " msecs.")

        val t1Save = System.currentTimeMillis
        dbInst.saveBarsFutAnal(resFSave)
        val t2Save = System.currentTimeMillis
        logger.info("Duration of saveing into mts_bars.bars_fa - " + (t2Save - t1Save) + " msecs.")
      //logger.debug("==========================================================")

    }
    }
  }


  def run = {
    /**
      * dbSess hides DB query execution logic and converting data sets into seq of scala objects.
      * Lets us get necessary structures of data.
      */
      /**
        * Add read last analyzed bar, don't recalc all.
        *
        * */
    val dbInst: DBImpl = new DBCass(nodeAddress, "cassandra")
    val t1 = System.currentTimeMillis
     calcIteration(dbInst)
    val t2 = System.currentTimeMillis
    logger.info("Duration of BarRangeCalculator.run() - "+(t2 - t1) + " msecs.")

    /*
    def taskCalcRange(): Future[Unit] = Future {
      val t1 = System.currentTimeMillis
      calcIteration(dbInst)
      val t2 = System.currentTimeMillis
      logger.info("Duration of BarRangeCalculator.run() - "+(t2 - t1) + " msecs.")
      Thread.sleep(1500)
    }
    def loopCalcRange(): Future[Unit] = {
      taskCalcRange.flatMap(_ => loopCalcRange())
    }
    def infiniteLoop(): Future[Unit] = {
      Future.sequence(List(loopCalcRange())).map(_ => ())
    }
    Await.ready(infiniteLoop(), Duration.Inf)
    */

  }

}
