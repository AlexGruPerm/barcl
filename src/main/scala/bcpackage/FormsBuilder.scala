package bcpackage

import bcstruct.{bForm, barsFaMeta, barsResToSaveDB, tinyTick}
import com.madhukaraphatak.sizeof.SizeEstimator
import db.{DBCass, DBImpl}
import org.slf4j.{Logger, LoggerFactory}
import com.datastax.driver.core.LocalDate

class FormsBuilder(nodeAddress: String, prcntsDiv: Seq[Double], formDeepKoef: Int, intervalNewGroupKoeff: Int) {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val seqWays: Seq[String] = Seq("mx", "mn")

  def allFABarsDebugLog(tickerId: Int, barWidthSec: Int, allFABars: Seq[barsResToSaveDB]): Unit = {
    if (allFABars.nonEmpty)
      logger.info("allFABars for " + (tickerId, barWidthSec) + " SIZE " + allFABars.size +
        "  (" + allFABars.head.ts_end + ") " +
        " (" + allFABars.head.dDate + " - " + allFABars.last.dDate + ") SIZE=" +
        SizeEstimator.estimate(allFABars) / 1024L / 1024L + " Mb.")
     else logger.info("allFABars for " + (tickerId, barWidthSec) + " SIZE = 0 EMPTY !")
  }

  def debugLastBarsOfGrp(lastBarsOfForms: Seq[(Int, barsResToSaveDB)]): Unit = {
    lastBarsOfForms.collect {
      case (grpNum, bar) =>
        logger.info("Group=[" + grpNum + "] Form_beginTS=[" + (bar.ts_end - formDeepKoef * bar.barWidthSec * 1000L) +
          "]  Bar = " + bar) // - same seconds can go into weekends.
    }
  }

  def firstLog(tickerId: Int, barWidthSec: Int): Unit = {
    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
    logger.info("   ")
    logger.info(" FormBuilder iteration for [" + (tickerId, barWidthSec) + "] ")
    logger.info("   ")
    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
  }

  def logFirstLastBars(thisTickerID: Int, firstBarOfLastBars: barsResToSaveDB,
                       lastBarOfLastBars: barsResToSaveDB): Unit = {
    logger.info(" thisTickerID=" + thisTickerID + " getAllTicksForForms for FB TS_END=" + firstBarOfLastBars.ts_end +
      " FB_DDATE=" + firstBarOfLastBars.dDate +
      " LB TS_END=" + lastBarOfLastBars.ts_end + " LB_DDATE=" + lastBarOfLastBars.dDate)
  }

  def logReadTicks(seqFormAllTinyTicks: Seq[tinyTick], sumReadDuration: Long): Unit = {
    logger.info(" Summart read ticks COUNT=[" + seqFormAllTinyTicks.size + "] SIZE =[" +
      SizeEstimator.estimate(seqFormAllTinyTicks) / 1024L / 1024L + "] Mb. Duration = " + sumReadDuration + "msec.")
  }

  def gedbugMinDdates(minDdateTsFromBForms: Option[(LocalDate, Long)], tickerId: Int,
                      barWidthSec: Int, barsFam: Seq[barsFaMeta]): Unit = {
    logger.debug("(3) minDdateFromBForms=" + minDdateTsFromBForms)
    logger.debug("Distinct ddates for (" + tickerId + "," + barWidthSec + ") SIZE=" +
      barsFam.count(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec))
  }

  def logSeqForms(seqForms: Seq[bForm]): Unit = {
    logger.info(" seqForms.ROWS=[" + seqForms.size + "] SIZE =[" +
      SizeEstimator.estimate(seqForms) / 1024L + "] Kb.")
  }

  /**
    * Read bars from mts_bars.bars_fa with iterations by (tickerId,barWidthSec)
    * internally by each ddate.
    * Next grouping bars (by resType and logOpenExit) for separated groups. We need eliminate neighboring bars.
    * And keep only last bars for each groups.
    *
    */
  def calcIteration(dbInst: DBImpl): List[List[Unit]] = {
    val barsFam: Seq[barsFaMeta] = dbInst.getAllBarsFAMeta
    logger.info("barsFam.size=" + barsFam.size + " Distinct(tickerID,BSW).size=" +
      barsFam.map(bh => (bh.tickerId, bh.barWidthSec)).distinct.size)

    barsFam.map(bFa => bFa.tickerId).distinct.toList.collect {
      case currentTickerId =>
        val lastBarsOfFormsAllTickers: Seq[(Int, barsResToSaveDB)] =
          barsFam.filter(bh => bh.tickerId == currentTickerId).map(bh => (bh.tickerId, bh.barWidthSec))
            .distinct.toList
            .flatMap {
              case (tickerId, barWidthSec) => firstLog(tickerId, barWidthSec)
                seqWays.flatMap {
                  wayType =>
                    val minDdateTsFromBForms: Option[(LocalDate, Long)] =
                      dbInst.getMinDdateBFroms(tickerId, barWidthSec, prcntsDiv, formDeepKoef, wayType)
                    gedbugMinDdates(minDdateTsFromBForms, tickerId, barWidthSec, barsFam)

                    //todo #1 Double reading same data for different wayType !!! Optimize it.
                    val allFABars = dbInst.getAllFaBars(
                      barsFam.filter(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec), minDdateTsFromBForms)
                    allFABarsDebugLog(tickerId, barWidthSec, allFABars)

                    prcntsDiv
                      .withFilter(thisPercent => allFABars.exists(b => b.log_oe == thisPercent && b.res_type == wayType))
                      .flatMap(
                        thisPercent =>
                          dbInst.filterFABars(allFABars.filter(b => b.log_oe == thisPercent && b.res_type == wayType),
                            intervalNewGroupKoeff)
                      )

                }
            }

        logger.debug("lastBarsOfFormsAllTickers ROWS=" + lastBarsOfFormsAllTickers.size + " SIZE OF WHOLE  = " +
          SizeEstimator.estimate(lastBarsOfFormsAllTickers) / 1024L + " Kb.")

        val (firstBarOfLastBars: barsResToSaveDB, lastBarOfLastBars: barsResToSaveDB) =
          lastBarsOfFormsAllTickers map (_._2) filter (_.tickerId == currentTickerId)
          match {
            case lastBars: List[barsResToSaveDB] => (lastBars.minBy(_.ts_end), lastBars.maxBy(_.ts_end))
          }

        logFirstLastBars(currentTickerId, firstBarOfLastBars, lastBarOfLastBars)
        val beforeReadTicks = System.currentTimeMillis

        val seqFormAllTinyTicks: Seq[tinyTick] = dbInst.getAllTicksForForms(currentTickerId,
          firstBarOfLastBars,
          lastBarOfLastBars
        )

        val afterReadTicks = System.currentTimeMillis
        logReadTicks(seqFormAllTinyTicks, afterReadTicks - beforeReadTicks)

        lastBarsOfFormsAllTickers.map(b => b._2)
          .filter(elm => elm.tickerId == currentTickerId)
          .map(fb => fb.barWidthSec)
          .distinct.toList
          .collect {
            case barWidthSec =>
              val seqForms: Seq[bForm] =
                lastBarsOfFormsAllTickers.filter(groupBars => groupBars._2.tickerId == currentTickerId &&
                  groupBars._2.barWidthSec == barWidthSec).collect {
                  case (_: Int, lb: barsResToSaveDB) =>
                    val seqFormTicks: Seq[tinyTick] = seqFormAllTinyTicks
                      .filter(t => t.db_tsunx >= (lb.ts_end - formDeepKoef * lb.barWidthSec * 1000L) &&
                        t.db_tsunx <= lb.ts_end)
                    bForm.create(lb, formDeepKoef, seqFormTicks)
                }
              logSeqForms(seqForms)
              dbInst.saveForms(seqForms.toList
                .filter(elm => elm.TsBegin != 0L &&
                  (elm.TsEnd - elm.TsBegin) / 1000L >= (elm.formDeepKoef - 1) * elm.barWidthSec))
          }
    }
  }

  def run(): Unit = {
    val dbInst: DBImpl = new DBCass(nodeAddress, "cassandra")
    val t1 = System.currentTimeMillis
    calcIteration(dbInst)
    val t2 = System.currentTimeMillis
    logger.info("Duration of FormsBuilder.run() - " + (t2 - t1) + " msecs.")
  }

}
