package bcpackage

import bcstruct.{bForm, barsFaMeta, barsResToSaveDB, tinyTick}
import com.madhukaraphatak.sizeof.SizeEstimator
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory
import com.datastax.driver.core.LocalDate

/*
import bcstruct.{bForm, barsFaData, barsFaMeta, tinyTick}
import com.madhukaraphatak.sizeof.SizeEstimator
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory
*/

class FormsBuilder(nodeAddress :String, prcntsDiv : Seq[Double], formDeepKoef :Int, intervalNewGroupKoeff :Int) {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val seqWays :Seq[String] = Seq("mx","mn")

  def  allFABarsDebugLog(tickerId :Int,barWidthSec :Int,allFABars : Seq[barsResToSaveDB] ) ={
    logger.info("allFABars for " + (tickerId,barWidthSec) + " SIZE " + allFABars.size + "  (" + allFABars.head.ts_end + ") " +
      " (" + allFABars.head.dDate + " - " + allFABars.last.dDate + ") SIZE=" + SizeEstimator.estimate(allFABars)/1024L/1024L + " Mb.")
  }

  def debugLastBarsOfGrp(lastBarsOfForms :Seq[(Int, barsResToSaveDB)]) ={
    lastBarsOfForms.collect {
      case (grpNum,bar) =>
        logger.info("Group=["+grpNum+"] Form_beginTS=["+(bar.ts_end - formDeepKoef*bar.barWidthSec*1000L)+"]  Bar = "+bar) // - some seconds can go into weekends.
    }
  }

  def firstLog(tickerId :Int,barWidthSec :Int) = {
    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
    logger.info("   ")
    logger.info(" FormBuilder iteration for [" + (tickerId, barWidthSec) + "] ")
    logger.info("   ")
    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
  }

  def logFirstLastBars(thisTickerID :Int, firstBarOfLastBars :barsResToSaveDB,lastBarOfLastBars :barsResToSaveDB) ={
    logger.info(" thisTickerID=" + thisTickerID + " getAllTicksForForms for FB TS_END=" + firstBarOfLastBars.ts_end + " FB_DDATE=" + firstBarOfLastBars.dDate +
      " LB TS_END=" + lastBarOfLastBars.ts_end + " LB_DDATE=" + lastBarOfLastBars.dDate)
  }

  /**
    * Read bars from mts_bars.bars_fa with iterations by (tickerId,barWidthSec)
    *  internally by each ddate.
    *  Next grouping bars (by resType and logOpenExit) for separated groups. We need eliminate neighboring bars. And keep only last bars for each groups.
    *
    */
  def calcIteration(dbInst :DBImpl) = {
   val barsFam :Seq[barsFaMeta] =  dbInst.getAllBarsFAMeta
    logger.info("barsFam.size="+barsFam.size+" Distinct(tickerID,BSW).size="+barsFam.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.size)

    val lastBarsOfFormsAllTickers :Seq[(Int, barsResToSaveDB)] =
    barsFam.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.toList
      .flatMap{
        case (tickerId,barWidthSec) =>
          firstLog(tickerId,barWidthSec)
          seqWays.flatMap{
            case wayType :String =>
          val minDdateTsFromBForms :Option[(LocalDate,Long)] = dbInst.getMinDdateBFroms(tickerId, barWidthSec, prcntsDiv, formDeepKoef, wayType)
          logger.info("(3) minDdateFromBForms="+minDdateTsFromBForms)
          logger.info("Distinct ddates for ("+tickerId+","+barWidthSec+") SIZE="+barsFam.count(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec))
          val allFABars: Seq[barsResToSaveDB] = dbInst.getAllFaBars(barsFam.filter(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec),minDdateTsFromBForms)
          allFABarsDebugLog(tickerId,barWidthSec,allFABars)
              prcntsDiv
                .withFilter(thisPercent => allFABars.exists(b => b.log_oe == thisPercent && b.res_type == wayType))
                .flatMap(
                thisPercent =>
                  dbInst.filterFABars(allFABars.filter(b => b.log_oe == thisPercent && b.res_type == wayType), intervalNewGroupKoeff)
                )
          }
    }

    //debugLastBarsOfGrp(lastBarsOfFormsAllTickers)
    logger.info(" -!!! --     lastBarsOfFormsAllTickers ROWS=" + lastBarsOfFormsAllTickers.size + " SIZE OF WHOLE  = " + SizeEstimator.estimate(lastBarsOfFormsAllTickers) / 1024L + " Kb.")

    lastBarsOfFormsAllTickers.map(b => b._2.tickerId).distinct.map {
      case thisTickerID =>

        /*
        val firstBarOfLastBars = lastBarsOfFormsAllTickers.map(b => b._2).filter(elm => elm.tickerId == thisTickerID).minBy(elm => elm.ts_end)
        val lastBarOfLastBars = lastBarsOfFormsAllTickers.map(b => b._2).filter(elm => elm.tickerId == thisTickerID).maxBy(elm => elm.ts_end)
        */
        val (firstBarOfLastBars :barsResToSaveDB,lastBarOfLastBars :barsResToSaveDB) = lastBarsOfFormsAllTickers map(_._2) filter(_.tickerId == thisTickerID) match {
          case lastBars :List[barsResToSaveDB] => (lastBars.minBy(_.ts_end), lastBars.maxBy(_.ts_end))
        }

        logFirstLastBars(thisTickerID, firstBarOfLastBars, lastBarOfLastBars)

        val seqFormAllTinyTicks: Seq[tinyTick] = dbInst.getAllTicksForForms(
          thisTickerID,
          firstBarOfLastBars.ts_end,
          lastBarOfLastBars.ts_end,
          firstBarOfLastBars.dDate,
          lastBarOfLastBars.dDate)

        logger.info(" seqFormAllTinyTicks.ROWS=[" + seqFormAllTinyTicks.size + "] SIZE =[" + SizeEstimator.estimate(seqFormAllTinyTicks) / 1024L / 1024L + "] Mb.")

        lastBarsOfFormsAllTickers.map(b => b._2)
          .filter(elm => elm.tickerId == thisTickerID)
          .map(fb =>  fb.barWidthSec)
          .distinct.toList
          .collect {
            case  barWidthSec =>
              val seqForms: Seq[bForm] =
                lastBarsOfFormsAllTickers.filter(groupBars => groupBars._2.tickerId == thisTickerID &&
                                                              groupBars._2.barWidthSec == barWidthSec).collect {
                  case (grpNum: Int, lb: barsResToSaveDB) =>
                    val seqFormTicks: Seq[tinyTick] = seqFormAllTinyTicks.filter(t => t.db_tsunx >= (lb.ts_end - formDeepKoef * lb.barWidthSec * 1000L) && t.db_tsunx <= lb.ts_end)
                    bForm.create(lb, formDeepKoef, seqFormTicks)
                }

              // TODO: Convert to Function
              logger.info(" tickerId = " + thisTickerID + " barWidthSec = " + barWidthSec + " >  getAllTicksForForms "+
                " seqForms.ROWS=[" + seqForms.size + "] SIZE =[" + SizeEstimator.estimate(seqForms) / 1024L + "] Kb.")
              dbInst.saveForms(seqForms.toList.filter(elm => elm.TsBegin != 0L && (elm.TsEnd - elm.TsBegin) / 1000L >= (elm.formDeepKoef - 1) * elm.barWidthSec))
          }

    }
  }


  def run = {
    val dbInst: DBImpl = new DBCass(nodeAddress, "cassandra")
    val t1 = System.currentTimeMillis
    calcIteration(dbInst)
    val t2 = System.currentTimeMillis
    logger.info("Duration of FormsBuilder.run() - "+(t2 - t1) + " msecs.")
  }


}
