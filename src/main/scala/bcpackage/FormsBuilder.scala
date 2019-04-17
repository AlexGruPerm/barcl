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
    logger.debug("allFABars for " + (tickerId,barWidthSec) + " SIZE " + allFABars.size + "  (" + allFABars.head.ts_end + ") " +
      " (" + allFABars.head.dDate + " - " + allFABars.last.dDate + ") SIZE=" + SizeEstimator.estimate(allFABars)/1024L/1024L + " Mb.")
  }

  def debugLastBarsOfGrp(lastBarsOfForms :Seq[(Int, barsResToSaveDB)]) ={
    lastBarsOfForms.collect {
      case (grpNum,bar) =>
        logger.debug("Group=["+grpNum+"] Form_beginTS=["+(bar.ts_end - formDeepKoef*bar.barWidthSec*1000L)+"]  Bar = "+bar) // - some seconds can go into weekends.
    }
  }

  def firstLog(tickerId :Int,barWidthSec :Int) = {
    logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
    logger.debug("   ")
    logger.debug(" FormBuilder iteration for [" + (tickerId, barWidthSec) + "] ")
    logger.debug("   ")
    logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
  }

  /**
    * Read bars from mts_bars.bars_fa with iterations by (tickerId,barWidthSec)
    *  internally by each ddate.
    *  Next grouping bars (by resType and logOpenExit) for separated groups. We need eliminate neighboring bars. And keep only last bars for each groups.
    *
    */
  def calcIteration(dbInst :DBImpl) = {
   val barsFam :Seq[barsFaMeta] =  dbInst.getAllBarsFAMeta
    logger.debug("barsFam.size="+barsFam.size+" Distinct(tickerID,BSW).size="+barsFam.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.size)


    val lastBarsOfFormsAllTickers :Seq[(Int, barsResToSaveDB)] =
    barsFam.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.toList
      .flatMap{
        case (tickerId,barWidthSec) =>
          firstLog(tickerId,barWidthSec)
          seqWays.flatMap{
            case wayType :String =>
          val minDdateTsFromBForms :Option[(LocalDate,Long)] = dbInst.getMinDdateBFroms(tickerId, barWidthSec, prcntsDiv, formDeepKoef, wayType)
          logger.debug("(3) minDdateFromBForms="+minDdateTsFromBForms)
          logger.debug("Distinct ddates for ("+tickerId+","+barWidthSec+") SIZE="+barsFam.count(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec))
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

    //>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    //next code we must run by separated ticker_id in "loop"
    //and not use lastBarsOfFormsAllTickers inside

    lastBarsOfFormsAllTickers.map(b => b._2.tickerId).distinct.map {
      case thisTickerID =>

        val firstBarOfLastBars = lastBarsOfFormsAllTickers.map(b => b._2).filter(elm => elm.tickerId==thisTickerID).minBy(elm => elm.ts_end)
        val lastBarOfLastBars = lastBarsOfFormsAllTickers.map(b => b._2).filter(elm => elm.tickerId==thisTickerID).maxBy(elm => elm.ts_end)

        logger.debug(" thisTickerID="+thisTickerID+" getAllTicksForForms for FB TS_END=" + firstBarOfLastBars.ts_end + " FB_DDATE=" + firstBarOfLastBars.dDate +
          " LB TS_END=" + lastBarOfLastBars.ts_end + " LB_DDATE=" + lastBarOfLastBars.dDate)

        //lastBarsOfFormsAllTickers.map(b => (b._2.tickerId, b._2.barWidthSec)).distinct.toList
        lastBarsOfFormsAllTickers.map(b => b._2).filter(elm => elm.tickerId == thisTickerID).map(fb => (fb.tickerId, fb.barWidthSec)).distinct.toList
          .collect {
            case (tickerId, barWidthSec) =>

              //single read for each ddate for this tickerID. !!!!!!!!!!!!!!!!!!
              val seqFormAllTinyTicks: Seq[tinyTick] = dbInst.getAllTicksForForms(
                tickerId,
                firstBarOfLastBars.ts_end,
                lastBarOfLastBars.ts_end,
                firstBarOfLastBars.dDate,
                lastBarOfLastBars.dDate)

              logger.debug(" =[1]======= seqFormAllTinyTicks.ROWS=[" + seqFormAllTinyTicks.size + "] SIZE =[" + SizeEstimator.estimate(seqFormAllTinyTicks) / 1024L / 1024L + "] Mb. ========")

              val seqForms: Seq[bForm] =
                lastBarsOfFormsAllTickers.filter(groupBars => groupBars._2.tickerId == thisTickerID).collect {
                  case (grpNum: Int, lb: barsResToSaveDB) =>
                    val seqFormTicks: Seq[tinyTick] = seqFormAllTinyTicks.filter(t => t.db_tsunx >= (lb.ts_end - formDeepKoef * lb.barWidthSec * 1000L) && t.db_tsunx <= lb.ts_end)
                    bForm.create(lb, formDeepKoef, seqFormTicks)
                }
              logger.debug(" =[2]======= seqForms.ROWS=[" + seqForms.size + "] SIZE =[" + SizeEstimator.estimate(seqForms) / 1024L + "] Kb. ========")
              dbInst.saveForms(seqForms.toList.filter(elm => (elm.TsBegin != 0L && (elm.TsEnd - elm.TsBegin) / 1000L >= (elm.formDeepKoef - 1) * elm.barWidthSec)))

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
