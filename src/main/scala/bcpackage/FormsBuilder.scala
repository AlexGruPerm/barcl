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

  def  allFABarsDebugLog(tickerId :Int,barWidthSec :Int,allFABars : Seq[barsResToSaveDB] ) ={
    logger.debug("allFABars for " + (tickerId,barWidthSec) + " SIZE " + allFABars.size + "  (" + allFABars.head.ts_end + ") " +
      " (" + allFABars.head.dDate + " - " + allFABars.last.dDate + ") SIZE=" + SizeEstimator.estimate(allFABars)/1024L/1024L + " Mb.")
  }

  def debugLastBarsOfGrp(lastBarsOfForms :Seq[(Int, barsResToSaveDB)]) ={
    lastBarsOfForms.collect {
      case (grpNum,bar) =>
        logger.debug("Group=["+grpNum+"] Form_beginTS=["+(bar.ts_end - formDeepKoef*bar.barWidthSec*1000L)+"]  Bar = "+bar)
    }
  }


  def calcIteration(dbInst :DBImpl) = {
   val barsFam :Seq[barsFaMeta] =  dbInst.getAllBarsFAMeta
    logger.debug("barsFam.size="+barsFam.size+" Distinct(tickerID,BSW).size="+barsFam.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.size)
    /**
      * Read bars from mts_bars.bars_fa with iterations by (tickerId,barWidthSec)
      *  internally by each ddate.
      *  Next grouping bars (by resType and logOpenExit) for separated groups. We need eliminate neighboring bars. And keep only last bars for each groups.
      *
    */
    /*
    logger.debug("barsFam.size = "+barsFam.size)
    logger.debug("barsFaMeta.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.size = "+barsFam.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.size)
   */

    barsFam.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.toList
      .collect {
        case (tickerId,barWidthSec) =>
          logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
          logger.debug("   ")
          logger.debug(" FormBuilder iteration for ["+(tickerId,barWidthSec)+"] ")
          logger.debug("   ")
          logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")

          val minDdateTsFromBForms :Option[(LocalDate,Long)] = dbInst.getMinDdateBFroms(tickerId, barWidthSec, prcntsDiv, formDeepKoef)
          logger.debug("(3) minDdateFromBForms="+minDdateTsFromBForms)

          logger.debug("Distinct ddates for ("+tickerId+","+barWidthSec+") SIZE="+barsFam.filter(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec).size)

          val allFABars: Seq[barsResToSaveDB] = dbInst.getAllFaBars(barsFam.filter(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec),minDdateTsFromBForms)
          allFABarsDebugLog(tickerId,barWidthSec,allFABars)

          val lastBarsOfForms :Seq[(Int, barsResToSaveDB)] = Seq("mx","mn")
            .flatMap(resType =>
              prcntsDiv
                .withFilter(thisPercent => allFABars.exists(b => b.log_oe == thisPercent && b.res_type == resType))
                .flatMap(
                thisPercent =>
                  dbInst.filterFABars(allFABars.filter(b => b.log_oe == thisPercent && b.res_type == resType), intervalNewGroupKoeff)
              )
            )

          // на вход dbInst.filterFABars приходит пустой Seq и падает алгоритм., не передавтаь пустые Seq.

          //debugLastBarsOfGrp(lastBarsOfForms)
          logger.info(" -!!! --     lastBarsOfForms ROWS="+lastBarsOfForms.size+" SIZE OF WHOLE  = "+ SizeEstimator.estimate(lastBarsOfForms)/1024L  +" Kb.")

          val firstBarOfLastBars  = lastBarsOfForms.map(b => b._2).head
          val lastBarOfLastBars  = lastBarsOfForms.map(b => b._2).last
/*
          logger.debug("FB TS_END="+firstBarOfLastBars.TsEnd+" FB_DDATE="+firstBarOfLastBars.dDate+
                       " LB TS_END="+lastBarOfLastBars.TsEnd+" LB_DDATE="+lastBarOfLastBars.dDate)
*/
          val seqFormAllTinyTicks :Seq[tinyTick] = dbInst.getAllTicksForForms(
            tickerId,
            firstBarOfLastBars.ts_end,
            lastBarOfLastBars.ts_end,
            firstBarOfLastBars.dDate,
            lastBarOfLastBars.dDate)

          logger.debug(" =[1]======= seqFormAllTinyTicks.ROWS=["+ seqFormAllTinyTicks.size +"] SIZE =["+ SizeEstimator.estimate(seqFormAllTinyTicks)/1024L/1024L +"] Mb. ========")

          val seqForms : Seq[bForm] =
         lastBarsOfForms.collect {
           case (grpNum: Int, lb: barsResToSaveDB) =>
              val seqFormTicks :Seq[tinyTick] = seqFormAllTinyTicks.filter(t => t.db_tsunx >= (lb.ts_end - formDeepKoef*lb.barWidthSec*1000L) && t.db_tsunx <= lb.ts_end)

             /*
             logger.info(">>>  tickerId="+tickerId+" group="+grpNum+" ts_begin="+(lb.TsEnd - formDeepKoef*lb.barWidthSec*1000L)+
               " tsEnd="+lb.TsEnd+" seqFormTicks.ROWS = "+
               seqFormTicks.size+" SIZE = "+ SizeEstimator.estimate(seqFormTicks)/1024L +" Kb.")
             */
              bForm.create(lb,formDeepKoef,seqFormTicks)
          }
          logger.debug(" =[2]======= seqForms.ROWS=["+ seqForms.size +"] SIZE =["+ SizeEstimator.estimate(seqForms)/1024L +"] Kb. ========")

          dbInst.saveForms(seqForms)

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
