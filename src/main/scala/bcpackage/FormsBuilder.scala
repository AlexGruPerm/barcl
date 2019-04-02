package bcpackage

import bcstruct.{bForm, barsFaData, barsFaMeta, tinyTick}
import com.madhukaraphatak.sizeof.SizeEstimator
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

class FormsBuilder(nodeAddress :String, prcntsDiv : Seq[Double], formDeepKoef :Int, intervalNewGroupKoeff :Int) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def  allFABarsDebugLog(tickerId :Int,barWidthSec :Int,allFABars : Seq[barsFaData] ) ={
    logger.debug("allFABars for " + (tickerId,barWidthSec) + " SIZE " + allFABars.size + "  (" + allFABars.head.TsEnd + ") " +
      " (" + allFABars.head.dDate + " - " + allFABars.last.dDate + ") SIZE=" + SizeEstimator.estimate(allFABars)/1024L/1024L + " Mb.")
  }

  def debugLastBarsOfGrp(lastBarsOfForms :Seq[(Int, barsFaData)]) ={
    lastBarsOfForms.collect {
      case (grpNum: Int, bar: barsFaData) =>
        logger.debug("Group=["+grpNum+"] Form_beginTS=["+(bar.TsEnd - formDeepKoef*bar.barWidthSec*1000L)+"]  Bar = "+bar)
    }
  }


  def calcIteration(dbInst :DBImpl) = {
   val barsFaMeta :Seq[barsFaMeta] =  dbInst.getAllBarsFAMeta
    logger.debug("barsFaMeta.size="+barsFaMeta.size)
    /**
      * Read bars from mts_bars.bars_fa with iterations by (tickerId,barWidthSec)
      *  internally by each ddate.
      *  Next grouping bars (by resType and prcntsDiv) for separated groups. We need eliminate neighboring bars. And keep only last bars for each groups.
      *
    */
    barsFaMeta.map(bh => (bh.tickerId,bh.barWidthSec)).distinct
      .collect {
        case (tickerId,barWidthSec) =>
          val allFABars: Seq[barsFaData] = dbInst.getAllFaBars(barsFaMeta.filter(r => r.tickerId == tickerId && r.barWidthSec == barWidthSec))
          allFABarsDebugLog(tickerId,barWidthSec,allFABars)

          val lastBarsOfForms :Seq[(Int, barsFaData)] = Seq("mx","mn")
            .flatMap(resType =>
              prcntsDiv.flatMap(
               thisPercent => dbInst.filterFABars(allFABars.filter(b => b.prcnt == thisPercent && b.resType == resType), intervalNewGroupKoeff)
            )
          )
          //debugLastBarsOfGrp(lastBarsOfForms)
          logger.info("lastBarsOfForms ROWS="+lastBarsOfForms.size+" SIZE OF WHOLE  = "+ SizeEstimator.estimate(lastBarsOfForms)/1024L  +" Kb.")

          val firstBarOfLastBars :barsFaData = lastBarsOfForms.map(b => b._2).head
          val lastBarOfLastBars :barsFaData = lastBarsOfForms.map(b => b._2).last

          logger.debug("FirstBarOfForms TS_END="+firstBarOfLastBars.TsEnd+"    LastBarOfForms TS_END="+lastBarOfLastBars.TsEnd)

          val seqFormAllTinyTicks :Seq[tinyTick] = dbInst.getAllTicksForForms(
            tickerId,
            firstBarOfLastBars.TsEnd,
            lastBarOfLastBars.TsEnd,
            firstBarOfLastBars.dDate,
            lastBarOfLastBars.dDate)

          logger.debug(" =[1]======= seqFormAllTinyTicks.ROWS=["+ seqFormAllTinyTicks.size +"] SIZE =["+ SizeEstimator.estimate(seqFormAllTinyTicks)/1024L/1024L +"] Mb. ========")


          val seqForms : Seq[bForm] =
         lastBarsOfForms.collect {
           case (grpNum: Int, lb: barsFaData) =>
              val seqFormTicks :Seq[tinyTick] = seqFormAllTinyTicks.filter(t => t.db_tsunx >= (lb.TsEnd - formDeepKoef*lb.barWidthSec*1000L) && t.db_tsunx <= lb.TsEnd)

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
