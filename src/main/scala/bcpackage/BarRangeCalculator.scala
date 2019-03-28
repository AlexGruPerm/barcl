package bcpackage

import bcstruct.{barsForFutAnalyze, barsFutAnalyzeRes, barsMeta, barsResToSaveDB}
import com.madhukaraphatak.sizeof.SizeEstimator
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

class BarRangeCalculator(nodeAddress :String, seqPrcnts: Seq[Int]) {
  val logger = LoggerFactory.getLogger(getClass.getName)


  def calcIteration(dbInst :DBImpl) = {
    val allBarsHistMeta :Seq[barsMeta] = dbInst.getAllBarsHistMeta
    allBarsHistMeta.map(bh => (bh.tickerId,bh.barWidthSec)).distinct.foreach {
      bhm =>
        val allBars: Seq[barsForFutAnalyze] = dbInst.getAllCalcedBars(allBarsHistMeta.filter(r => r.tickerId==bhm._1 && r.barWidthSec==bhm._2))
        logger.debug("allBars BY ["+bhm._1+","+bhm._2+"] SIZE = " + allBars.size+"  ("+allBars.head.ts_end+" - "+allBars.last.ts_end+") "+
                                                               " (" + allBars.head.dDate+" - "+allBars.last.dDate+") SIZE="+SizeEstimator.estimate(dbInst)+" bytes.")

        val prcntsDiv = Seq(0.219,0.437,0.873)
        val t1FAnal = System.currentTimeMillis
        val futAnalRes :Seq[barsFutAnalyzeRes] = prcntsDiv.flatMap(p => dbInst.makeAnalyze(allBars,p))
        val t2FAnal = System.currentTimeMillis
        logger.debug("After analyze RES.size = "+futAnalRes.size+" Duration "+(t2FAnal-t1FAnal)+" msecs.")

          val t1FS = System.currentTimeMillis
          val resFSave :Seq[barsResToSaveDB] = futAnalRes.map(b => b.srcBar.ts_end).distinct.map(
            thisTsEnd => new barsResToSaveDB(futAnalRes.filter(ab => ab.srcBar.ts_end == thisTsEnd)))
          val t2FS = System.currentTimeMillis
         logger.info("Duration of gathering resFSave - "+(t2FS - t1FS) + " msecs.")

        /*
      logger.debug("=======================================================")
      for(sr <- resFSave) {
        logger.debug(sr.currB.toString)
         for(r <- sr.futBarsRes){
           logger.debug("   "+r.toString())
         }
        logger.debug("      ")
      }
      logger.debug("=======================================================")
       */
        val t1Save = System.currentTimeMillis
        dbInst.saveBarsFutAnal(resFSave)
        val t2Save = System.currentTimeMillis
        logger.info("Duration of saveing into mts_bars.bars_fa - "+(t2Save - t1Save) + " msecs.")
        //logger.debug("==========================================================")
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
