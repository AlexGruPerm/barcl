package bcpackage

import bcstruct.{barsForFutAnalyze, barsFutAnalyzeRes, barsMeta}
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


       // val futAnalRes :Seq[Seq[barsFutAnalyzeOneSearchRes]] = for (p<-seqPrcnts) yield dbInst.makeAnalyze(allBars,p)

        val futAnalRes :Seq[barsFutAnalyzeRes] = dbInst.makeAnalyze(allBars,0.438/*0.873*/)
        logger.debug("After research seq of RES = "+futAnalRes.size)

        logger.debug("=======================================================")


        for(b <- futAnalRes) {
          logger.debug("("+b.srcBar.ts_end+" - "+b.srcBar.c+")"+"  -  "+b.resAnal)
        }


        //logger.debug("curr Example = "+futAnalRes.toSeq.filter(b => b.srcBar.ts_end == 1550477310535L).map(e => e.srcBar).head)
        logger.debug("=======================================================")

    }
  }


  def run = {
    /**
      * dbSess hides DB query execution logic and converting data sets into seq of scala objects.
      * Lets us get necessary structures of data.
      */
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
