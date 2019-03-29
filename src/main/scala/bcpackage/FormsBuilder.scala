package bcpackage

import bcstruct.{barsFaData, barsFaMeta}
import com.madhukaraphatak.sizeof.SizeEstimator
import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

class FormsBuilder(nodeAddress :String) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def calcIteration(dbInst :DBImpl) = {
   val barsFaMeta :Seq[barsFaMeta] =  dbInst.getAllBarsFAMeta
    logger.debug("barsFaMeta.size="+barsFaMeta.size)
    barsFaMeta.map(bh => (bh.tickerId,bh.barWidthSec)).distinct
      .foreach {
        bfm =>
          logger.debug("   ")
          logger.debug("=============================================================================================")
          val allFABars: Seq[barsFaData] = dbInst.getAllFaBars(barsFaMeta.filter(r => r.tickerId == bfm._1 && r.barWidthSec == bfm._2))
          logger.debug("allFABars for " + bfm + " SIZE " + allFABars.size + "  (" + allFABars.head.TsEnd + ") " +
           " (" + allFABars.head.dDate + " - " + allFABars.last.dDate + ") SIZE=" + SizeEstimator.estimate(allFABars)/1024L/1024L + " Mb.")


          for (b <- allFABars){
            println(b)
          }


         // val faMx0219 :Seq[barsFaData] = dbInst.getFaBarsFiltered(allFABars, "mx",0.219,10000)


          val faBarsMX: Seq[barsFaData] = allFABars.filter(b => (b.resInterval=="res_0_219" && b.resType=="mx"))

          logger.debug("faBarsMX.size = "+faBarsMX.size)


          val faBarsGrpMx :Seq[(Int,barsFaData)]  = dbInst.filterFABars(faBarsMX,10000 /* groups interval 10 min*/)

          logger.debug("--------------- faBarsGrpMx.size = "+faBarsGrpMx.size)

          for(elm <- faBarsGrpMx){
            println(elm._2.TsEnd+" - "+elm._2.res)
          }



          logger.debug("=============================================================================================")
          logger.debug("   ")
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
