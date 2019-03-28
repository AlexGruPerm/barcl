package bcpackage

import db.{DBCass, DBImpl}
import org.slf4j.LoggerFactory

class FormsBuilder(nodeAddress :String) {
  val logger = LoggerFactory.getLogger(getClass.getName)

  def calcIteration(dbInst :DBImpl) = {

  }

  def run = {
    val dbInst: DBImpl = new DBCass(nodeAddress, "cassandra")
    val t1 = System.currentTimeMillis
    calcIteration(dbInst)
    val t2 = System.currentTimeMillis
    logger.info("Duration of FormsBuilder.run() - "+(t2 - t1) + " msecs.")
  }


}
