package bcstruct

import com.datastax.driver.core.LocalDate


/**
  * Meta Information about which ticker and which deeps will be calculated.
  * And all information about last bar and first/last ticks.
  */
case class CalcProperty(tickerId      :Int,
                        barDeepSec    :Int,
                        isEnabled     :Int,
                        //-----------------
                        dDateLastBar  :Option[LocalDate],
                        tsEndLastBar  :Option[Long],
                        //-----------------
                        dDateLastTick :Option[LocalDate],
                        tsLastTick    :Option[Long],
                        //-----------------
                        tsBeginTicks : Option[Long]
                       ){
  /*
  REWRITE IT WITH getOrElse(0L)

  require({tsEndLastBar match {
    case Some(thisTsEndLastBar) => {
      tsLastTick match {
        case Some(thisTsLastTick) => thisTsEndLastBar <= thisTsLastTick
        case _ => true
      }
    }
    case _ =>  {
      tsLastTick match {
        case Some(thisTsLastTick) => 0L <= thisTsLastTick
        case _ => true
    }}
  }
  },"TS End last bar must be less than TS End last tick.")
  */

  val diffLastTickTSBarTS = tsLastTick.getOrElse(0L)/1000L -
    tsEndLastBar.getOrElse(
      tsBeginTicks.getOrElse(
        tsLastTick.getOrElse(0L)))/1000L



}

/**
  * Contains all CalcProperty(s)
  */
case class CalcProperties(cProps :Seq[CalcProperty]){
}

/**
  * Last bar for key: ticker_id, barDeepSec
  */
case class LastBar(dDate      :LocalDate,
                   tsEnd      :Long)

/**
  * Contains all LastBar(s)
  */
case class LastBars(lBars :Seq[LastBar])