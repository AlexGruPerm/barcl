package bcstruct

import com.datastax.driver.core.LocalDate


/**
  * Meta Information about which ticker and which deeps will be calculated.
  *
  */
case class CalcProperty(tickerId      :Int,
                        barDeepSec    :Int,
                        isEnabled     :Int,
                        //-----------------
                        dDateLastBar  :Option[LocalDate/*java.util.Date*/],
                        tsEndLastBar  :Option[Long],
                        //-----------------
                        dDateLastTick :Option[LocalDate/*java.util.Date*/],
                        tsLastTick    :Option[Long]
                       )

/**
  * Contains all CalcProperty(s)
  */
case class CalcProperties(cProps :Seq[CalcProperty]){
}

/**
  * Last bar for key: ticker_id, barDeepSec
  */
case class LastBar(dDate      :LocalDate/*java.util.Date*/,
                   tsEnd      :Long)

/**
  * Contains all LastBar(s)
  */
case class LastBars(lBars :Seq[LastBar])