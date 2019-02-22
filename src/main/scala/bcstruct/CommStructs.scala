package bcstruct


/**
  * Meta Information about which ticker and which deeps will be calculated.
  *
  */
case class CalcProperty(tickerId   :Int,
                        barDeepSec :Int,
                        isEnabled  :Int,
                        dDate      :Option[java.util.Date],
                        tsEnd      :Option[Long]
                       )

/**
  * Contains all CalcProperty(s)
  */
case class CalcProperties(cProps :Seq[CalcProperty]){
}

/**
  * Last bar for key: ticker_id, barDeepSec
  */
case class LastBar(dDate      :java.util.Date,
                   tsEnd      :Long)

/**
  * Contains all LastBar(s)
  */
case class LastBars(lBars :Seq[LastBar])
