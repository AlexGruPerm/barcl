package bcpackage


/**
  * Made as separate class for future. It is planning to add new members.
  * @param cProps
  */
case class CalcProperties(cProps :Seq[CalcProperty]){

}

/**
  * Meta Information about which ticker and which deeps will be calculated.
  *
  */
case class CalcProperty(tickerId    :Int,
                        barDeepSec  :Int,
                        isEnabled   :Int)

