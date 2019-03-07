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
                        dDateBeginLastBar  :Option[LocalDate],
                        tsBeginLastBar     :Option[Long],
                        dDateEndLastBar    :Option[LocalDate],
                        tsEndLastBar       :Option[Long],
                        //-----------------
                        dDateLastTick :Option[LocalDate],
                        tsLastTick    :Option[Long],
                        //-----------------
                        tsFirstTicks  :Option[Long]
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

  val beginFrom = tsEndLastBar.getOrElse(
    tsFirstTicks.getOrElse(
      tsLastTick.getOrElse(0L)))

  val diffLastTickTSBarTS = tsLastTick.getOrElse(0L)/1000L - beginFrom/1000L

}

/**
  * Contains all CalcProperty(s)
  */
case class CalcProperties(cProps :Seq[CalcProperty]){
}

/**
  * Last bar for key: ticker_id, barDeepSec
  */
case class LastBar(dDateBegin  :LocalDate,
                   tsBegin     :Long,
                   dDateEnd    :LocalDate,
                   tsEnd       :Long)

/**
  * Contains all LastBar(s)
  */
case class LastBars(lBars :Seq[LastBar])

case class Tick(
                 tickerId  :Int,
                 dDate     :LocalDate,
                 db_tsunx  :Long,
                 ask       :Double,
                 bid       :Double
               )

case class seqTicksObj(
                    sqTicks :Seq[Tick]
                   )

class Bar (p_ticker_id : Int, p_bar_width_sec : Int, barTicks : Seq[Tick]) {

  def simpleRound4Double(valueD : Double) = {
    (valueD * 10000).round / 10000.toDouble
  }

  def simpleRound5Double(valueD : Double) = {
    (valueD * 100000).round / 100000.toDouble
  }

  def simpleRound6Double(valueD : Double) = {
    (valueD * 1000000).round / 1000000.toDouble
  }

  val ticker_id       :Int = p_ticker_id
  val ddate           :Long = barTicks(0).db_tsunx
  val bar_width_sec   :Int= p_bar_width_sec
  val ts_begin        :Long = barTicks(0).db_tsunx
  val ts_end          :Long = barTicks.last.db_tsunx
  val o               :Double = simpleRound5Double((barTicks(0).ask + barTicks(0).bid)/2)
  val h               :Double = simpleRound5Double(barTicks.map(x => (x.ask+x.bid)/2).max)
  val l               :Double = simpleRound5Double(barTicks.map(x => (x.ask+x.bid)/2).min)
  val c               :Double = simpleRound5Double((barTicks.last.ask + barTicks.last.bid)/2)
  val h_body          :Double = simpleRound5Double(math.abs(c-o))
  val h_shad          :Double = simpleRound5Double(math.abs(h-l))
  val btype           :String =(o compare c).signum match {
    case -1 => "g" // bOpen < bClose
    case  0 => "n" // bOpen = bClose
    case  1 => "r" // bOpen > bClose
  }
  val ticks_cnt       :Int = if (barTicks.nonEmpty) barTicks.size else 0
  //standard deviation
  val disp            :Double =  if (ticks_cnt != 0)
    simpleRound6Double(
      Math.sqrt(
        barTicks.map(x => Math.pow((x.ask+x.bid)/2,2)).sum/ticks_cnt-
          Math.pow( barTicks.map(x => (x.ask+x.bid)/2).sum/ticks_cnt ,2)
      )
    )
  else 0
  val log_co          :Double = simpleRound5Double(Math.log(c)-Math.log(o))

  override def toString =
    "[ "+ts_begin+":"+ts_end+"] ohlc=["+o+","+h+","+l+","+c+"] "+btype+"   body,shad=["+h_body+","+h_shad+"]"

}