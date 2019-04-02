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
case class LastBar(//dDateBegin  :LocalDate,
                   //tsBegin     :Long,
                   //dDateEnd    :LocalDate,
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
  val ddate           :Long =  barTicks.last.db_tsunx// barTicks(0).db_tsunx
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

case class barsMeta(
                     tickerId    :Int,
                     barWidthSec :Int,
                     dDate       :LocalDate,
                     lastTsEnd   :Long
                   )

case class barsForFutAnalyze(
                              tickerId    :Int,
                              barWidthSec :Int,
                              dDate       :LocalDate,
                              ts_begin    :Long,
                              ts_end      :Long,
                              o           :Double,
                              h           :Double,
                              l           :Double,
                              c           :Double
                            ){
  val minOHLC = Seq(o,h,l,c).min
  val maxOHLC = Seq(o,h,l,c).max
}

/**
  * Seq(
  * ("prcnt_5",{res, ts_exit, ..., ... }),
  * ("prcnt_10",{})
  * )
  * //resType mn - Down, mx - Up, bt- Both, nn- Not found.
*/
case class barsFutAnalyzeRes(
                             srcBar   : barsForFutAnalyze,
                             resAnal  : Option[barsForFutAnalyze],
                             p        : Double,
                             resType  : String
                            )

/**
  * Take on input seq only for same ts_end
  * -----
  * (1549268939406 - 1.14457)  -  Some(barsForFutAnalyze(1,3600,2019-02-06,1549430920383,1549434507567,1.14005,1.14012,1.13922,1.13943)) p=0.438
  * (1549268939406 - 1.14457)  -  Some(barsForFutAnalyze(1,3600,2019-02-07,1549528110345,1549531709700,1.13498,1.13571,1.13439,1.13453)) p=0.873
  * -----
  * (1549272539680 - 1.14452)  -  Some(barsForFutAnalyze(1,3600,2019-02-06,1549430920383,1549434507567,1.14005,1.14012,1.13922,1.13943)) p=0.438
  * (1549272539680 - 1.14452)  -  Some(barsForFutAnalyze(1,3600,2019-02-07,1549528110345,1549531709700,1.13498,1.13571,1.13439,1.13453)) p=0.873
  * -----
*/


case class barsResToSaveDB(
                            tickerId      :Int,
                            barWidthSec   :Int,
                            dDate         :LocalDate,
                            ts_end        :Long,
                            prcnt         :Double,
                            res_type      :String,
                            res           :Map[String,String]
                          )

case class barsFaMeta(
                     tickerId    :Int,
                     barWidthSec :Int,
                     dDate       :LocalDate,
                     lastTsEnd   :Long
                    )

case class  barsFaData(
                       tickerId    :Int,
                       barWidthSec :Int,
                       dDate       :LocalDate,
                       TsEnd       :Long,
                       prcnt       :Double,
                       resType     :String,
                       res         :Map[String,String]
                     )

case class tinyTick(
                     db_tsunx  :Long,
                     ask       :Double,
                     bid       :Double
                   )

class bForm private(
            barFa        :barsFaData,
            formDeepKoef :Int,
            ticksCnt     :Int
           ){
}

/**
  * We use factory method - create to make all calculation and for eleminate stogin big object -
  * seqTicks inside bForm instances.
*/
object bForm {
  def create(barFa        :barsFaData,
             formDeepKoef :Int,
             seqTicks     :Seq[tinyTick]):bForm = {
    val ticksCnt = seqTicks.size
      new bForm(barFa,formDeepKoef,ticksCnt)
  }
}
