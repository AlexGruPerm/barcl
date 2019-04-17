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
                     dDate       :LocalDate
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
                            tickerId     :Int,
                            dDate        :LocalDate,
                            barWidthSec  :Int,
                            ts_end       :Long,
                            c            :Double,
                            log_oe       :Double,
                            ts_end_res   :Long,
                            dursec_res   :Int,
                            ddate_res    :LocalDate,
                            c_res        :Double,
                            res_type     :String
                          )

case class barsFaMeta(
                     tickerId    :Int,
                     barWidthSec :Int,
                     dDate       :LocalDate,
                     lastTsEnd   :Long
                    )
/*
case class  barsFaData(
                       tickerId    :Int,
                       barWidthSec :Int,
                       dDate       :LocalDate,
                       TsEnd       :Long,
                       prcnt       :Double,
                       resType     :String,
                       res         :Map[String,String]
                     )
*/

case class tinyTick(
                     db_tsunx  :Long,
                     ask       :Double,
                     bid       :Double
                   )

case class   bForm(
                    tickerId     :Int,
                    barWidthSec  :Int,
                    dDate        :LocalDate,
                    TsBegin      :Long,
                    TsEnd        :Long,
                    log_oe       :Double,
                    resType      :String,
                    //res          :Map[String,String],
                    ts_end_res     :Long,
                    dursec_res     :Int,
                    ddate_res      :LocalDate,
                    c_res          :Double,
                    formDeepKoef :Int,
                    FormProps    :Map[String,String]
                  )

/**
  * We use factory method - create to make all calculation and for eleminate stogin big object -
  * seqTicks inside bForm instances.
*/
object bForm {
  /**
    * Return price where VSA profile has maximum = PEAK
    * calculated by price frequency
  */
  def getMaxRo(seqTicks :Seq[tinyTick]) :Double = {
    val formCMin = seqTicks.map(_.ask).min          // total min price
    val formCMax = seqTicks.map(_.ask).max          // total max price
    val n = 10
    val rngCStep = (formCMax-formCMin)/n

    val rngC = formCMin.to(formCMax).by(rngCStep)
    val rangesC = rngC.zip(rngC.tail)

    val rangeFreq :Seq[(Double,Int)] = rangesC.map(rng =>
      (rng._1, seqTicks.count(t => t.ask >= rng._1 && t.ask <= rng._2)))

    rangeFreq.maxBy(r => r._2)._1
  }

  def create(barFa        :barsResToSaveDB,//barsFaData,
             formDeepKoef :Int,
             seqTicks     :Seq[tinyTick]) :bForm = {
    val ticksCnt: Int = seqTicks.size
    val tsBegin = seqTicks.size match {
      case 0 => 0L
      case _ => seqTicks.map(t => t.db_tsunx).min
    }

    /**
      * Calculate form configuration.
      * Basic grid:
      * c_begin  peak_pos  c_end
      * 1         1        1
      * 2         2        2
      * 3         3        3
      *
      * If form begins with high c and then peak exists in down part and then c going up.
      * We can get (131)
      * There are possible 27 distinct values.
      * Plus: (000)=0 if form width in seconds less < than 5*bws = (formDeepKoef-1)*bws
      * and Plus 9 values if we can determine peak position.
      * then possible (1,0,1),(1,0,2) etc.
      */

    def simpleRound3Double(valueD: Double) = {
      (valueD * 1000).round / 1000.toDouble
    }


    def simpleRound6Double(valueD: Double) = {
      (valueD * 1000000).round / 1000000.toDouble
    }

    // println("create1:                       seqTicks.size = "+seqTicks.size)
    // println("create2: seqTicks.maxBy(_.db_tsunx).db_tsunx = "+seqTicks.maxBy(_.db_tsunx).db_tsunx)

    val frmConfPeak: Int = {
      if (seqTicks.isEmpty) 0
      else {
        // Check that real seq width in seconds no less than contol limit.
        if ((seqTicks.maxBy(_.db_tsunx).db_tsunx - seqTicks.minBy(_.db_tsunx).db_tsunx) / 1000L < (formDeepKoef - 1) * barFa.barWidthSec) 0
        else {
          val formBeginC = seqTicks.minBy(_.db_tsunx).ask // begin price of ticker for form
          val formEndC = seqTicks.maxBy(_.db_tsunx).ask // last price of form
          val formCMin = seqTicks.map(_.ask).min // total min price
          val formCMax = seqTicks.map(_.ask).max // total max price
          val deltaC = (formCMax - formCMin) / 3
          val c1 = formCMin
          val c2 = c1 + deltaC
          val c3 = c2 + deltaC
          val c4 = formCMax
          val fc1: Int = if (formBeginC >= c1 && formBeginC <= c2) 3
          else if (formBeginC >= c2 && formBeginC < c3) 2
          else 1
          val fc3: Int = if (formEndC >= c1 && formEndC <= c2) 3
          else if (formEndC >= c2 && formEndC < c3) 2
          else 1
          val cMaxRo: Double = getMaxRo(seqTicks)
          val fc2: Int = if (cMaxRo >= c1 && cMaxRo <= c2) 3
          else if (cMaxRo >= c2 && cMaxRo < c3) 2
          else 1
          fc1 * 100 + fc2 * 10 + fc3
        }
      }
    }


    //here we can calculate any all forms properties.
    //         (SpS,SmS)
    val SdivS: (Double, Double) = if (seqTicks.toList.size <= 1) {
      (0.0,0.0)
    }
    else {
      val pairsCurrNxt :Seq[(tinyTick,tinyTick)] = seqTicks.zip(seqTicks.tail)

      val filterUp: ((tinyTick,tinyTick)) => Boolean = {
        case (f: tinyTick, s: tinyTick) => if (f.ask < s.ask) true else false
      }

      val filterDown: ((tinyTick,tinyTick)) => Boolean = {
        case (f: tinyTick, s: tinyTick) => if (f.ask > s.ask) true else false
      }

      val filterPairInInterval : ((tinyTick,tinyTick),Double,Double) => Boolean = {
        case ((f: tinyTick, s: tinyTick),beginInterval,endInterval) =>
          if ((s.ask-f.ask) >= beginInterval && (s.ask-f.ask) < endInterval) true
          else false
      }

      val seqPairsUp = pairsCurrNxt.filter(filterUp)
      val seqPairsDown = pairsCurrNxt.filter(filterDown).map(elm => (elm._2,elm._1))

      println(" seqPairsUp.size="+seqPairsUp.size)
      println(" seqPairsDown.size="+seqPairsDown.size)

      println(" example (ask1 - ask2): "+seqPairsUp.head._1.ask+" - "+seqPairsUp.head._2.ask)

      val n = 10
      val minPairUpStep = seqPairsUp.map(e => (e._2.ask-e._1.ask)).reduceOption(_ min _).getOrElse(0.0)
      val maxPairUpStep = seqPairsUp.map(e => (e._2.ask-e._1.ask)).reduceOption(_ max _).getOrElse(0.0)
      val widthPairsUp = simpleRound6Double((maxPairUpStep - minPairUpStep)/n)

      println(" minPairUpStep="+minPairUpStep)
      println(" maxPairUpStep="+maxPairUpStep)
      println(" widthPairsUp="+widthPairsUp)

      val Sp :Double = if (widthPairsUp == 0) 0.000000
      else (
        for (idx <- Range(1, n + 2)) yield {
          val freqInterval = seqPairsUp.count(elm => filterPairInInterval(
            elm,
            minPairUpStep + (idx - 1) * widthPairsUp,
            minPairUpStep + idx * widthPairsUp
          ))
          simpleRound6Double((minPairUpStep + idx * widthPairsUp) - (minPairUpStep + (idx - 1) * widthPairsUp)
          ) * freqInterval
        }).sum

      val minPairDownStep = seqPairsDown.map(e => (e._2.ask-e._1.ask)).reduceOption(_ min _).getOrElse(0.0)
      val maxPairDownStep = seqPairsDown.map(e => (e._2.ask-e._1.ask)).reduceOption(_ max _).getOrElse(0.0)
      val widthPairsDown = simpleRound6Double((maxPairDownStep - minPairDownStep)/n)

      val Sm :Double = if (widthPairsDown == 0) 0.000000
      else (
        for (idx <- Range(1, n + 2)) yield {
          val freqInterval: Int = seqPairsDown.count(elm => filterPairInInterval(
            elm,
            minPairDownStep + (idx - 1) * widthPairsDown,
            minPairDownStep + idx * widthPairsDown
          ))
          simpleRound6Double((minPairDownStep + idx * widthPairsDown) - (minPairDownStep + (idx - 1) * widthPairsDown)
          ) * freqInterval
        }).sum

      println("ticker_id= "+barFa.tickerId)

      println("barFa.ts_end = "+barFa.ts_end)
      println("barFa.c = "+barFa.c)
      println("barFa.ts_end_res = "+barFa.ts_end_res)
      println("barFa.c_res = "+barFa.c_res)

      println("seqTicks TS begin= "+seqTicks.minBy(e => e.db_tsunx).db_tsunx)
      println("seqTicks TS end= "+seqTicks.maxBy(e => e.db_tsunx).db_tsunx)
      println("seqTicks CNT= "+seqTicks.size)

      println("SpS = " + simpleRound3Double(Sp/(Sp+Sm)))
      println("SmS = " + simpleRound3Double(Sm/(Sp+Sm)))
      println("====================================================================")
      (simpleRound3Double(Sp/(Sp+Sm)),simpleRound3Double(Sm/(Sp+Sm)))
    }


    /**
      * Here calculate any properties for FormProps.
    */
      new bForm(
        barFa.tickerId,
        barFa.barWidthSec,
        barFa.dDate,
        tsBegin,
        barFa.ts_end,
        barFa.log_oe,
        barFa.res_type,
        barFa.ts_end_res,
        barFa.dursec_res,
        barFa.ddate_res,
        barFa.c_res,
        formDeepKoef,
        Map(
          "tickscnt"    -> ticksCnt.toString,
          "frmconfpeak" -> frmConfPeak.toString,
          "sps"         -> SdivS._1.toString,
          "sms"         -> SdivS._2.toString
        )
      )
  }
}
