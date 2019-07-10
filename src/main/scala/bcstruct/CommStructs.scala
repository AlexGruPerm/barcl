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
                        tsEndLastBar  :Option[Long],
                        //-----------------
                        dDateLastTick :Option[LocalDate],
                        tsLastTick    :Option[Long],
                        //-----------------
                        tsFirstTicks  :Option[Long]//todo: it's not necessary to read it on each iterations.
                       ){

  val beginFrom = tsEndLastBar.getOrElse(
    tsFirstTicks.getOrElse(
      tsLastTick.getOrElse(0L)))

  val diffLastTickTSBarTS = tsLastTick.getOrElse(0L)/1000L - beginFrom/1000L

}

/**
  * Meta information for first ticks for bars calculator.
  * Optimization from 26.06.2019
*/
case class FirstTickMeta(tickerId   :Int,
                         firstDdate :Option[LocalDate],
                         firstTs    :Option[Long]
                        )



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
               ) {
  override def toString: String =
    dDate.toString+" - "+db_tsunx
}

case class seqTicksObj(
                    sqTicks :Seq[Tick]
                   )
/**
  * class used inside bar
*/
case class TsPoint(
                   ts    :Long,
                   index :Int
                  )

object TsPoint{
  def create(initVal :(Long,Int)) :TsPoint = {
    new TsPoint(initVal._1,initVal._2)
  }
}

case class TsIntervalGrp(
                        tsBegin     :Long,
                        tsEnd       :Long,
                        groupNumber :Int
                        )

object TsIntervalGrp{
  def create(initVal :(TsPoint,TsPoint)) :TsIntervalGrp = {
    new TsIntervalGrp(initVal._1.ts, initVal._2.ts, initVal._2.index)
  }
}

package object bcstruct {
  type seqTicksWithReadDuration = (seqTicksObj, Long)
}

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
  val ddateFromTick   :LocalDate = barTicks.last.dDate
  val bar_width_sec   :Int= p_bar_width_sec
  val ts_begin        :Long = barTicks(0).db_tsunx
  /**
    * It was barTicks.last.db_tsunx, but exists cases when
    * we have barTicks with just 1 ticks. For example we read seq of ticks with size 4
    * And calculate bars with BWS=30.
    * And in our seq interval between 1-st and 2-nd ticks is 49 sec.
    * We need calculate ts_end as ts_begin + bar_width_sec (*1000)
    * It was bad idea when (ts_begin = ts_end in BAR)
    * OLD CODE:
    * val ts_end :Long = barTicks.last.db_tsunx
    *
  */
  val ts_end          :Long = if (barTicks.size==1) ts_begin+bar_width_sec*1000L else barTicks.last.db_tsunx
/*
  println(" >>>> INSIDE CONSTRUCTOR BAR: barTicks.size="+barTicks.size+" ts_begin="+ts_begin+" ts_end="+ts_end+" ddate(ts_begin)="+core.LocalDate.fromMillisSinceEpoch(ts_begin)
    +"  ddate(barTicks.last.db_tsunx)="+core.LocalDate.fromMillisSinceEpoch(ddate)+" possibleDDATE="+ddateFromTick
  )
  */

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

    if (rngCStep != 0) {
      val rngC = formCMin.to(formCMax).by(rngCStep)
      val rangesC = rngC.zip(rngC.tail)

      val rangeFreq: Seq[(Double, Int)] = rangesC.map(rng =>
        (rng._1, seqTicks.count(t => t.ask >= rng._1 && t.ask <= rng._2)))
      rangeFreq.maxBy(r => r._2)._1
    } else formCMin
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

    def simpleRound3Double(valueD: Double) =
      (valueD * 1000).round / 1000.toDouble

    def simpleRound6Double(valueD: Double) =
      (valueD * 1000000).round / 1000000.toDouble

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

      /*
      println(" seqPairsUp.size="+seqPairsUp.size)
      println(" seqPairsDown.size="+seqPairsDown.size)
      println(" example (ask1 - ask2): "+seqPairsUp.head._1.ask+" - "+seqPairsUp.head._2.ask)
      */

      val n = 10
      val minPairUpStep = seqPairsUp.map(e => e._2.ask-e._1.ask).reduceOption(_ min _).getOrElse(0.0)
      val maxPairUpStep = seqPairsUp.map(e => e._2.ask-e._1.ask).reduceOption(_ max _).getOrElse(0.0)
      val widthPairsUp = simpleRound6Double((maxPairUpStep - minPairUpStep)/n)

      /*
      println(" minPairUpStep="+minPairUpStep)
      println(" maxPairUpStep="+maxPairUpStep)
      println(" widthPairsUp="+widthPairsUp)
      */

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

      val minPairDownStep = seqPairsDown.map(e => e._2.ask-e._1.ask).reduceOption(_ min _).getOrElse(0.0)
      val maxPairDownStep = seqPairsDown.map(e => e._2.ask-e._1.ask).reduceOption(_ max _).getOrElse(0.0)
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
      /*
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
      */
      (simpleRound3Double(Sp/(Sp+Sm)),simpleRound3Double(Sm/(Sp+Sm)))
    }


    def analyzePVx(pAnalyze :Double,pv2 :Double,pv3 :Double) :Int ={
      if (pAnalyze > Seq(pv2,pv3).max) 1
      else if (pAnalyze < Seq(pv2,pv3).min) 3
      else 2
    }

    /**
      * ticks volume profile.
      * All interval of ticks diveded on 3 parts, from left (ts_begin) to right (ts_end)
      * equidistant.
      * And we have 3 subproperties: pv1, pv2 , pv3
      * each value take one value from (1,2,3) 1 for maximum, 2 for middle value, and 3 for minimum.
      * For example: Volume profile 120,90,100
      * then p1 = 1(max) pv2 = 3(min) pv3 = 2(mdl)  (1,3,2)
      */
      val pvX : (Int,Int,Int) = {
       val totalTickVolume :Double = if (seqTicks.hasDefiniteSize) seqTicks.size else 0.0
        if (totalTickVolume==0) (0,0,0)
        else {
//          println("totalTickVolume="+totalTickVolume)
         val tsBegin :Long = seqTicks.minBy(_.db_tsunx).db_tsunx
         val tsEnd   :Long = seqTicks.maxBy(_.db_tsunx).db_tsunx
         val tsStepWidth = (tsEnd - tsBegin)/3
//          println("tsBegin="+tsBegin+" tsEnd="+tsEnd+" tsStepWidth="+tsStepWidth)
/*
          println("count1="+seqTicks.count(tc => tc.db_tsunx >= tsBegin && tc.db_tsunx <= (tsBegin+tsStepWidth)))
          println("count2="+seqTicks.count(tc => tc.db_tsunx >= (tsBegin+tsStepWidth) && tc.db_tsunx <= (tsBegin+2*tsStepWidth)))
          println("count3="+seqTicks.count(tc => tc.db_tsunx >= (tsBegin+2*tsStepWidth) && tc.db_tsunx <= tsEnd))
*/
          val pv1 = simpleRound3Double(seqTicks.count(tc => tc.db_tsunx >= tsBegin && tc.db_tsunx <= (tsBegin+tsStepWidth))/totalTickVolume)
          val pv2 = simpleRound3Double(seqTicks.count(tc => tc.db_tsunx >= (tsBegin+tsStepWidth) && tc.db_tsunx <= (tsBegin+2*tsStepWidth))/totalTickVolume)
          val pv3 = simpleRound3Double(seqTicks.count(tc => tc.db_tsunx >= (tsBegin+2*tsStepWidth) && tc.db_tsunx <= tsEnd)/totalTickVolume)
//         println("barFa.tickerId="+barFa.tickerId+" ts_end="+barFa.ts_end+ " pv1="+pv1+" pv2="+pv2+" pv3="+pv3+"")
//          println("===================================")
          (
            analyzePVx(pv1,pv2,pv3),
            analyzePVx(pv2,pv1,pv3),
            analyzePVx(pv3,pv1,pv2)
          )
        }
      }


    /*
    Property calculated from previous one.
    As a short form
    Just 5 possible values: 0,1,2,3,4 (0- unknown.)
    */
    val ticksValusFormation :Int = pvX match {
      case (0,0,0) => 0 //not defined
      case (1,2,3) => 1 // volume decreasing
      case (3,2,1) => 2 // volume increasing
      case (2,1,3) | (3,1,2) => 3 // Up peak in middle - maximum
      case (1,3,2) | (2,3,1) => 4 // Down peak in middle - minimum
      case _ => 0 //not defined
    }

    //println("barFa.tickerId="+barFa.tickerId+" ts_end="+barFa.ts_end+ " ticksValusFormation="+ticksValusFormation)


    def getACFKoeff(acLevel :Int) :Double ={
      val seqSrcX = seqTicks.drop(acLevel).map(_.ask)
      val seqSrcY = seqTicks.take(seqTicks.size - acLevel).map(_.ask)

      val n = seqSrcX.size

      val avgX = seqSrcX.sum / n
      val avgY = seqSrcY.sum / n

      val avgXY = seqSrcX.zip(seqSrcY).map(elm => elm._1 * elm._2).sum / n
      val prodAvgXY = avgX * avgY

      val sigma2X = seqSrcX.map(e => Math.pow(e, 2)).sum / n - Math.pow(avgX, 2)
      val sigma2Y = seqSrcY.map(e => Math.pow(e, 2)).sum / n - Math.pow(avgY, 2)

      val r = (avgXY - prodAvgXY) / Math.sqrt(sigma2X * sigma2Y)
      r
    }

    val acfLevel1 = simpleRound3Double(getACFKoeff(barFa.barWidthSec/2))
    val acfLevel2 = simpleRound3Double(getACFKoeff(barFa.barWidthSec))
    val acfLevel3 = simpleRound3Double(getACFKoeff(barFa.barWidthSec*2))

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
          "tickscnt"     -> ticksCnt.toString,
          "frmconfpeak"  -> frmConfPeak.toString,
          "sps"          -> SdivS._1.toString,
          "sms"          -> SdivS._2.toString,
          "tcvolprofile" -> ticksValusFormation.toString,
          "acf_05_bws"  -> acfLevel1.toString,
          "acf_1_bws"   -> acfLevel2.toString,
          "acf_2_bws"   -> acfLevel3.toString
        )
      )
  }
}
