

Seq(0,1,2,3,4,5).find


/*
//autocorrelation
//Статистика. Часть 1. Теория статистики Авторы: Наталья Королькова, Инна Минина Стр. 181.
case class tinyTick(db_tsunx :Long, ask :Double)

val seqTicks = Seq(tinyTick(1,1.13449), tinyTick(2,1.13447), tinyTick(3,1.13444), tinyTick(4,1.13449),
  tinyTick(5,1.13451), tinyTick(6,1.13459),  tinyTick(7,1.13469), tinyTick(8,1.13479), tinyTick(9,1.13489),  tinyTick(10,1.13499), tinyTick(11,1.13499),
  tinyTick(12,1.13489), tinyTick(13,1.13479), tinyTick(14,1.13469), tinyTick(15,1.13459), tinyTick(16,1.13449), tinyTick(17,1.13439), tinyTick(18,1.13439), tinyTick(19,1.13449), tinyTick(20,1.13469), tinyTick(21,1.13489))

val srcN = seqTicks.size
val acLevel = 1


def getACFKoeff(seqTicks :Seq[tinyTick],acLevel :Int) :Double ={
  val seqSrcX = seqTicks.drop(acLevel).map(_.ask)
  val seqSrcY = seqTicks.take(srcN - acLevel).map(_.ask)

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

for(acl <- Range(1,4))
  println(acl+" "+getACFKoeff(seqTicks,acl))

*/



//val seqLogCO  = seqTicks.zip(seqTicks.tail)
// .map(thisPair => Math.log(thisPair._2.ask/thisPair._1.ask))

/*

val x = seqTicks.drop(acLevel).map
val y = seqTicks.take(seqTicks.size - acLevel)
val n = x.size

val avgX = x.map(_).sum / n
val avgY = seqAutoElms.map(elm => elm._2).sum/n
val avgXY = seqAutoElms.map(elm => elm._1*elm._2).sum/n

*/

/*
//-1.762922219907152E-5, -2.6444416042893746E-5, 4.407363824189353E-5,
 1 .7628911415075048E-5
val seqLogCO  = seqTicks.zip(seqTicks.tail)
  .map(thisPair => Math.log(thisPair._2.ask/thisPair._1.ask))
seqLogCO.size

// (-1.762922219907152E-5,-2.6444416042893746E-5),
// (-2.6444416042893746E-5,4.407363824189353E-5)
val seqAutoElms = seqLogCO.zip(seqLogCO.tail)
val n = seqAutoElms.size

val avgX = seqAutoElms.map(elm => elm._1).sum/n
val avgY = seqAutoElms.map(elm => elm._2).sum/n
val avgXY = seqAutoElms.map(elm => elm._1*elm._2).sum/n

val s2X = seqAutoElms.map(elm => Math.pow(elm._1,2)).sum/n - avgX
val s2Y = seqAutoElms.map(elm => Math.pow(elm._2,2)).sum/n - avgY

val sX = Math.sqrt(s2X)
val sY = Math.sqrt(s2Y)

val r = (avgXY - avgX*avgY)/(s2X*s2Y)
*/












/*
def simpleRound3Double(valueD : Double) = {
  (valueD * 1000).round / 1000.toDouble
}

case class tinyTick(db_tsunx  :Long,
                    ask       :Double)

val seqTicks = Seq(
  tinyTick(1,20),  tinyTick(2,21),  tinyTick(3,22),  tinyTick(4,22),  tinyTick(5,21),
  tinyTick(6,21),  tinyTick(7,21),  tinyTick(8,24),  tinyTick(9,25),  tinyTick(10,27),
  tinyTick(11,25),  tinyTick(12,24),  tinyTick(13,23),  tinyTick(14,22),  tinyTick(15,21),
  tinyTick(16,22),  tinyTick(17,23),  tinyTick(18,25),  tinyTick(29,27),  tinyTick(20,28),
  tinyTick(21,28))

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

val n = 10
val minPairUpStep = seqPairsUp.map(e => (e._2.ask-e._1.ask)).min
val maxPairUpStep = seqPairsUp.map(e => (e._2.ask-e._1.ask)).max
val widthPairsUp = simpleRound3Double((maxPairUpStep - minPairUpStep)/n)

val Sp = for(idx <- Range(1,n+2)) yield {
  val freqInterval = seqPairsUp.count(elm => filterPairInInterval(
    elm,
    minPairUpStep+(idx-1)*widthPairsUp,
    minPairUpStep+idx*widthPairsUp
  ))
  simpleRound3Double((minPairUpStep+idx*widthPairsUp) - (minPairUpStep+(idx-1)*widthPairsUp)
  )*freqInterval
}

val minPairDownStep = (seqPairsDown.map(e => (e._2.ask-e._1.ask)).min)
val maxPairDownStep = (seqPairsDown.map(e => (e._2.ask-e._1.ask)).max)
val widthPairsDown = simpleRound3Double((maxPairDownStep - minPairDownStep)/n)

val Sm = for(idx <- Range(1,n+2)) yield {
  val freqInterval :Int = seqPairsDown.count(elm => filterPairInInterval(
    elm,
    minPairDownStep+(idx-1)*widthPairsDown,
    minPairDownStep+idx*widthPairsDown
  ))
  simpleRound3Double((minPairDownStep+idx*widthPairsDown)-(minPairDownStep+(idx-1)*widthPairsDown)
    )*freqInterval
}

println("Sp = " + simpleRound3Double(Sp.sum/(Sp.sum+Sm.sum)))
println("Sm = " + simpleRound3Double(Sm.sum/(Sp.sum+Sm.sum)))
*/


//val freqsPairsUp = for (r <- range())


//Prepare descriptions of SpS SmS with sceenshots.


/*
val formCMin = seqTicks.map(_.ask).min          // total min price
val formCMax = seqTicks.map(_.ask).max          // total max price
val n = 10
val rngCStep = (formCMax-formCMin)/n

val rngC = formCMin.to(formCMax).by(rngCStep)
val rangesC = rngC.zip(rngC.tail)

val rangeFreq :Seq[(Double,Int)] = rangesC.map(rng =>
  (rng._1, seqTicks.count(t => t.ask >= rng._1 && t.ask <= rng._2)))

for (r <- rangeFreq) println(r)

rangeFreq.maxBy(r => r._2)._1
*/

/*

(21.5,5)
(22.0,5)

(21.0,8)
(22.0,8)

*/



/*
val currBar_c  :Double = 1.1400
val p          :Double = 0.0044

val cUp = (Math.exp( Math.log(currBar_c) + p)* 10000).round / 10000.toDouble
val cDw = (Math.exp( Math.log(currBar_c) - p)* 10000).round / 10000.toDouble
*/

/*
val l :Seq[Int] = Seq(1,2,4,3,5,6,9,7)

val r :Seq[Int] = l.withFilter(e => e>=5)
*/

  /*
val r :Seq[Int] = for(e <- l) yield {
  if (e >=5)
    (1 to e).sum
}
*/

/*
val l :Seq[Int] = Seq(1,2,4,3,5,6,9,7)
val cur = l.init
val nxt = l.tail

def getCntMovingHigh (s : Seq[Int]) : (Int,Int) =
  (s.size,s.sum)

//val cntUp =
 val cntUp   = getCntMovingHigh(cur.zip(nxt).filter{case (c,n) => (n>c)})
 val cntDown = cur.zip(nxt).filter{case (c,n) => (n<c)}.size
*/


/*

val s :Seq[Long] = Seq(
  17990,7199,43201,10792,
  88820,//188820,
  39622,10800,10804,
  61197,
  7200,39629,43202,14401,
  121213,//221213,
  7184,
  14399)

val mo = s.sum / s.size

math.pow(2,3)

val sumKv = s.map(v => Math.pow((v-mo),2)).sum

val disp = sumKv / s.size

*/





/*
case class bar(ts:Int, v :Int)

val l = List(
  bar(1,10),
  bar(2,20),
  bar(3,30),
  //---------
  bar(6,60),
  bar(7,70),
  bar(8,80),
  bar(9,85),
  //---------
  bar(12,700),
  bar(13,800)
)


val acc_bar = l.head

val r = l.tail.foldLeft(List((1,acc_bar))) ((acc :List[(Int,bar)],elm :bar) =>
 if ((elm.ts - acc.head._2.ts) < 2)
   ((acc.head._1, elm) :: acc)
   else
   ((acc.head._1+1, elm) :: acc)
).reverse

val f = r.groupBy(elm => elm._1).map(
  s => (s._1,s._2.filter(
    e => e._2.ts == (s._2.map(
      b => b._2.ts).max)
  ))
).toSeq.sortBy(elm => elm._1).map(elm => elm._2)

*/


/*
for(elm <-f) {
  println(elm)
}
*/


