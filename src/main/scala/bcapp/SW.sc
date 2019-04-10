
case class tinyTick(db_tsunx  :Long,
                    ask       :Double)
val seqTicks = Seq(
  tinyTick(1,20),  tinyTick(2,21),  tinyTick(3,22),  tinyTick(4,22),  tinyTick(5,21),
  tinyTick(6,22),  tinyTick(7,21),  tinyTick(8,24),  tinyTick(9,25),  tinyTick(10,27),
  tinyTick(11,25),  tinyTick(12,24),  tinyTick(13,23),  tinyTick(14,22),  tinyTick(15,21),
  tinyTick(16,22),  tinyTick(17,23),  tinyTick(18,25),  tinyTick(29,27),  tinyTick(20,28),
  tinyTick(21,30))

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


