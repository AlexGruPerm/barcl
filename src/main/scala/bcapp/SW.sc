val l :Seq[Int] = Seq(1,2,4,3,5,6,9,7)

val r :Seq[Int] = l.withFilter(e => e>=5)

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


