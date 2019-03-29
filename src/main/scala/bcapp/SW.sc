
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




/*
for(elm <-f) {
  println(elm)
}
*/


