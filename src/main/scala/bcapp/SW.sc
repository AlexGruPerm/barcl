
var l = List(11,2202,32,443,534)

for (
  b <-l ;
  idx = l.indexOf(b);
  s = l.drop(idx+1)
){
  println(b+" - "+s)
}



/*
val l :Seq[Int] = Seq(1,2,3,4,5,6,7,8,9,10)

val sl = l.grouped(3)

for (s <- sl){
  println(s)
}
*/

/*
val keys = List(2, 5, 1)

val mk :Map[Int,List[Int]] = Map(1->List(11,12,13), 2->List(21,22,23), 5->List(51,52,53))

//mk.get(x)

val nums1 = keys.map(x => mk.get(x))

val nums2 = keys.flatMap(x => mk.get(x))
*/