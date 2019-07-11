
val optInt :Option[Int] =
 None
// Option(345)


optInt.map(v => v*10).getOrElse(0)
optInt.flatMap(v => Option(v*10))


