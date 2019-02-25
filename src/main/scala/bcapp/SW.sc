//возвращающую список из всех не-None элементов списка.

1551115965 568L/1000L


/*
val neg : PartialFunction[Option[Int], Int] = {
  case Some(x) if x<0 => -1*x
  case Some(x) if x>0 => x
}

val pos : PartialFunction[Option[Int], Int] = {
  case Some(x) if x>0 && x<5 => x
  case Some(x) if x>5        => x*2
}

List(Some(-10),None,Some(4),None,Some(-3),Some(10)) collect neg
List(Some(-10),None,Some(4),None,Some(-3),Some(10)) collect pos

val fcomm = neg orElse pos

List(Some(-10),None,Some(4),None,Some(-3),Some(10)) collect fcomm
*/

