val keys = List(2, 5, 1)

val mk :Map[Int,List[Int]] = Map(1->List(11,12,13), 2->List(21,22,23), 5->List(51,52,53))

//mk.get(x)

val nums1 = keys.map(x => mk.get(x))

val nums2 = keys.flatMap(x => mk.get(x))
