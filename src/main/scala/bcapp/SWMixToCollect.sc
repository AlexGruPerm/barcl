case class A(tickerID :Int)
case class B(tickerID: Int, smth :Int)

val seqA :Seq[A] = Seq(A(1),A(2),A(3),A(4))
val seqB :Seq[B] = Seq(B(2,25),B(3,55),B(2,33),B(3,66))

seqA.filter(ftm => seqB.map(_.tickerID).contains(ftm.tickerID))
  .flatMap(thisTicker =>
    seqB.filter(bp => bp.tickerID == thisTicker.tickerID)
      .map(bp => (thisTicker.tickerID,bp.smth)))

//List((2,25), (2,33), (3,55), (3,66))

seqA.flatMap(thisTicker =>
    seqB.filter(bp => bp.tickerID == thisTicker.tickerID)
      .map(bp => (thisTicker.tickerID,bp.smth)))

//withFilter avoids one iteration and the creation of a filtered list as intermediate step
seqA.flatMap(thisTicker =>
  seqB.withFilter(bp => bp.tickerID == thisTicker.tickerID)
    .map(bp => (thisTicker.tickerID,bp.smth)))

seqA.flatMap(thisTicker =>
  seqB.collect{
    case bp if bp.tickerID == thisTicker.tickerID =>
      (thisTicker.tickerID,bp.smth)
  }
)

seqA.flatMap(thisTicker =>
  seqB.collect{
    case bp if bp.tickerID == thisTicker.tickerID =>
      (thisTicker.tickerID,bp.smth)
  }
)