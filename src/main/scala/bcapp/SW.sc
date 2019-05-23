
/*
val p = Option(1)

p.map(_*2).getOrElse(0)

p.getOrElse(0)

p.fold(0)(_*2)
*/

/*

val seqOpts :Seq[Option[Int]] = Seq(Option(1),None,Option(5),None,Option(9))

val res = seqOpts.map(elm => elm)

*/

/*
val barDeepSec = 10

val barsSides :Seq[Int] = Seq(1,4,7,10,  11,15,18,20   ,21,25,29,30,   39,42)

val seqBarSides :Seq[(Int,Int)] = barsSides.head.to(barsSides.last).by(barDeepSec).zipWithIndex
*/
//seqBarRanges: scala.collection.immutable.IndexedSeq[(Int, Int, Int)]
// = Vector((1,11, 1), (11,21, 2), (21,31, 3), (31,31, 4))

/*
val seqBarRanges = seqBarSides.init.zip(seqBarSides.tail)
  .map(pair => (pair._1._1, pair._2._1, pair._2._2))
*/

/*
val seqBarRanges = for (i <- seqBarSides.indices) yield {
  if (i < seqBarSides.last._2)
    (seqBarSides(i)._1, seqBarSides(i + 1)._1, seqBarSides(i)._2 + 1)
  else
    (seqBarSides(i)._1, seqBarSides(i)._1, seqBarSides(i)._2 + 1)
}
*/






/*
val seqMinsDdateTs :List[(Option[Long], Option[Long])] =
  List(
       (None,Some(1000L)),
       (Some(5L),Some(1000L)),
       (None,Some(1551144386007L)),
       (Some(100500L),None)
      )

val ord = Ordering.by((_: (Option[Long], Option[Long]))._2
 match {
  case Some(l) => l
  case None => 0L
 })

seqMinsDdateTs.reduceOption(ord.min).head
*/


/*
seqMinsDdateTs.collect{
  case elm :(Option[Long],Option[Long]) =>
    (elm._1,elm._2) match {
     case (_,Some(l)) => l
     case (_,None) => 0L
  }
}
*/


/*
seqMinsDdateTs.minBy(elm => elm {
  case (elm :(Option[Long], Some[Long])) =>
    elm._2
  case None => 0L
 }
)
*/

/*
Error:(9, 88) not enough arguments for method minBy: (implicit cmp: Ordering[B])(Option[Long], Option[Long]).
Unspecified value parameter cmp.
def get$$instance$$res0 = /* ###worksheet### generated $$end$$ */ seqMinsDdateTs.minBy(elm => elm {
                                                                                      ^
*/


/*
import com.datastax.driver.core.{Cluster, LocalDate, Row}

val session = Cluster.builder()
  .addContactPoint("10.241.5.234")
  .build().connect()

val bndBarsFormsMaxDdate = session.prepare(
  """ select max(ddate)  as ddate,
                       max(ts_end) as ts_end
                 from mts_bars.bars_forms
                where ticker_id     = :p_ticker_id and
                      bar_width_sec = :p_bar_width_sec and
                      formdeepkoef  = :p_formdeepkoef and
                      log_oe        = :p_log_oe and
                      res_type      = :p_res_type
                allow filtering;
       """).bind()

def getDdateTsEndFromRow(row :Row) : Option[(LocalDate,Long)] = {
  Option(row.getDate("ddate")) match {
    case Some(ld) => Some((ld,row.getLong("ts_end")))
    case None => None
  }
}

  Seq(0.0022,0.0033,0.0044).map(
  pr => {
    getDdateTsEndFromRow(
      session.execute(bndBarsFormsMaxDdate
        .setInt("p_ticker_id", 1)
        .setInt("p_bar_width_sec", 30)
        .setInt("p_formdeepkoef", 6)
        .setDouble("p_log_oe", pr)
        .setString("p_res_type","mx")
      ).one()
    )
  }
).collect{case Some(d)=>d}
  match {
    case List() => None
    case nel : List[(LocalDate,Long)] => Option(nel.minBy(_._2))
  }

*/