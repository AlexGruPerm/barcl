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

//val r :Option[(LocalDate,Long)] =
//val seqDdates :Seq[(LocalDate,Long)] =
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