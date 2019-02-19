package casspackage

import bcpackage.{CalcProperties, CalcProperty}
import com.datastax.driver.core.Row

/**
  * Contains functions to convert Cassandra Dataset rows into Scala objects.
  */
class DataSetOperations(cassQueryBinds :CassQueriesBinds) {

  private val rowToCalcProperty = (row : Row) => {
    new CalcProperty(
      row.getInt("ticker_id"),
      row.getInt("bar_width_sec"),
      row.getInt("is_enabled")
    )
  }

  def getAllCalcProperties= {
    CalcProperties(cassQueryBinds.dsBCalcProperty
      .toSeq.map(rowToCalcProperty)
      .sortBy(sr => sr.tickerId)(Ordering[Int])
      .sortBy(sr => sr.barDeepSec)(Ordering[Int]))
  }




}
