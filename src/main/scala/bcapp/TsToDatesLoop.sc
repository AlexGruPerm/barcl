import com.datastax.driver.core.LocalDate

val d1 = LocalDate.fromMillisSinceEpoch(1563483602072L).getDaysSinceEpoch
val d2 = LocalDate.fromMillisSinceEpoch(1563775726804L).getDaysSinceEpoch

for(d <- d1 to d2) {
  println(d+" - "+LocalDate.fromDaysSinceEpoch(d))
}