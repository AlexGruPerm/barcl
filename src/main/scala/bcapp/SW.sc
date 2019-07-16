
Seq(5,2,10).max

/*
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Random, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {

def calcIteration(tickerId :Int) :Future[Int] ={
  tickerId match {
    case 1 => {
      println("  ")
      println("Calculation of [1] ~~~~~")
      Thread.sleep(1000)
      println("End Calculation of [1]")
      Future(Random.nextInt(10000))
    }
    case 2 => {
      println("  ")
      println("  Calculation of [2] ------------------------------")
      Thread.sleep(2000)
      println("  End Calculation of [2]")
      Future(Random.nextInt(20000))
    }
    case 3 => {
      println("  ")
      println("    Calculation of [3] ========================================")
      Thread.sleep(3000)
      println("    End Calculation of [3]")
      Future(Random.nextInt(30000))
    }
  }
}

val fTicksMeta :Seq[Int] = Seq(1,2,3)

def taskCalcBars(tm :Int): Future[Unit] = Future {
  calcIteration(tm).onComplete {
    case Success(waitDur) => {
      tm match {
        case 1 => {
          println("Begin wait for [1] interval=" + waitDur)
          Thread.sleep(waitDur)
          println("End wait for [1]  ~~~~~")
          println("  ")
        }
        case 2 => {
          println("  Begin wait for [1] interval=" + waitDur)
          Thread.sleep(waitDur)
          println("  End wait for [2] ------------------------------")
          println("  ")
        }
        case 3 => {
          println("    Begin wait for [1] interval=" + waitDur)
          Thread.sleep(waitDur)
          println("    End wait for [3]  ========================================")
          println("  ")
        }
      }
    }
    case Failure(ex) => {
      println("Exception from calcIteration " + ex.getMessage + " - " + ex.getCause)
    }
  }
}

def loopCalcBars(tm :Int): Future[Unit] = taskCalcBars(tm).flatMap(_ => loopCalcBars(tm))

def infiniteLoop(): Seq[Future[Unit]] =
  fTicksMeta.map(tm => Future.sequence(List(loopCalcBars(tm))).map(_ => ()))

  Await.ready(Future.sequence(infiniteLoop), Duration.Inf)
}
*/






