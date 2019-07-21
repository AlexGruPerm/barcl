import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}

def calc1 = {
  println("begin calc1")
  Thread.sleep(1000)
  println(" calc1 middle message")
  Thread.sleep(1000)
  println("end calc1")
  1
}

def calc2 = {
  println("begin calc1")
  Thread.sleep(1500)
  println(" calc1 middle message")
  Thread.sleep(1500)
  println("end calc1")
  2
}

def seqCalc :Int ={
  calc1+calc2
}

def prlCalc :Int ={
  val v1 = Future(calc1)
  val v2 = Future(calc2)
  val c = v1.flatMap(r1 => v2.map(r2 => r1+r2))
  Await.result(c,)
}

def test1 = {
  val startCalc = System.nanoTime
  seqCalc
  val durr = TimeUnit.MILLISECONDS.convert((System.nanoTime()-startCalc), TimeUnit.NANOSECONDS);
  println("Test 1 duration = "+durr+" ms.")
}

def test2 = {
  val startCalc = System.nanoTime
  prlCalc
  val durr = TimeUnit.MILLISECONDS.convert((System.nanoTime()-startCalc), TimeUnit.NANOSECONDS);
  println("Test 2 duration = "+durr+" ms.")
}

test1