package analytics

import init.InitSpark

object Tc1And2 extends InitSpark {

  val tc1 = sc.textFile("data3/tc1.txt")
  val tc2 = sc.textFile("data3/tc2.txt")


  println(tc1.count())
  println(tc2.count())
  println(tc2.intersection(tc1).count())
  println(tc2.subtract(tc1).count())



}
