package sparkbasics

import init.InitSpark

object CollectInsteadOfFilterAndMap extends InitSpark{


  val rdd1 = spark.range(10).rdd

  val rs2 = rdd1.filter(_>=5).map(_%5+1)

  // same operation as above using collect and "partial function"
  // https://www.scala-lang.org/api/2.12.8/scala/PartialFunction.html
  val rs1 = rdd1.collect{case x if x>=5 => x%5+1}

  rs1.collect().foreach(print(_))
  println("")
  rs2.collect().foreach(print(_))

  //val (l1,l2) =  (1 to 10).partition(_>5)  // Vector(6, 7, 8, 9, 10), Vector(1, 2, 3, 4, 5)
  //val ok = (1 to 10).grouped(3); ok.foreach(x=>println(x.size))   //  3 3 1
  //val (l1,l2) = (1 to 6).splitAt(3)  // Range(1, 2, 3), Range(4, 5, 6)

}
