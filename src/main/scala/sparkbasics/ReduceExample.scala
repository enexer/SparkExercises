package sparkbasics

object ReduceExample extends InitializeSpark {


  val rdd = sc.parallelize(Seq(1,2,23,4,5,-6,23,2,2))

  println(rdd.reduce((a, b) => findMax(a, b)))
  println(rdd.reduce((a, b) => findMin(a, b)))


  def findMax(a: Int, b: Int): Int = if (a > b) a else b
  def findMin(a: Int, b: Int): Int = if (a < b) a else b


}
