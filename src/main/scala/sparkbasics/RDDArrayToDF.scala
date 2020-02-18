package sparkbasics

import init.InitSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column

object RDDArrayToDF extends InitSpark {

  val rdd: RDD[Array[Any]] = spark.range(5).rdd.map(s => Array(s,s+1,s%2))
  val size = rdd.first().length
  rdd.foreach(s=>println(s.foreach(print(_))))

  def splitCol(col: Column): Seq[(String, Column)] = {
    (for (i <- 0 to size - 1) yield ("_" + i, col(i)))
  }

  import spark.implicits._
  rdd.map(s=>s.map(s=>s.toString()))
    .toDF("x")
    .select(splitCol('x).map(_._2):_*)
    .toDF(splitCol('x).map(_._1):_*)
    .show()

  /*
  010()
  121()
  230()
  341()
  450()
  +---+---+---+
  | _0| _1| _2|
  +---+---+---+
  |  0|  1|  0|
  |  1|  2|  1|
  |  2|  3|  0|
  |  3|  4|  1|
  |  4|  5|  0|
  +---+---+---+
   */
}
