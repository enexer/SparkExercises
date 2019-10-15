package sparkbasics

import init.InitSpark

object FilterColumnPairs extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  /*
    Filter out rows where diff for columns _1| _2| _3| _4
    between each pair (_1,_2)(_2,_3)(_3,_4) is  > 10
   */
  val df = Seq(
    (10, 21, 32, 43),
    (10, 20, 30, 40),
    (1, 2, 3, 4),
    (1, 100, 200, 300)
  ).toDF().withColumn("id",monotonically_increasing_id())

  df.show()

  val cols = df.columns.dropRight(1)
  var pairs: Array[(String, String)] = new Array[(String, String)](cols.length - 1)
  for (i <- 0 to cols.length - 2) {
    pairs(i) = (cols.apply(i), cols.apply(i + 1))
  }

  println("pairs:")
  pairs.foreach(print(_))
  println()

  val calcDiff = array_contains(
    array(
      pairs.map(s=>(df(s._2)-df(s._1))>10):_*
    ), false
  )

  df.filter(!calcDiff).show()


/*
+---+---+---+---+---+
| _1| _2| _3| _4| id|
+---+---+---+---+---+
| 10| 21| 32| 43|  0|
| 10| 20| 30| 40|  1|
|  1|  2|  3|  4|  2|
|  1|100|200|300|  3|
+---+---+---+---+---+

pairs:
(_1,_2)(_2,_3)(_3,_4)

+---+---+---+---+---+
| _1| _2| _3| _4| id|
+---+---+---+---+---+
| 10| 21| 32| 43|  0|
|  1|100|200|300|  3|
+---+---+---+---+---+
 */
}
