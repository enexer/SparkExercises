package sparkbasics

import init.InitSpark

object ConcatSplitExplode extends InitSpark{

  import spark.implicits._

  val df = Seq("1,2,3,4,5", "6,7,8,9").toDF("val")
  df.show()
  val df2 = df.join(df, df("val")===df("val")).toDF("v1","v2")
  df2.show()
  import org.apache.spark.sql.functions._
  val df3 = df2.select(concat_ws(",", 'v1,'v2).as("val"))
  df3.show()
  val df4 = df3.select(split('val, ",").as("val"))
  df4.show()
  val df5 = df4.select(explode('val))
  df5.show()

}
/*
+---------+
|      val|
+---------+
|1,2,3,4,5|
|  6,7,8,9|
+---------+

+---------+---------+
|       v1|       v2|
+---------+---------+
|1,2,3,4,5|1,2,3,4,5|
|  6,7,8,9|  6,7,8,9|
+---------+---------+

+-------------------+
|                val|
+-------------------+
|1,2,3,4,5,1,2,3,4,5|
|    6,7,8,9,6,7,8,9|
+-------------------+

+--------------------+
|                 val|
+--------------------+
|[1, 2, 3, 4, 5, 1...|
|[6, 7, 8, 9, 6, 7...|
+--------------------+

+---+
|col|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
|  6|
|  7|
|  8|
|  9|
+---+
 */
