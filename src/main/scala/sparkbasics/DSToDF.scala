package sparkbasics

import init.InitSpark

object DSToDF extends InitSpark {

  import spark.implicits._

  import scala.util.Random

  val ds = spark.range(10).map((_, Random.nextInt()))
  ds.show()

  val df = ds.toDF()
  df.printSchema()
  df.show()

  // implicit
  val df2 = spark.createDataFrame(ds.rdd)
  df2.printSchema()

  // explicit
  // DS[Int, Long] -> RDD[Row] -> DF
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  val rdd2 = ds.rdd.map(s=>Row(s._1,s._2))
  val schema = new StructType().add("a1","long").add("a2","integer")
  val df3 = spark.createDataFrame(rdd2, schema)
  df3.printSchema()
  df3.show()

  val cols = df3.columns.map(s=>df3.col(s))
  df3.select(cols: _*)


  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  val schemaa = (new StructType).add("id","long")
  spark.createDataFrame(spark.range(10).rdd.map(s=>Row(s)), schemaa).show()

}
/*
+---+-----------+
| _1|         _2|
+---+-----------+
|  0| 2010321520|
|  1|  596639145|
|  2| 1158312808|
|  3| 2113672809|
|  4|-1542638334|
|  5| 2015313153|
|  6|  -49941397|
|  7|-2141425441|
|  8| -299064388|
|  9|   47757084|
+---+-----------+

root
 |-- _1: long (nullable = true)
 |-- _2: integer (nullable = false)

+---+-----------+
| _1|         _2|
+---+-----------+
|  0|-1781417252|
|  1| -966814844|
|  2|-1272309378|
|  3|-1305544459|
|  4|-1594970105|
|  5|  -88299709|
|  6|   41791067|
|  7|  225475840|
|  8| -818486552|
|  9|-1413616166|
+---+-----------+

root
 |-- _1: long (nullable = true)
 |-- _2: integer (nullable = false)

root
 |-- a1: long (nullable = true)
 |-- a2: integer (nullable = true)

+---+-----------+
| a1|         a2|
+---+-----------+
|  0| 1759572328|
|  1| -703520147|
|  2|-1996724186|
|  3| -628403012|
|  4|  523783126|
|  5|  477975725|
|  6| 1730266432|
|  7| 1724961337|
|  8| 1749483843|
|  9|  611952436|
+---+-----------+

+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
+---+
 */