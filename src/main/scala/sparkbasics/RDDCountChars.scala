package sparkbasics

import init.InitSpark

object RDDCountChars extends InitSpark{

  import spark.implicits._

  val data = Seq("  12 3,. 8","ab cd e e 12  ")
  val rdd = sc.parallelize(data)
  println(rdd.map(_.size).reduce((a, b) => a + b))
  println(rdd.map(_.trim().size).reduce((a, b) => a + b))

  val df =  data.toDF("value")
  import org.apache.spark.sql.functions._
  import spark.implicits._
  df.select(sum(length('value))).show()
  df.select(sum(length(trim('value)))).show()


}
/*
24
20
+------------------------+
|sum(length(trim(value)))|
+------------------------+
|                      20|
+------------------------+

+------------------+
|sum(length(value))|
+------------------+
|                24|
+------------------+
 */
