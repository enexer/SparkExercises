package sparkbasics

import init.InitSpark

object RDDRowNumber extends InitSpark {


  val data = Seq("a", "b", "c", "d", "e", "f", "g")
  val rdd = sc.parallelize(data)
  rdd.collect().foreach(println(_))
  val rdd2 = rdd.zipWithIndex().map(s => (s._1, s._2 % 3 + 1, s._2))
  rdd2.collect().foreach(println(_))


  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df = data.toDF("value")
  df.withColumn("id",monotonically_increasing_id())
    .select('value, ('id%3+1).as("id"), 'id)
    .show()


  df.createTempView("data123")
  spark.sql("select value, id%3+1 from (select value, monotonically_increasing_id() as id from data123) x").show()


  val df3 = Seq("asdas","asdsa","123","a").toDF("value")
  df3.select('value, lit(length('value)+1), lit(monotonically_increasing_id().as("id"))).show()


}
/*
a
b
c
d
e
f
g
(a,1,0)
(b,2,1)
(c,3,2)
(d,1,3)
(e,2,4)
(f,3,5)
(g,1,6)
+-----+---+---+
|value| id| id|
+-----+---+---+
|    a|  1|  0|
|    b|  2|  1|
|    c|  3|  2|
|    d|  1|  3|
|    e|  2|  4|
|    f|  3|  5|
|    g|  1|  6|
+-----+---+---+
 */
