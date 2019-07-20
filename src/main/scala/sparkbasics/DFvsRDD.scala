package sparkbasics

import init.InitSpark

object DFvsRDD extends InitSpark {

  import spark.implicits._

  val data = Seq(
    ("a1", "AAA", "6233"),
    ("a2", "CCC", "1233"),
    ("a3", "AAA", "4233"),
    ("a4", "BBB", "6633"),
    ("a5", "AAA", "4233"),
    ("a6", "BBB", "8233")
  ).toDF("name", "state", "balance")

  val df = data.select('name, 'state, 'balance.cast("integer"))

  df.show()
  df.printSchema()

  df.groupBy('state).count().show()

  import org.apache.spark.sql.functions._

  df.where('balance > 2000)
    .groupBy('state)
    .agg(avg('balance).as("avg"), count('balance).as("count"))
    .show()

  case class Bank(name: String, state: String, balance: Int)

  df.as[Bank].rdd
    .filter(_.balance > 2000)
    .map(s => (s.state, (s.balance, 1L)))
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .mapValues(s => s._1 / s._2)
    .foreach(println(_))

}
/*
+----+-----+-------+
|name|state|balance|
+----+-----+-------+
|  a1|  AAA|   6233|
|  a2|  CCC|   1233|
|  a3|  AAA|   4233|
|  a4|  BBB|   6633|
|  a5|  AAA|   4233|
|  a6|  BBB|   8233|
+----+-----+-------+

root
 |-- name: string (nullable = true)
 |-- state: string (nullable = true)
 |-- balance: integer (nullable = true)

+-----+-----+
|state|count|
+-----+-----+
|  CCC|    1|
|  BBB|    2|
|  AAA|    3|
+-----+-----+

+-----+-----------------+-----+
|state|              avg|count|
+-----+-----------------+-----+
|  BBB|           7433.0|    2|
|  AAA|4899.666666666667|    3|
+-----+-----------------+-----+

(AAA,4899)
(BBB,7433)
 */