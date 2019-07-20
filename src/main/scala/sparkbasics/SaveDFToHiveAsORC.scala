package sparkbasics

import init.InitSpark

object SaveDFToHiveAsORC extends InitSpark {

  spark.sql("show tables").show()

  val df = spark.range(5) //.show()

  df.write.format("parquet").mode("append").saveAsTable("data")
  df.write.format("parquet").mode("append").saveAsTable("data")

  //spark.sql("create table data2 stored as parquet as (select * from data)")

  spark.sql("show tables").show()
  spark.sql("select * from data").show()
}
/*
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
+--------+---------+-----------+

+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
| default|     data|      false|
+--------+---------+-----------+

+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
 */

// in spark shell with ORC format


/*
scala> val spark3 = SparkSession.builder().enableHiveSupport().getOrCreate()
spark3: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@7187078a

scala> spark3.range(5)
res3: org.apache.spark.sql.Dataset[Long] = [id: bigint]

scala> res3.write.format("orc").mode("append").saveAsTable("data")

scala> spark.sql("show tables").show()
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
| default|     data|      false|
+--------+---------+-----------+


scala> spark.sql("select * from data").show()
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+


scala> res3.write.format("orc").mode("append").saveAsTable("data")

scala> spark.sql("select * from data").show()
+---+
| id|
+---+
|  0|
|  0|
|  1|
|  1|
|  2|
|  2|
|  3|
|  4|
|  3|
|  4|
+---+
 */