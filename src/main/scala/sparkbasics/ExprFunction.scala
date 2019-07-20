package sparkbasics

import init.InitSpark

object ExprFunction extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val data = Seq("0000123", "00102012312", "0000001","123.41242121","999999.0000000009").toDF("val")
  data.show()
  val data2 = data
    .withColumn("toDecimal", expr("cast(val as decimal(15,2))"))
    .withColumn("toDate", expr("from_unixtime(val)").cast("date"))
    .withColumn("toDate2", expr("val").cast("date"))
    .withColumn("toInt", expr("cast(val as decimal)").cast("int"))

  data2.show()
  data2.printSchema()
}
/*
+-----------------+
|              val|
+-----------------+
|          0000123|
|      00102012312|
|          0000001|
|     123.41242121|
|999999.0000000009|
+-----------------+

+-----------------+------------+----------+-------+---------+
|              val|   toDecimal|    toDate|toDate2|    toInt|
+-----------------+------------+----------+-------+---------+
|          0000123|      123.00|1970-01-01|   null|      123|
|      00102012312|102012312.00|1973-03-26|   null|102012312|
|          0000001|        1.00|1970-01-01|   null|        1|
|     123.41242121|      123.41|1970-01-01|   null|      123|
|999999.0000000009|   999999.00|1970-01-12|   null|   999999|
+-----------------+------------+----------+-------+---------+

root
 |-- val: string (nullable = true)
 |-- toDecimal: decimal(15,2) (nullable = true)
 |-- toDate: date (nullable = true)
 |-- toDate2: date (nullable = true)
 |-- toInt: integer (nullable = true)
 */
