package sparkbasics

import init.InitSpark

object RowsToCols extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df = spark.read.csv("data/rows_to_cols.csv").toDF("region", "key", "val")
  df.printSchema()
  df.show()

  val regex = "(\\d{1,})" // "^[^_]+"

  df.groupBy('region, regexp_extract('key, regex, 1)) // select until _ character
    .agg('region, collect_list('key).as("key"), collect_list('val).as("val"))
    .select('region,
    'key.getItem(0).as("key"),
    'val.getItem(0).as("val"),
    'val.getItem(1).as("Category"),
    'val.getItem(2).as("Unit")
  ).show()

  /*
  root
 |-- region: string (nullable = true)
 |-- key: string (nullable = true)
 |-- val: string (nullable = true)

+------+----------------+-----+
|region|             key|  val|
+------+----------------+-----+
|Sample|          row1_9|    6|
|Sample|  row1_9category|Cat 1|
|Sample|      row1_9Unit|   Kg|
|Sample|        row_2_33|    4|
|Sample|row_2_33category|Cat 2|
|Sample|     row2_33Unit|  ltr|
+------+----------------+-----+

+------+--------+---+--------+----+
|region|     key|val|Category|Unit|
+------+--------+---+--------+----+
|Sample|  row1_9|  6|   Cat 1|  Kg|
|Sample|row_2_33|  4|   Cat 2| ltr|
+------+--------+---+--------+----+
   */
}
