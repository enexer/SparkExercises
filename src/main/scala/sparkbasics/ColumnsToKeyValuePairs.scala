package sparkbasics

import init.InitSpark

object ColumnsToKeyValuePairs extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._


  val df = spark.sqlContext.createDataFrame(Seq(("1", "black", "apple", "89998", "****", "$$$$$")))
    .toDF("id", "column1", "column2", "column3", " column4", "column5")
  df.show

  val df2 = df.select(array('*).as("v"), lit(df.columns).as("k"))
    .select('v.getItem(0).as("id"), map_from_arrays('k,'v).as("map"))
    .select('id, explode('map))

  df2.show(10)
/*
+---+-------+-------+-------+--------+-------+
| id|column1|column2|column3| column4|column5|
+---+-------+-------+-------+--------+-------+
|  1|  black|  apple|  89998|    ****|  $$$$$|
+---+-------+-------+-------+--------+-------+

+---+--------+-----+
| id|     key|value|
+---+--------+-----+
|  1|      id|    1|
|  1| column1|black|
|  1| column2|apple|
|  1| column3|89998|
|  1| column4| ****|
|  1| column5|$$$$$|
+---+--------+-----+
 */
}
