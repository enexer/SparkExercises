package sparkbasics

import init.InitSpark
import org.apache.spark.sql.{Column, DataFrame}

object UnionColumns extends InitSpark{

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df = Seq("1,2","3,4","5,6")
    .toDF("columnToSplit")
    .withColumn("temp", split(col("columnToSplit"), "\\,"))
    .select(col("*") +: (0 until 2).map(i => col("temp").getItem(i).as(s"col$i")): _*)

  df.show()

  def unionColumns(df: DataFrame, cols: Array[Column]) : DataFrame = {
    df.select(array(cols: _*).as("val")).select(explode('val).as("unionCols"))
  }

  def unionColumns(df: DataFrame) : DataFrame = {
    unionColumns(df, df.columns.map(df(_)))
  }

  unionColumns(df.select('col0,'col1)).show()

  // or in spark sql
  df.createTempView("dataUC")
  spark.sql("select explode(array(col0, col1)) as unionCols from (select col0, col1 from dataUC) x").show()



}

/*
+-------------+------+----+----+
|columnToSplit|  temp|col0|col1|
+-------------+------+----+----+
|          1,2|[1, 2]|   1|   2|
|          3,4|[3, 4]|   3|   4|
|          5,6|[5, 6]|   5|   6|
+-------------+------+----+----+

+---------+
|unionCols|
+---------+
|        1|
|        2|
|        3|
|        4|
|        5|
|        6|
+---------+

+---------+
|unionCols|
+---------+
|        1|
|        2|
|        3|
|        4|
|        5|
|        6|
+---------+
 */