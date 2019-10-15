package sparkbasics

import org.apache.spark.sql.expressions.Window

object DataFrameTo2DArray extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val myDataframe = sc.parallelize(List("a", "b", "c", "d", "e")).toDF("value")
  val withIndex = myDataframe.select(row_number().over(Window.orderBy('value)).as("index").cast("INT"), '*)

  withIndex.show()

  myDataframe.foreach { row =>
    for (i <- 0 until (row.length)) {
      val rowNum = row.get(0)
      val colNum = i
    }
  }


  val list: Array[Array[String]] = withIndex
    .select(concat_ws(",", withIndex.columns.map(withIndex(_)): _*))
    .map(s => s.getString(0))
    .collect()
    .map(s => s.toString.split(","))

  for (elem <- 0 until  list.length) {
    for (elem2 <- 0 until list.apply(elem).length) {
      println(list.apply(elem).apply(elem2),", row:"+elem+", col:"+elem2)
    }
  }

  /*
+-----+-----+
|index|value|
+-----+-----+
|    1|    a|
|    2|    b|
|    3|    c|
|    4|    d|
|    5|    e|
+-----+-----+

(1,, row:0, col:0)
(a,, row:0, col:1)
(2,, row:1, col:0)
(b,, row:1, col:1)
(3,, row:2, col:0)
(c,, row:2, col:1)
(4,, row:3, col:0)
(d,, row:3, col:1)
(5,, row:4, col:0)
(e,, row:4, col:1)


   */
}
