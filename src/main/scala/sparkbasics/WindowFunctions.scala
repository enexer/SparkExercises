package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object WindowFunctions extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._


  val seq = List(Row("a", 1),Row("a", 2))
  val struct = new StructType()
      .add("id",StringType)
      .add("x", IntegerType)

  val df1 = spark.createDataFrame(
    sc.parallelize(seq),
    struct
  )

  df1.printSchema()
  df1.show()


  val df2 = Seq((1, 1),(2, 2),(3, 5),(4, 5),(5, 5)).toDF("id","x")
  df2.printSchema()
  df2.show()

  df2.createTempView("data")
  /// SPARK SQL
  spark.sql("select id, x,  sum(x) over (order by id) as spark_sql from data").show()
  /// DATAFRAME API
  import org.apache.spark.sql.expressions.Window
  df2.select('id, 'x, sum('x).over(Window.orderBy('id)).as("df_api")).show()

/*
+---+---+
| id|  x|
+---+---+
|  1|  1|
|  2|  2|
|  3|  5|
|  4|  5|
|  5|  5|
+---+---+

+---+---+---------+
| id|  x|spark_sql|
+---+---+---------+
|  1|  1|        1|
|  2|  2|        3|
|  3|  5|        8|
|  4|  5|       13|
|  5|  5|       18|
+---+---+---------+

+---+---+------+
| id|  x|df_api|
+---+---+------+
|  1|  1|     1|
|  2|  2|     3|
|  3|  5|     8|
|  4|  5|    13|
|  5|  5|    18|
+---+---+------+
 */


}
