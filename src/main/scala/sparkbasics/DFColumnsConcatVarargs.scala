package sparkbasics

import org.apache.spark.sql.Row

object DFColumnsConcatVarargs extends InitializeSpark {

  import spark.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._


  // PROBLEM: transform 2-col DF to 1-col DF with concatenated values from selected/all columns

///  println(sc.parallelize(List(1)).map("d" + _ + "d").first())

  val data = List(Row(123, "b"),
    Row(123, "c"))

  val schema = StructType.apply(Seq(
    StructField("id", IntegerType),
    StructField("letter", StringType)))

  val df = spark.createDataFrame(sc.parallelize(data),schema)

  df.show()

  // SOLUTION 1 (CONCAT CHOOSEN COLUMNS):
  val concatDF = df.withColumn("concatCols", concat_ws(",", 'id,'letter))
  concatDF.drop("id","letter").show()

  // SOLUTION 2*** (USING VARARGS AND CONCAT ALL COLUMNS):
  val dfColumns = df.columns.map(s=>col(s))
  val concatDF2 = df.withColumn("concatCols", concat_ws(",", dfColumns: _*))
  // _*  -  vararg  if method param is String* and u wanna use List[String] here
  concatDF2.drop(df.columns: _*).show()
}
/*
+---+------+
| id|letter|
+---+------+
|123|     b|
|123|     c|
+---+------+

+----------+
|concatCols|
+----------+
|     123,b|
|     123,c|
+----------+

+----------+
|concatCols|
+----------+
|     123,b|
|     123,c|
+----------+
 */