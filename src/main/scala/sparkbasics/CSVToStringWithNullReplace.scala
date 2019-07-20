package sparkbasics

import init.InitSpark

object CSVToStringWithNullReplace extends InitSpark {

  import spark.implicits._

  val df = Seq(("a", null, "b"), (null, "y", null)).toDF("c0", "c1", "c2")
  df.show()

  import org.apache.spark.sql.functions._
  // add to each column some characters
  val columns1 = df.columns.map(df(_)).map(concat(lit("~"),_,lit("~")))
  val mapped = df.select(columns1:_*)
  mapped.show()
  // fill null values
  val filled = mapped.na.fill("***")
  // add 2 new columns using lit()
  val columns2 = lit("<") +: filled.columns.map(filled(_)) :+ lit(">")
  // concatenate
  val concatWs = filled.select(concat_ws("_", columns2: _*).as("result"))
  concatWs.show()
  concatWs.printSchema()

  // error ???
  //df.na.fill("***").select(concat(filled.columns.map(filled(_)):_*)).show()
}
/*
+----+----+----+
|  c0|  c1|  c2|
+----+----+----+
|   a|null|   b|
|null|   y|null|
+----+----+----+

+----------------+----------------+----------------+
|concat(~, c0, ~)|concat(~, c1, ~)|concat(~, c2, ~)|
+----------------+----------------+----------------+
|             ~a~|            null|             ~b~|
|            null|             ~y~|            null|
+----------------+----------------+----------------+

+---------------+
|         result|
+---------------+
|<_~a~_***_~b~_>|
|<_***_~y~_***_>|
+---------------+

root
 |-- result: string (nullable = false)

 */