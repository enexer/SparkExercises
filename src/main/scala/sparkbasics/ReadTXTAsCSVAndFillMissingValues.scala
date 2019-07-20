package sparkbasics

import init.InitSpark

object ReadTXTAsCSVAndFillMissingValues extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df1 = spark.read
    .option("delimiter", ":")
    .schema("col1 int, col2 int, col3 int")
    .csv("data/missing.txt")
  df1.show()
  df1.printSchema()

  val df2 = df1.select(df1.columns.map(df1(_).cast("string")): _*)
    .na.fill("00001") ///  fill(0) -> null // fill("0") -> 0 ///  WHY?
    //.select(df1.columns.map(df1(_).cast("decimal")): _*)  // error
    .select(expr("cast(col3 as decimal)"),expr("cast(col1 as decimal)")) // but this works

  df2.show()
  df2.printSchema()

  df2.select(concat_ws(",",df2.columns.map(df2(_)):_*).as("a1"))
    .select(split('a1,",").as("a1"))
    .select(explode('a1))
    .show()//.select(explode('a1)).show()


}
//+-----+------+-----+
//| col1|  col2| col3|
//+-----+------+-----+
//| 1231|123123| 2133|
//| null|  null| null|
//|12312|    99| 2131|
//| null|213123| null|
//|12312|  null|21312|
//+-----+------+-----+
//
//root
//|-- col1: integer (nullable = true)
//|-- col2: integer (nullable = true)
//|-- col3: integer (nullable = true)
//
//+-----+-----+
//| col3| col1|
//+-----+-----+
//| 2133| 1231|
//|    1|    1|
//| 2131|12312|
//|    1|    1|
//|21312|12312|
//+-----+-----+
//
//root
//|-- col3: decimal(10,0) (nullable = true)
//|-- col1: decimal(10,0) (nullable = true)
//
//+-----+
//|  col|
//+-----+
//| 2133|
//| 1231|
//|    1|
//|    1|
//| 2131|
//|12312|
//|    1|
//|    1|
//|21312|
//|12312|
//+-----+
