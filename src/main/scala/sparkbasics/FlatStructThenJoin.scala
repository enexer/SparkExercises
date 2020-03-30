package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

object FlatStructThenJoin extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df1 = Seq(("A", 1, "@", "2020-12-12")).toDF("ACCT_NO", "SUBID", "MCODE", "NewClosedDate")
  df1.printSchema()
  df1.show()

  val arrayStructData = Seq(
    Row(List(Row("A", 1, "@", "2000-11-11", "2012-12-12")))
  )

  val arrayStructSchema = new StructType()
    .add("accountlinks", ArrayType(new StructType()
      .add("acctno", StringType)
      .add("subid", IntegerType)
      .add("mcode", StringType)
      .add("openeddate", StringType)
      .add("closeddate", StringType)
    ))

  val df2 = spark.createDataFrame(spark.sparkContext
    .parallelize(arrayStructData), arrayStructSchema)

  df2.printSchema()
  df2.show(false)

  val df3 = df2.select(explode('accountlinks)).select("col.*")
  df3.show()
  df3.printSchema()

  val df4 = df3.join(df1.toDF("acctno","subid","mcode","NewClosedDate"), Seq("acctno","subid","mcode"), "left")
    .select("acctno","subid","mcode","openeddate","NewClosedDate")
    .toDF("acctno","subid","mcode","openeddate","closeddate")

  df4.show()
  df4.printSchema()
  /*
  root
 |-- ACCT_NO: string (nullable = true)
 |-- SUBID: integer (nullable = false)
 |-- MCODE: string (nullable = true)
 |-- NewClosedDate: string (nullable = true)

+-------+-----+-----+-------------+
|ACCT_NO|SUBID|MCODE|NewClosedDate|
+-------+-----+-----+-------------+
|      A|    1|    @|   2020-12-12|
+-------+-----+-----+-------------+

root
 |-- accountlinks: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- acctno: string (nullable = true)
 |    |    |-- subid: integer (nullable = true)
 |    |    |-- mcode: string (nullable = true)
 |    |    |-- openeddate: string (nullable = true)
 |    |    |-- closeddate: string (nullable = true)

+-----------------------------------+
|accountlinks                       |
+-----------------------------------+
|[[A, 1, @, 2000-11-11, 2012-12-12]]|
+-----------------------------------+

+------+-----+-----+----------+----------+
|acctno|subid|mcode|openeddate|closeddate|
+------+-----+-----+----------+----------+
|     A|    1|    @|2000-11-11|2012-12-12|
+------+-----+-----+----------+----------+

root
 |-- acctno: string (nullable = true)
 |-- subid: integer (nullable = true)
 |-- mcode: string (nullable = true)
 |-- openeddate: string (nullable = true)
 |-- closeddate: string (nullable = true)

+------+-----+-----+----------+----------+
|acctno|subid|mcode|openeddate|closeddate|
+------+-----+-----+----------+----------+
|     A|    1|    @|2000-11-11|2020-12-12|
+------+-----+-----+----------+----------+

root
 |-- acctno: string (nullable = true)
 |-- subid: integer (nullable = true)
 |-- mcode: string (nullable = true)
 |-- openeddate: string (nullable = true)
 |-- closeddate: string (nullable = true)


Process finished with exit code 0

   */
}
