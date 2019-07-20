package sparkbasics

import init.InitSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes

/*
PROBLEM :    FORMAT CSV FILE TO SQL INSERT VALUES FORMAT, HANDLE NULL VALUES

          I0001,,
          SHOULD BE
          ('I0001',NULL,NULL),
 */
object CSVToSQLInsertValues extends InitSpark {


  import org.apache.spark.sql.functions._
  import spark.implicits._

  val ds = Seq(("a", null, "b"), (null, "y", null)).toDF("c0", "c1", "c2")

  print(ds.columns.length + ", length")

  val mode = udf((v: String) => {
    val ok = "'" + v + "'"
    if (ok == "'null'") "NULL" else ok
  }, DataTypes.StringType)

  val mode2 = udf((v: String) => {
    "(" + v + "),"
  }, DataTypes.StringType)

  val seq2 = ds.columns.map(s => mode(ds(s))).toSeq

  def udfOnAllColumns(df: DataFrame, udf: UserDefinedFunction): DataFrame = {
    val cols = df.columns.map(s => udf(df(s))).toSeq
    df.select(cols: _*)
  }

  val ds2 = udfOnAllColumns(ds, mode)
  ds2.show()

  val cols = ds2.columns.map(ds2.col(_))

  val ds3 = ds2.withColumn("final", concat_ws(",", cols: _*))
  ds3.show()

  val ds4 = ds3.select(mode2('final))
  ds4.show()

}
/*
3, length+-------+-------+-------+
|UDF(c0)|UDF(c1)|UDF(c2)|
+-------+-------+-------+
|    'a'|   NULL|    'b'|
|   NULL|    'y'|   NULL|
+-------+-------+-------+

+-------+-------+-------+-------------+
|UDF(c0)|UDF(c1)|UDF(c2)|        final|
+-------+-------+-------+-------------+
|    'a'|   NULL|    'b'| 'a',NULL,'b'|
|   NULL|    'y'|   NULL|NULL,'y',NULL|
+-------+-------+-------+-------------+

+----------------+
|      UDF(final)|
+----------------+
| ('a',NULL,'b'),|
|(NULL,'y',NULL),|
 */
