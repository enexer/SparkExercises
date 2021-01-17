package analytics

import sparkbasics.CSVToSQLInsertValues.spark
import init.InitSpark
import org.apache.spark.sql.types.DataTypes

object Us13 extends InitSpark{

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val ds = spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .csv("Downloads/Book1.csv")

  val mode = udf((v: String) => {
    val ok = "'" + v + "'"
    if (ok == "'null'") "NULL" else ok
  }, DataTypes.StringType)

  val mode2 = udf((v: String) => {
    "(" + v + "),"
  }, DataTypes.StringType)


  //val ok = ds.columns.map(s=>mode($"Confirmation"))
  ds.printSchema()


}
