package analytics

import sparkbasics.CSVToSQLInsertValues.{ds4, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes
import sparkbasics.InitializeSpark

object CSVToSQL2 extends InitializeSpark{

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val ds = spark.read
    .option("header", "true")
    .csv("data3/iflot.csv")

  ds.printSchema()
  ds.show()

  val columns1 = ds.columns.dropRight(4).map(ds(_))
  columns1.foreach(println(_))

  val dsOK = ds.select(columns1: _*)


  val mode = udf((v: String) => {
    val ok = "'" + v + "'"
    if (ok == "'null'") "NULL" else ok
  }, DataTypes.StringType)

  def udfOnAllColumns(df: DataFrame, udf: UserDefinedFunction): DataFrame ={
    val cols = df.columns.map(s=>udf(df(s))).toSeq
    df.select(cols: _*)
  }



  val mode2 = udf((v: String) => {
    "(" + v + ",NULL,NULL,NULL),"
  }, DataTypes.StringType)


  val ds2 = udfOnAllColumns(dsOK, mode)
  ds2.show()
  val cols = ds2.columns.map(ds2.col(_))
  val ds3 = ds2.select(concat_ws(",", cols: _*).as("final"))


  val ds4 = ds3.select(mode2('final))
  ds4.show()

  ds4.coalesce(1).write.text("data3/final3")







  //  ds.na.fill("NULL")
//    .withColumn("final", concat_ws(",", ))

}
