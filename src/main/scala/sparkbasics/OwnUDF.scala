package sparkbasics

import init.InitSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes

object OwnUDF extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val ds1 = sc.parallelize(List("avs", "oiu", "erp", "ree", "hqr")).toDF("firstCol")
  val ds2 = sc.parallelize(List(1,2,3,4)).toDF("firstCol")
  // APPLY OWN FUNCTION TO COLUMN ELEMENTS
  //UDF

  // FIRST OPTION
  val mode = udf((v: String) => v.toUpperCase.reverse.take(1).toString, DataTypes.StringType)
  val mode2 = udf((v: Int) => v+100, DataTypes.IntegerType)
 // val mode = udf((v: Array[String]) => v.apply(0).toUpperCase.reverse.take(1).toString+"_"+v.apply(0).toUpperCase.reverse.take(1).toString, DataTypes.StringType)

  // OR SECOND
  //val modeXX: String => String = _.toUpperCase()
  //val mode2 = udf(modeXX)

  ds1.withColumn("new", mode($"firstCol")).show()
  ds2.withColumn("new",mode2('firstCol)).show()

  def udfOnAllColumns(df: DataFrame, udf: UserDefinedFunction): DataFrame ={
    val cols = df.columns.map(s=>udf(df(s))).toSeq
    df.select(cols: _*)
  }

}
