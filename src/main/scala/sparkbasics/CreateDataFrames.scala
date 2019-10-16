package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object CreateDataFrames extends InitSpark {

  val df2 = List(Row(1, 1))

  val fields = Seq(
    StructField("ok1", DataTypes.IntegerType),
    StructField("ok2", DataTypes.IntegerType)
  )

  val struct = new StructType()
    .add("ok", DataTypes.IntegerType)
    .add("ok2", DataTypes.IntegerType)

  val someSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )


  spark.createDataFrame(sc.parallelize(df2),StructType(fields)).printSchema()

  spark.createDataFrame(sc.parallelize(df2),struct).printSchema()

  spark.createDataFrame(sc.parallelize(df2),StructType(someSchema)).printSchema()
}
/*
root
 |-- ok1: integer (nullable = true)
 |-- ok2: integer (nullable = true)

root
 |-- ok: integer (nullable = true)
 |-- ok2: integer (nullable = true)

root
 |-- number: integer (nullable = true)
 |-- word: string (nullable = true)
 */
