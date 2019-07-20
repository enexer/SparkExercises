package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object CreateDataFrames extends InitSpark {

  val df2 = List(Row(1, 1))
  val fields = Seq(StructField("ok1", DataTypes.IntegerType),
    StructField("ok2", DataTypes.IntegerType))

  val struct = new StructType()
    .add("ok", DataTypes.IntegerType)
    .add("ok2", DataTypes.IntegerType)

  val someSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )


 // spark.createDataFrame(df2,struct).printSchema()
  // spark.createDataFrame(sc.parallelize(df2),someSchema)

}
