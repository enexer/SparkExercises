package sparkbasics

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateDataFrame extends InitializeSpark {

  val someData = Seq(
    Row(8, "bat"),
    Row(64, "mouse"),
    Row(-27, "horse")
  )

  val someSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )

  val someDF = spark.createDataFrame(
    spark.sparkContext.parallelize(someData),
    StructType(someSchema)
  )

  someDF.printSchema()
  someDF.show()

}
