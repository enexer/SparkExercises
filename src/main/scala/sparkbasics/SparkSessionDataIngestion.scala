package sparkbasics

import org.apache.spark.sql.types.DataTypes

object SparkSessionDataIngestion extends InitializeSpark {

  val df = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/iris.csv")

  df.printSchema()
  df.show()

  import org.apache.spark.sql.functions._

  val sorted = df.orderBy(desc("sepal_length"))
  // sorted.show()
  //sorted.coalesce(1).write.json("data/heh")
  //sorted.coalesce(1).write.option("header","true").csv("data/hCSV")
  //sorted.coalesce(1).write.text("data/ggg")



  //   CREATE NEW DF FROM OTHER DF - CAST AND RENAME COLUMN
  import spark.implicits._
  val newDF = sorted.select('sepal_length.cast("string").as("hehe"),
    'sepal_length>7, 'sepal_length.contains('9'))
  newDF.printSchema()
  newDF.show()
  /////////////////////////////////

  val NewUDFFunction = udf((v: Double) => v+1.0)
  sorted.withColumn("ok",
    'sepal_length+'sepal_width*'petal_width+2.1/0.5+NewUDFFunction('sepal_length))
    .show()
  //////////////////////
  newDF.filter($"contains(sepal_length, 9)".equalTo("true")).show()


}
