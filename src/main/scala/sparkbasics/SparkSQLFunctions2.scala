package sparkbasics

import init.InitSpark

object SparkSQLFunctions2 extends InitSpark{

  val df =  spark.read.option("inferSchema","true").option("header","true").csv("data/north_pole/data.csv")
  df.printSchema()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  df.groupBy('station).agg(max('temp), min('temp), max('date), min('date)).show()
  // EQUAL TO BELOW
  //df.createTempView("data")
  //spark.sql("select station, MAX(temp), MIN(temp), MAX(date), MIN(date) from data group by station").show()




}
