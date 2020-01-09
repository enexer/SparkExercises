package sparkbasics

import org.apache.log4j.{Level, Logger}

object MultiFilter extends App {

  // INFO DISABLED
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("INFO").setLevel(Level.OFF)

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val data = spark.read.csv("data/multiFilter.csv").toDF("code1", "code2")

  data.withColumn("scenario",
    when('code1.isin("A", "B"), 1).otherwise(
      when('code1.isin("A", "D") && 'code2.isin("2","3"), 2).otherwise(
        when('code2==="2",3)
      )
    )
  ).show()


  data.withColumn("s1", when('code1.isin("A", "B"), 1).otherwise(0))
    .withColumn("s2",when('code1.isin("A", "D") && 'code2.isin("2","3"), 1).otherwise(0))
    .withColumn("s3",when('code2==="2",1).otherwise(0))
    .show()
}
