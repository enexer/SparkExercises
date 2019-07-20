package sparkbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


trait InitializeSpark extends App{
  // INFO DISABLED
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("INFO").setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("ok")
    .getOrCreate()
  val sc =spark.sparkContext

  def printX(a: Any): Unit = println("--->"+a)
}


