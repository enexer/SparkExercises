package init

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait InitSpark extends App{
  import org.apache.log4j.{Level, Logger}
  // INFO DISABLED
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("INFO").setLevel(Level.OFF)

  val conf: SparkConf = new SparkConf()
    .setAppName("testApp")
    .setMaster("local")


  val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


  def printX(s: Any) = println("=========>"+s)
}
