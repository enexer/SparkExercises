package sparkbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SaveRDDAsCSV {
  def main(args: Array[String]): Unit = {
    // INFO DISABLED
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    val conf: SparkConf = new SparkConf()
      .setAppName("testApp")
      .setMaster("local")

    val sc: SparkContext = new SparkContext(conf)
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df2 = ss.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/googleplaystore.csv")


    val df3 = df2.select("Size", "Content Rating", "Reviews")
    df3.printSchema()
    val rdd: RDD[String] = df3.rdd
      .map(s => {
        val x = s.schema.fields.size
        var str = ""
        for (z <- 0 to x - 1) {
          if (z == x - 1) {
            str = str + "" + s.getString(z)
          } else {
            str = str + "" + s.getString(z) + ","
          }
        }
        str
      })

    //rdd.take(10).foreach(s=> println(s))
    //rdd.foreach(s => println(s))
    val rdd2 = rdd.repartition(4)
    println(rdd2.getNumPartitions)
    val rdd3 = rdd2.coalesce(1)
    println(rdd3.getNumPartitions)
    //rdd.coalesce(1).saveAsTextFile("data/okkk")
  }
}
