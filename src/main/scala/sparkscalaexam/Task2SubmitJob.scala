package sparkscalaexam

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//export SPARK_MAJOR_VERSION=2
/**
  * 1. creating jar - run sbt/mvn package or create artifact with compile output
  *
  * 2. submitting app
  *
  * .\spark-submit --master spark://10.126.10.106:7077 \
  * --deploy-mode cluster \    // result available on spark_worker/stdout
  * --class sparkscala.Task2SubmitJob \  //  package_name.your_app_main_class
  * C:\Users\sparkscala.jar   // jar
  */

object Task2SubmitJob {
  def main(args: Array[String]) = {

    /*
    ---DataFrame is simply a type alias of Dataset[Row]---

    Conceptually, consider DataFrame as an alias
    for a collection of generic objects Dataset
    [Row], where a Row is a generic untyped JVM
    object.Dataset, by contrast, is a collection
    of strongly - typed JVM objects, dictated
    by a case class you define in Scala or a
    class in Java
    */

    val conf = new SparkConf()
      .setAppName("testApp")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val ss = SparkSession.builder().config(conf).getOrCreate()

    val rdd1 = sc.parallelize(List(1, 2, 3, 4))
    println(rdd1.count())
  }
}
