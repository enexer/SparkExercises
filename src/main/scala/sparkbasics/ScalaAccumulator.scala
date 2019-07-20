package sparkbasics

import java.util.Scanner

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object ScalaAccumulator extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("ScalaAccumulator")
  val sc = new SparkContext(sparkConf)
  val ss = SparkSession.builder().getOrCreate()

  val acc: LongAccumulator = sc.longAccumulator("accumulator123")
  val ds  = ss.range(100)
  ds.foreach(s => acc.add(1L))
  println(acc.value)
  ds.foreach(s => acc.add(2L))
  println(acc.value)
  new Scanner(System.in).nextInt
}
