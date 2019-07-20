package sparkbasics

import init.InitSpark
import org.apache.spark.rdd.RDD

object PathFinder extends InitSpark {

  val points = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data2/points.csv")

  val range = 1;
  val target1 = Array(0.0, 0.0);
  val target2 = Array(10.0, 25.0);


  import spark.implicits._

  val points2 = points.drop('label)

  val rdd = points2.map(s => {
    var array = new Array[Double](s.size)
    for (i <- 0 to s.size - 1) {
      array(i) = s.getDouble(i)
    }
    array
  }).rdd

  val target = target2

  val rdd2 = closest(rdd,target,range)
  print("target: ")
  target.foreach(s=>print(s+","))
  println("\nrange: "+rdd2._2)
  rdd2._1.collect().foreach(s=>println(s(0)+", "+s(1)))


  def closest(rdd :RDD[Array[Double]], target:Array[Double], range: Double): (RDD[Array[Double]], Double) = {
    var range1 = range
    var count = 0
    val step = 1.0
    var rdd3 = sc.emptyRDD[Array[Double]]
    while (count==0){
      val rdd2 = rdd.map(s => {
        val dist = distanceEuclidean(s, target)
        (dist, s)
      }).filter(s => {
        s._1 <= range1 && s._1 != 0
      })
        .map(_._2)
      count=count+rdd2.count().toInt
      range1=range1+step
      rdd3=rdd2
      println("count: "+count+", range:"+range1)
    }
    (rdd3,range1)
  }


  def distanceEuclidean(t1: Array[Double], t2: Array[Double]) = {
    var sum: Double = 0.0;
    for (i <- 0 to t1.length-1) {
      sum += Math.pow((t1(i) - t2(i)), 2.0);
    }
    Math.sqrt(sum);
  }
}
/*
count: 0, range:2.0
count: 0, range:3.0
count: 0, range:4.0
count: 0, range:5.0
count: 0, range:6.0
count: 0, range:7.0
count: 0, range:8.0
count: 0, range:9.0
count: 0, range:10.0
count: 0, range:11.0
count: 0, range:12.0
count: 0, range:13.0
count: 0, range:14.0
count: 0, range:15.0
count: 0, range:16.0
count: 0, range:17.0
count: 3, range:18.0
target: 10.0,25.0,
range: 18.0
8.0, 8.8
8.25, 7.7
6.25, 9.25
6.95, 9.25

 */
