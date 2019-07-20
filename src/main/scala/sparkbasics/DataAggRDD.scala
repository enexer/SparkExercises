package sparkbasics

import init.InitSpark

import scala.util.Random

object DataAggRDD extends InitSpark {

  val groups = Map(0 -> (0, 1), 1 -> (1, 1), 2 -> (2, 2), 3 -> (3, 3),
    4 -> (4, 3), 5 -> (5, 2), 6 -> (6, 1))
  val n = 1000000
  val (mu, sigma) = groups(5);

  println("mu" + mu + ", sigma:" + sigma)
  println(groups(5))


  val random = Random
  val data = sc.parallelize(for (i <- 1 to n) yield {
    val g = random.nextInt(7);
    val (mu, sigma) = groups(g);
    (g, mu + sigma * random.nextGaussian)
  })

  println(data.take(1).apply(0))

  //groups.foreach(println(_))
  printX("")

  // GROUP COUNT
  data.map(s=>{
    (s._1,(s._2,1))
  }).reduceByKey((a,b)=>{
    (0, a._2+b._2)
  }).map(s=>{
    (s._1,s._2._2)
  })
    .foreach(println(_))

  // GROUP COUNT
  data.countByKey().foreach(println(_))

  println(data.map(_._2).variance())
  println(data.map(_._2).mean())

  // GROUP AVG
  data.map(s=>(s._1,(s._2,1L)))
    .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
    .mapValues(a=>a._1/a._2)
    .foreach(println(_))


}

/*
mu5, sigma:2
(5,2)
(4,7.3104140365950965)
=========>
(4,142441)
(0,142958)
(1,143595)
(6,142618)
(3,142830)
(5,143060)
(2,142498)
(0,142958)
(5,143060)
(1,143595)
(6,142618)
(2,142498)
(3,142830)
(4,142441)
8.135860976912342
2.998023653992623
(4,3.988464622038522)
(0,0.0023455220707443024)
(1,0.9997022511561271)
(6,5.997854433538149)
(3,3.0056178447589765)
(5,5.000833743194601)
(2,2.0063547081023736)

 */