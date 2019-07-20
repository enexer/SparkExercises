package sparkbasics

import init.InitSpark

import scala.util.Random

object DataAgg extends InitSpark {

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

  import spark.implicits._

  val df = data.toDF("group", "value")

  import org.apache.spark.sql.functions._

  val df2 = df.groupBy('group)
    .agg(count('group).as("count"),
      avg('value).as("avg"),
      variance('value).as("variance"))
  df2.cache()
  df2.show()

  df.agg(count('group).as("count"),
      avg('value).as("avg"),
      variance('value).as("variance"))
    .show()

  df2.agg(sum('count).as("count"),
    avg('avg).as("avg"),
    avg('variance).as("variance"))
    .show()
}
/*
mu5, sigma:2
(5,2)
(5,4.053286011244968)
=========>
+-----+------+--------------------+------------------+
|group| count|                 avg|          variance|
+-----+------+--------------------+------------------+
|    1|143042|  1.0018121837031397|1.0006965839313562|
|    6|143020|   5.997179310218414|0.9996547169957353|
|    3|142952|  2.9958257080179393| 9.029511863657525|
|    5|142278|  4.9921676619410595|3.9921734856512447|
|    4|142938|   3.997669945740472| 9.004920193480613|
|    2|143134|   2.001686632686124| 3.984215401019354|
|    0|142636|-1.12543528457940...| 0.996970987111578|
+-----+------+--------------------+------------------+

+-------+------------------+---------------+
|  count|               avg|       variance|
+-------+------------------+---------------+
|1000000|2.9974650189752974|8.1337986718472|
+-------+------------------+---------------+

+-------+-----------------+-----------------+
|  count|              avg|         variance|
+-------+-----------------+-----------------+
|1000000|2.998032699825527|4.144020461692487|
+-------+-----------------+-----------------+
 */
