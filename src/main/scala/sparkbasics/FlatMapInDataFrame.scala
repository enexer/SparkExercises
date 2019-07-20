package sparkbasics

import init.InitSpark
/*
      count elements from text lines
 */
object FlatMapInDataFrame extends InitSpark{

  import spark.implicits._
  val data = Seq("a,a,a,b,b,a,c","e,e,c,a,b")

  // RDD
  println("USING RDD:")
  sc.parallelize(data)
    .flatMap(_.split(","))
    .map((_,1))
    .reduceByKey(_+_)
    .sortBy(_._2, false)
    .collect()
    .foreach(println(_))

  // DF
  println("USING DF:")
  import org.apache.spark.sql.functions._
  data.toDF("value")
    .select(explode(split($"value", ",")).alias("value"))
    .groupBy('value)
    .agg(count('value).alias("count"))
    .orderBy('count.desc)
    .show()


  //split
  //data.toDF("ok").select(split('ok, ",")).show()
  //explode
  //data.toDF("ok").select(split('ok, ",").as("ok")).select(explode('ok)).show()

/*
  USING RDD:
    (a,5)
    (b,3)
    (e,2)
    (c,2)
  USING DF:
    +-----+-----+
    |value|count|
    +-----+-----+
    |    a|    5|
    |    b|    3|
    |    e|    2|
    |    c|    2|
    +-----+-----+
*/
}
