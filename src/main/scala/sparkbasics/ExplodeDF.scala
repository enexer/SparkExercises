package sparkbasics

object ExplodeDF extends InitializeSpark {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val data = Seq("a,b,c","d,e,f","a,b,e","a,a,c").toDF()
  data.show()

  data.select(split('value,",").alias("split")).show()
  data.select(explode(split('value,",")).alias("explode")).show()

  data.select(explode(split('value,",")).as("val"))
    .groupBy('val)
    .count()
    .orderBy('count.desc)
    .show()

}

//+-----+
//|value|
//+-----+
//|a,b,c|
//|d,e,f|
//|a,b,e|
//|a,a,c|
//+-----+
//
//+---------+
//|    split|
//+---------+
//|[a, b, c]|
//|[d, e, f]|
//|[a, b, e]|
//|[a, a, c]|
//+---------+
//
//+-------+
//|explode|
//+-------+
//|      a|
//|      b|
//|      c|
//|      d|
//|      e|
//|      f|
//|      a|
//|      b|
//|      e|
//|      a|
//|      a|
//|      c|
//+-------+
//
//+---+-----+
//|val|count|
//+---+-----+
//|  a|    4|
//|  e|    2|
//|  c|    2|
//|  b|    2|
//|  f|    1|
//|  d|    1|
//+---+-----+
