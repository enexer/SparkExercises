package sparkbasics

import init.InitSpark

object ShiftJoin extends InitSpark {

  import spark.implicits._

  val df = Seq(("a", "a", "a"), ("a", "b", "c"), ("b", "b", "c")).toDF("Col1","Col2","Col3")
  df.show()

  val ok = df.rdd.map(s => {
    var arr = new Array[(String, String)](s.size)
    for (i <- 0 to s.size - 1) {
      arr(i) = (s.getString(i), s.schema.fieldNames(i))
    }
    arr
  }).map(s => {
    for (i <- s) yield ((i._2, i._1), 1)
  }).flatMap(s => s)
    .reduceByKey(_ + _)
    .map(s => (s._1._1, s._1._2 + "=" + s._2))
    .reduceByKey(_ +","+ _)

  ok.foreach(println(_))

    val list = ok.collect()
    val cols = new Array[String](list.length)
    val vals = new Array[Array[String]](list.length)
    for (i <- 0 to list.length-1) {
      cols(i) = list.apply(i)._1.toString
      vals(i) = Array(list.apply(i)._2)
    }

    vals.toSeq.toDF().show()

  /*
  +----+----+----+
  |Col1|Col2|Col3|
  +----+----+----+
  |   a|   a|   a|
  |   a|   b|   c|
  |   b|   b|   c|
  +----+----+----+

  (Col1,a=2,b=1)
  (Col2,b=2,a=1)
  (Col3,a=1,c=2)
  +---------+
  |    value|
  +---------+
  |[a=2,b=1]|
  |[b=2,a=1]|
  |[a=1,c=2]|
  +---------+
   */
}
