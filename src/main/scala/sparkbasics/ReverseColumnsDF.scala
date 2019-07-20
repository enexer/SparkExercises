package sparkbasics

import init.InitSpark

object ReverseColumnsDF extends InitSpark{
  import spark.implicits._
  val data = Seq((1,2,3),(1,5,6),(7,8,9)).toDF("a1","a2","a3")
  data.show()

  val cols = data.columns.map(s=>data(s)).reverse
  cols.foreach(print(_))

  data.select(cols: _*).show()
}
/*
+---+---+---+
| a1| a2| a3|
+---+---+---+
|  1|  2|  3|
|  1|  5|  6|
|  7|  8|  9|
+---+---+---+

a3a2a1+---+---+---+

| a3| a2| a1|
+---+---+---+
|  3|  2|  1|
|  6|  5|  1|
|  9|  8|  7|
+---+---+---+
 */