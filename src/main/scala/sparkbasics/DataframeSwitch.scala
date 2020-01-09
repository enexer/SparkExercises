package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Column

object DataframeSwitch extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df = spark.range(6).toDF("val")
  val conditions = Array(("two", "2"), ("three", "3"), ("four", "4"))
  df.show()

  def switch(s: Column, conditions: Array[(String,String)]): Column = {
    var col: Column = null
    for (i <- 0 to conditions.length - 1) {
      val tp = conditions.apply(i)
      if (i == 0) col = when(s.equalTo(tp._2), tp._1)
      else col = col.when(s.equalTo(tp._2), tp._1)
    }
    col = col.otherwise("other")
    col.explain(true)
    col
  }

  df.withColumn("type", switch('val,conditions)).show()

  /*
  +---+
  |val|
  +---+
  |  0|
  |  1|
  |  2|
  |  3|
  |  4|
  |  5|
  +---+

  CASE WHEN ('val = 2) THEN two WHEN ('val = 3) THEN three WHEN ('val = 4) THEN four ELSE other END
  +---+-----+
  |val| type|
  +---+-----+
  |  0|other|
  |  1|other|
  |  2|  two|
  |  3|three|
  |  4| four|
  |  5|other|
  +---+-----+
   */
}
