package sparkbasics

import init.InitSpark
import org.apache.spark.sql.{Dataset, Row}

object FillRowsWithNulls extends InitSpark {

  val df = spark.createDataFrame(Seq(
    (1, None, Some(5)),
    (2, Some(2), Some(5)),
    (3, Some(3), None),
    (4, None, Some(5)),
    (25, None, None)
  )).toDF("c0", "c1", "c2")

  df.show()

  // Drops rows containing null or NaN values.
  df.na.fill(2).show()


  import spark.implicits._

  // equivalent to df.na.fill(2).show()
  def fillNullValues(df: Dataset[Row], value: Int): Dataset[Row] = {
    df.map(s => {
      var list: List[Int] = List()
      for (x <- 0 until s.size) {
        if (s.getAs[AnyRef](x) == null) { // or s.getAs[java.lang.Object]
          list = value :: list
        } else {
          list = s.getInt(x) :: list
        }
      }
      val t = list match {
        case List(a, b, c) => (a, b, c)
      }  // or (list.apply(0),list.apply(1),list.apply(2))
      t
    }).toDF("A1","A2","A3")
  }

  fillNullValues(df, 9999).show()


}
/*
+---+----+----+
| c0|  c1|  c2|
+---+----+----+
|  1|null|   5|
|  2|   2|   5|
|  3|   3|null|
|  4|null|   5|
| 25|null|null|
+---+----+----+

+---+---+---+
| c0| c1| c2|
+---+---+---+
|  1|  2|  5|
|  2|  2|  5|
|  3|  3|  2|
|  4|  2|  5|
| 25|  2|  2|
+---+---+---+

+----+----+---+
|  A1|  A2| A3|
+----+----+---+
|   5|9999|  1|
|   5|   2|  2|
|9999|   3|  3|
|   5|9999|  4|
|9999|9999| 25|
+----+----+---+
 */