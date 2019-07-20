package sparkbasics

import init.InitSpark

object RemoveRowsWithNulls extends InitSpark {

  val df = spark.createDataFrame(Seq(
    (1, None, Some(5)),
    (2, Some(2), Some(5)),
    (3, Some(3), None),
    (4, None, Some(5)),
    (25, None, None)
  )).toDF("c0", "c1", "c2")

  df.show()

  // Drops rows containing null or NaN values.
  df.na.drop().show()
  // If how is "any", then drop rows containing any null or NaN values.
  df.na.drop("any").show()
  // If how is "all", then drop rows only if every column is null or NaN for that row.
  df.na.drop("all").show()
  // Drops rows containing any null or NaN values in the specified columns.
  df.na.drop(Seq("c2")).show()
  // Drops rows containing only null or NaN values in the specified columns.
  df.na.drop("all", Seq("c1", "c2")).show()


  // equivalent to df.na.drop().show()
  df.filter(s => {
    var b = true
    for (x <- 0 until s.size) {
      if (s.getAs[AnyRef](x) == null) {  // or s.getAs[java.lang.Object]
        b = false
      }
    }
    b
  }).toDF("A1","A2","A3").show()
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
|  2|  2|  5|
+---+---+---+

+---+---+---+
| c0| c1| c2|
+---+---+---+
|  2|  2|  5|
+---+---+---+

+---+----+----+
| c0|  c1|  c2|
+---+----+----+
|  1|null|   5|
|  2|   2|   5|
|  3|   3|null|
|  4|null|   5|
| 25|null|null|
+---+----+----+

+---+----+---+
| c0|  c1| c2|
+---+----+---+
|  1|null|  5|
|  2|   2|  5|
|  4|null|  5|
+---+----+---+

+---+----+----+
| c0|  c1|  c2|
+---+----+----+
|  1|null|   5|
|  2|   2|   5|
|  3|   3|null|
|  4|null|   5|
+---+----+----+

+---+---+---+
| A1| A2| A3|
+---+---+---+
|  2|  2|  5|
+---+---+---+
 */