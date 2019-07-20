package sparkbasics

import init.InitSpark

object TestJoins extends InitSpark {


  case class Row(id: Int, value: String)
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val r1 = Seq(Row(1, "A1"), Row(2, "A2"), Row(3, "A3"), Row(4, "A4")).toDS()
  val r2 = Seq(Row(3, "A3_1"), Row(4, "A4_1"), Row(4, "A4_1"), Row(5, "A5_1"), Row(6, "A6_1")).toDS()

  r1.show()
  r1.printSchema()
  r2.show()
  r2.printSchema()

  val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")

  joinTypes foreach {joinType =>
    println(s"${joinType.toUpperCase()} JOIN")
    r1.join(right = r2, Seq("id"), joinType).orderBy("id").show()
  }

}
/*
+---+-----+
| id|value|
+---+-----+
|  1|   A1|
|  2|   A2|
|  3|   A3|
|  4|   A4|
+---+-----+

root
 |-- id: integer (nullable = false)
 |-- value: string (nullable = true)

+---+-----+
| id|value|
+---+-----+
|  3| A3_1|
|  4| A4_1|
|  4| A4_1|
|  5| A5_1|
|  6| A6_1|
+---+-----+

root
 |-- id: integer (nullable = false)
 |-- value: string (nullable = true)

INNER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
+---+-----+-----+

OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
|  5| null| A5_1|
|  6| null| A6_1|
+---+-----+-----+

FULL JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
|  5| null| A5_1|
|  6| null| A6_1|
+---+-----+-----+

FULL_OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
|  5| null| A5_1|
|  6| null| A6_1|
+---+-----+-----+

LEFT JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
+---+-----+-----+

LEFT_OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
+---+-----+-----+

RIGHT JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
|  5| null| A5_1|
|  6| null| A6_1|
+---+-----+-----+

RIGHT_OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  3|   A3| A3_1|
|  4|   A4| A4_1|
|  4|   A4| A4_1|
|  5| null| A5_1|
|  6| null| A6_1|
+---+-----+-----+

LEFT_SEMI JOIN
+---+-----+
| id|value|
+---+-----+
|  3|   A3|
|  4|   A4|
+---+-----+

LEFT_ANTI JOIN
+---+-----+
| id|value|
+---+-----+
|  1|   A1|
|  2|   A2|
+---+-----+
 */
