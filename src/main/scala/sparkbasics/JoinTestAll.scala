package sparkbasics

import init.InitSpark

object JoinTestAll extends InitSpark {

  // AVAILABLE JOINING CONDITIONS :
  // inner, cross, outer, full,
  // full_outer, left, left_outer,
  // right, right_outer, left_semi, left_anti.

  // with outer joins no differences ???
  import spark.implicits._

  val d1 = 1 to 6
  val d2 = 3 to 9
  val df1 = d1.toDF("id")
  val df2 = d2.toDF("id")

  printX("DF1")
  df1.show()
  printX("DF2")
  df2.show()

  // INNER JOIN
  df1.join(df2).where(df1("id") === df2("id")).show()
  df1.join(df2, "id").show()
  df1.join(df2, df1("id") === df2("id")).show()


  // USING IMPLICITS (USE DIFFERENT COL NAMES !!!)
  val df11 = d1.toDF("id1")
  val df22 = d2.toDF("id2")
  df11.join(df22, 'id1 === 'id2).show() /// ERROR if both col names are equal
  df11.join(df22, $"id1" === $"id2").show() // ERROR if both col names are equal


  // LEFT JOIN
  println("LEFT JOIN")
  df1.join(df2, df1("id") === df2("id"), "left").show()
  // RIGHT JOIN
  println("RIGHT JOIN")
  df1.join(df2, df1("id") === df2("id"), "right").show()
  // FULL JOIN
  println("FULL OUTER")
  df1.join(df2, df1("id") === df2("id"), "full").show()
  // CROSS
  println("CROSS")
  df1.join(df2, df1("id") === df2("id"), "cross").show()
  // LEFT ANTI
  println("LEFT ANTI")
  df1.join(df2, df1("id") === df2("id"), "left_anti").show()



  //df1.join(df2, df1("id") === df2("id") && df1("id2") === df2("id2"), "left").show()

}
/*
=========>DF1
+---+
| id|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
+---+

=========>DF2
+---+
| id|
+---+
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
+---+

+---+---+
| id| id|
+---+---+
|  3|  3|
|  4|  4|
|  5|  5|
|  6|  6|
+---+---+

+---+
| id|
+---+
|  3|
|  4|
|  5|
|  6|
+---+

+---+---+
| id| id|
+---+---+
|  3|  3|
|  4|  4|
|  5|  5|
|  6|  6|
+---+---+

+---+---+
|id1|id2|
+---+---+
|  3|  3|
|  4|  4|
|  5|  5|
|  6|  6|
+---+---+

+---+---+
|id1|id2|
+---+---+
|  3|  3|
|  4|  4|
|  5|  5|
|  6|  6|
+---+---+

LEFT JOIN
+---+----+
| id|  id|
+---+----+
|  1|null|
|  2|null|
|  3|   3|
|  4|   4|
|  5|   5|
|  6|   6|
+---+----+

RIGHT JOIN
+----+---+
|  id| id|
+----+---+
|   3|  3|
|   4|  4|
|   5|  5|
|   6|  6|
|null|  7|
|null|  8|
|null|  9|
+----+---+

FULL OUTER
+----+----+
|  id|  id|
+----+----+
|   1|null|
|   6|   6|
|   3|   3|
|   5|   5|
|null|   9|
|   4|   4|
|null|   8|
|null|   7|
|   2|null|
+----+----+

CROSS
+---+---+
| id| id|
+---+---+
|  3|  3|
|  4|  4|
|  5|  5|
|  6|  6|
+---+---+

LEFT ANTI
+---+
| id|
+---+
|  1|
|  2|
+---+
 */
