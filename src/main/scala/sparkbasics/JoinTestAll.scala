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



  df1.join(df2, df1("id") === df2("id") && df1("id2") === df2("id2"), "left").show()

}
