package sparkbasics

object StringDFToArray extends InitializeSpark {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  // PROBLEM: transform lines to array with words, remove multiple spaces (delimiter is space)

  val df1 = List(" ok          ok  ok", "tok   no yes    ").toDF("id")
  df1.show()

  val df2 = df1
    .withColumn("id", regexp_replace('id, " +", " "))
      //.withColumn("id", ltrim('id))
    .withColumn("id", split('id, " "))
  df2.show()
}
