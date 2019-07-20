package sparkbasics

import org.apache.spark.sql.{Column, DataFrame}

object SparkSQLFunctions extends InitializeSpark {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

  List(
    ("I LIKE food"),
    ("   this    fun")
  ).toDF("words")
    .withColumn("count1", org.apache.spark.sql.functions.length('words))
    //.withColumn("clean_words", regexp_replace('words, "\\s+", ""))
    .withColumn("clean_words", removeAllWhitespace('words))
    .withColumn("count2", org.apache.spark.sql.functions.length('clean_words))
    .withColumn("sum", 'count1 + 'count2)
    .withColumn("all", concat_ws("+", 'count1, 'count2, 'sum))
    .withColumn("x", upper(lower('clean_words)))
    .withColumn("lit", lit(12 + "-" + 'clean_words))
    .transform(withAgePlusOne("sum","formTransform"))
    .show()


  // takes column than +1 and create new column with result ??
  def withAgePlusOne(ageColName: String, resultColName: String)(df: DataFrame): DataFrame = {
    df.withColumn(resultColName, col(ageColName) + 1)
  }




  //  def toLowerFun(str: String): Option[String] = {
  //    val s = Option(str).getOrElse(return None)
  //    Some(s.toLowerCase())
  //  }
  //
  //  val toLower = udf[Option[String], String](toLowerFun)
  //
  //  val df = List(
  //    ("HI ThErE"),
  //    ("ME so HAPPY"),
  //    (null)
  //  ).toDF("phrase")
  //
  //  df
  //    .withColumn(
  //      "lower_phrase",
  //      toLower(col("phrase"))
  //    )
  //    .show()

}
