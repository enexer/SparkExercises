package sparkbasics

object IsInAndWhenOrOtherwise extends InitializeSpark {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val sourceDF = Seq(
    (5),
    (14),
    (19),
    (75)
  ).toDF("age")

  val actualDF = sourceDF.withColumn(
    "age_category",
    when(col("age").between(13, 19), "teenager").otherwise(
      when(col("age") <= 7, "young child").otherwise(
        when(col("age") > 65, "elderly")
      )
    )
  )
  actualDF.show()


  // CHECK IF ROW VALUE IS IN LIST
  val correctAge = List(5,19)
  val isInDF = sourceDF.withColumn("isIn?", 'age.isin(correctAge: _*))
  isInDF.show()
}
/*
+---+------------+
|age|age_category|
+---+------------+
|  5| young child|
| 14|    teenager|
| 19|    teenager|
| 75|     elderly|
+---+------------+

+---+-----+
|age|isIn?|
+---+-----+
|  5| true|
| 14|false|
| 19| true|
| 75|false|
+---+-----+
 */
