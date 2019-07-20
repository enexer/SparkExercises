package sparkbasics

import init.InitSpark

object ArrayTypeDF extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._


  // PROBLEM: add new column with words which exist in column 1 and given list/values

  val df = Seq(
    ("i like blue and red"),
    ("you pink and blue")
  ).toDF("word1")

  val actualDF = df.withColumn(
    "colors",
    array(
      when(col("word1").contains("blue"), "blue"),
      when(col("word1").contains("red"), "red"),
      when(col("word1").contains("pink"), "pink"),
      when(col("word1").contains("cyan"), "cyan")
    )
  )
  actualDF.show(truncate = false)


  //  ADD LOOP
  // concat_ws instead of array (remove null)

  val colors = Array("blue", "red", "pink", "cyan")
  val actualDF2 = df.withColumn(
    "colors",
    concat_ws(",",
      colors.map( c => when(col("word1").contains(c), c)): _* // vararg when need value*
    )
  )
  actualDF2.show(truncate = false)


}
