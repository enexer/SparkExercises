package sparkbasics

import init.InitSpark

object HowToReadNestedJSON extends InitSpark {

  /*
    if columns: array -> then columns[0]
    if cachchedContents: struct -> columns[0].cachedContents.top
   */
  val df = spark.read.json("data/socrata_metadata.json")
  df.createTempView("abc")
  spark.sql("show tables").show()
  spark.sql("select columns[0].cachedContents.top[0].item from abc").show()
  //df.select("columns.element").printSchema()

}
