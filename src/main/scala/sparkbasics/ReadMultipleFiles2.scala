package sparkbasics

import init.InitSpark

object ReadMultipleFiles2 extends InitSpark {


  // 1st METHOD- DATAFRAME read multiple files into 1 ds
  val df = spark.read.option("header","true").csv("data/multiple/")
  println(df.count())
  df.show()

  // 2nd METHOD- DATAFRAME read multiple files into 1 ds
  val df2 = spark.read.option("header","true")
    .csv("data/multiple/1.csv", "data/multiple/2.csv")
  df2.show()


  // 1st METHOD-- RDD read lines with csv header !!!
  val rdd = sc.textFile("data/multiple/*")
  rdd.collect().foreach(println(_))
}
