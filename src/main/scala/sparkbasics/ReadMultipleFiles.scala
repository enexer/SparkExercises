package sparkbasics

import init.InitSpark

object ReadMultipleFiles extends InitSpark {

  // 1st METHOD- DATAFRAME read multiple files into 1 ds
  val df = spark.read.option("header","true").csv("data/multiple/")
  println(df.count())
  df.show()

  // 2nd METHOD- DATAFRAME read multiple files into 1 ds
  val df2 = spark.read.option("header","true")
    .csv("data/multiple/1.csv", "data/multiple/2.csv")
  df2.show()

  // -- RDD
  // read lines with csv header !!!
  val rdd = sc.textFile("data/multiple/*")
  println(rdd.first())
  println(rdd.count())
  //path and value
  val rdd2 = sc.wholeTextFiles("data/multiple/*")
  val first = rdd2.first()
  println("PATH:"+first._1) //path
  println("VAUES:"+first._2) //all lines form file !!!

  /// remove header from csv file
  // get header
  val header = rdd.first()
  // remove header
  val rddNoHeader  = rdd.filter(_!=header)
  rddNoHeader.collect().foreach(showBydlak(_))
  def showBydlak(a: Any): Unit = println("->"+a)

}
