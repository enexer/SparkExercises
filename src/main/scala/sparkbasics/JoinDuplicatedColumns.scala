package sparkbasics

import init.InitSpark

object JoinDuplicatedColumns extends InitSpark{
  import spark.implicits._
  // problem with duplicated columns using data("a1")===data2("a1") , use Seq("a2","a3")
  /// https://docs.databricks.com/spark/latest/faq/join-two-dataframes-duplicated-column.html
  val data = Seq((1,"blue",2),(4,"red",10)).toDF("a1","a2","a3")
  val data2 = Seq((1,"green",2)).toDF("a1","a2","a3")
  data.show()
  data2.show()
  //val data3 = data.join(data2, data("a1")===data2("a1") && data("a1")===data2("a1"), "left")//.toDF("1","2","3","4","5","6")
  val data3 = data.join(data2, Seq("a1","a3"), "left") /// PREVENT DUPLICATING COLUMNS
  data3.printSchema()
  data3.show()
}
