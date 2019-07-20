package sparkbasics

import init.InitSpark

object BroadcastJoin extends InitSpark {

  //https://docs.microsoft.com/pl-pl/azure/hdinsight/spark/apache-spark-perf
  //helps with joining big datasets by small datasets


  import spark.implicits._

  val df1 = (1 to 99999).toDF("id")
  val df2 = (1 to 99999 by 20).toDF("id")

  println(df1.join(df2, "id").count())

  import org.apache.spark.sql.functions._

  println(df1.join(broadcast(df2), "id").count())


  df1.join(df2, "id").explain()
  df2.join(df1, "id").explain()

}
