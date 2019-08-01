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
/*
5000
5000
== Physical Plan ==
*(1) Project [id#3]
+- *(1) BroadcastHashJoin [id#3], [id#8], Inner, BuildRight
   :- LocalTableScan [id#3]
   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
      +- LocalTableScan [id#8]
== Physical Plan ==
*(1) Project [id#8]
+- *(1) BroadcastHashJoin [id#8], [id#3], Inner, BuildLeft
   :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
   :  +- LocalTableScan [id#8]
   +- LocalTableScan [id#3]
 */