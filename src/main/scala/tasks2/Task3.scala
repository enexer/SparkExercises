package tasks2

import init.InitSpark

object Task3 extends InitSpark {



  import spark.implicits._
  import org.apache.spark.sql.functions._
  spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
    .agg(collect_list('value).as("list")) // make aggregation for complete output
    .select(explode('list)) // explode list to show all values
    .writeStream
    .format("console")
    .outputMode("complete") // need aggregation for complete mode
    .option("checkpointLocation", "data/checkpoint")
    .start()
    .awaitTermination()

  //Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;
  //Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
}
/*
>nc -lk 9999
>first
>second

-------------------------------------------
Batch: 0
-------------------------------------------
+---+
|col|
+---+
+---+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+
|  col|
+-----+
|first|
+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+
|   col|
+------+
| first|
|second|
+------+
 */