package sparkstreaming

import init.InitSpark

object StreamSocket extends InitSpark {

  // tip: usun checkpointy zeby wyniki byly za kazdym razem takie same



  //1. cat C:/logs/* > file.txt  (merge files)
  //2. ncat -lk 9999 < file.txt  (send file)
  //or in one step
  //1. cat C:/logs/* | ncat -lk 9999  (merge and send)
  //3. ncat localhost 9999      (read file)

  import spark.implicits._

  spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
    .filter('value.cast("int").isNotNull)
    .filter('value>10)
    .coalesce(1)
    .writeStream
    .format("text") // default parquet
    .outputMode("append")
    .option("checkpointLocation", "data/checkpoint")
    .start("data/stream_out")
    .awaitTermination()

//  val out = spark.read.text("data/stream_out")
//  println("partitions: "+out.rdd.getNumPartitions)
//  out.show()
//  out.coalesce(1).write.text("data/stream_out/single")
}
/*
partitions: 1
+--------+
|   value|
+--------+
|13351423|
|  223231|
|  123131|
|   88888|
|    1230|
|     213|
|      12|
|      11|
|      13|
|      14|
|      15|
|      16|
|      17|
+--------+
 */