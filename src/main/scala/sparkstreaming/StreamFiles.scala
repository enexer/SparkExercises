package sparkstreaming

import init.InitSpark
import org.apache.spark.sql.streaming.Trigger

object StreamFiles extends InitSpark {

  spark.readStream
    .text("data/stream") // contains .txt files
    .coalesce(1)
    .writeStream
    .format("text") // default parquet
    .outputMode("append")
    .option("checkpointLocation", "data/checkpoint")
    .start("data/stream_out")
    .awaitTermination()


  //spark.read.text("data/stream_out").show()
  /*
  +-----+
  |value|
  +-----+
  |    2|
  |    1|
  |    3|
  +-----+
   */
}
