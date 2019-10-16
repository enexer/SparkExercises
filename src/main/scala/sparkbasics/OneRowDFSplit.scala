package sparkbasics

import init.InitSpark

object OneRowDFSplit extends InitSpark{
  import spark.implicits._
  import org.apache.spark.sql.functions._

  val data = Seq("####  #### ####","12312 12312 12312","abc abc ddd")
  val df = data.toDF("value")

  df.select(split('value, "\\s+").as("value"))
    .select('value(0).as("a1"), 'value(1).as("a2"), 'value(2).as("a3"))
    .show()
}
/*
+-----+-----+-----+
|   a1|   a2|   a3|
+-----+-----+-----+
| ####| ####| ####|
|12312|12312|12312|
|  abc|  abc|  ddd|
+-----+-----+-----+
 */