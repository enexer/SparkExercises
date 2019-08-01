package sparkbasics

import init.InitSpark

object DatasetTest extends InitSpark {

  val ok = 1 to 20
  import spark.implicits._
  val ds = spark.createDataset(ok)

  val df = ds.map(_+2)
    .map(s=>{(s+1)/2*1.001})
    .map(s=>(s,if(s>10) 1 else 0)).toDF("value","group")
    .groupBy('group) // or  '_2
    .count()

  df.printSchema()
  df.show()


  ds.map(_+2)
    .map(s=>{(s+1)/2*1.001})
    .map(s=>(s,if(s>10) 1 else 0))
    .filter(_._2==1)
    .show()

}
/*
root
 |-- group: integer (nullable = false)
 |-- count: long (nullable = false)

+-----+-----+
|group|count|
+-----+-----+
|    1|    4|
|    0|   16|
+-----+-----+

+------------------+---+
|                _1| _2|
+------------------+---+
|10.009999999999998|  1|
|10.009999999999998|  1|
|            11.011|  1|
|            11.011|  1|
+------------------+---+
 */
