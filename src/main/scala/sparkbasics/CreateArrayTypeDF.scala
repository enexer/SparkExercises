package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object CreateArrayTypeDF extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // PROBLEM: Create array type DF then use spark.sql.explode.

  val list = List(
    Row("bieber", Array("baby", "sorry")),
    Row("ozuna", Array("criminal"))
  )

  val schema = StructType.apply(Seq(
    StructField("name", StringType, true),
    StructField("hit_songs", ArrayType(StringType, true), true)))

  val singersDF = spark.createDataFrame(sc.parallelize(list),schema)

  singersDF.show()
  singersDF.printSchema()

  // explode() method creates a new row for every element in an array.
  //  ok | [1,2,3]
  //   will be
  //  ok | 1
  //  ok | 2
  //  ok | 3
  singersDF.select('name, explode('hit_songs).as("song")).show()
}
/*
+------+-------------+
|  name|    hit_songs|
+------+-------------+
|bieber|[baby, sorry]|
| ozuna|   [criminal]|
+------+-------------+

root
 |-- name: string (nullable = true)
 |-- hit_songs: array (nullable = true)
 |    |-- element: string (containsNull = true)

+------+--------+
|  name|    song|
+------+--------+
|bieber|    baby|
|bieber|   sorry|
| ozuna|criminal|
+------+--------+
 */
