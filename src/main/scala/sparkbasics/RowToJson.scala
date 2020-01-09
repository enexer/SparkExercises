package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RowToJson extends InitSpark {

  /*
  manually create smhth like df.toJSON
   */
  val data = Seq(
    Row(1, "a", "b", "c", "d"),
    Row(5, "z", "b", "c", "d")
  )

  val schema = StructType(
    List(
      StructField("id", IntegerType, true),
      StructField("f2", StringType, true),
      StructField("f3", StringType, true),
      StructField("f4", StringType, true),
      StructField("f5", StringType, true)
    )
  )

  val df1 = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
  )

  df1.show()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df2 = df1.select(
    to_json(
      map_from_arrays(lit(df1.columns), array('*))
    ).as("json")
  )

  df1.toJSON.show(false)

//  df2.show(false)
//
//  df2.write.json("data/json123")
//
//  spark.read.json("data/json123").select('json('id)).show()

/*
+---+---+---+---+---+
| id| f2| f3| f4| f5|
+---+---+---+---+---+
|  1|  a|  b|  c|  d|
|  5|  z|  b|  c|  d|
+---+---+---+---+---+

+--------------------------------------------+
|value                                       |
+--------------------------------------------+
|{"id":1,"f2":"a","f3":"b","f4":"c","f5":"d"}|
|{"id":5,"f2":"z","f3":"b","f4":"c","f5":"d"}|
+--------------------------------------------+
 */
}
