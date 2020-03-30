package sparkbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object ArrayFilter extends App {
  // INFO DISABLED
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("INFO").setLevel(Level.OFF)

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext

  import org.apache.spark.sql.functions._
  import spark.implicits._

      val df = Seq(
        Seq("prefix1-a", "prefix2-b", "prefix3-c", "prefix4-d"),
        Seq("prefix4-e", "prefix5-f", "prefix6-g", "prefix7-h", "prefix8-i"),
        Seq("prefix6-a", "prefix7-b", "prefix8-c", "prefix9-d"),
        Seq("prefix8-d", "prefix9-e", "prefix10-c", "prefix12-a")
      ).toDF("arr")


      val schema = StructType(Seq(
        StructField("arr", ArrayType.apply(StringType)),
        StructField("arr2", ArrayType.apply(StringType))
      ))
      val encoder = RowEncoder(schema)

      val df2 = df.map(s =>
        (s.getSeq[String](0).toArray, s.getSeq[String](0).map(s => s.substring(0, s.indexOf("-"))).toArray)
      ).map(s => RowFactory.create(s._1, s._2))(encoder)

      val prefixesList = Array("prefix4", "prefix5", "prefix6", "prefix7")
      val prefixesListSize = prefixesList.size
      val prefixesListCol = lit(prefixesList)

      df2.select('arr,'arr2,
        arrays_overlap('arr2,prefixesListCol).as("OR"),
        (size(array_intersect('arr2,prefixesListCol)) === prefixesListSize).as("AND")
      ).show(false)

      df2.filter(size(array_intersect('arr2,prefixesListCol)) === prefixesListSize).show(false)

  /*
+-------------------------------------------------------+---------------------------------------------+-----+-----+
|arr                                                    |arr2                                         |OR   |AND  |
+-------------------------------------------------------+---------------------------------------------+-----+-----+
|[prefix1-a, prefix2-b, prefix3-c, prefix4-d]           |[prefix1, prefix2, prefix3, prefix4]         |true |false|
|[prefix4-e, prefix5-f, prefix6-g, prefix7-h, prefix8-i]|[prefix4, prefix5, prefix6, prefix7, prefix8]|true |true |
|[prefix6-a, prefix7-b, prefix8-c, prefix9-d]           |[prefix6, prefix7, prefix8, prefix9]         |true |false|
|[prefix8-d, prefix9-e, prefix10-c, prefix12-a]         |[prefix8, prefix9, prefix10, prefix12]       |false|false|
+-------------------------------------------------------+---------------------------------------------+-----+-----+

+-------------------------------------------------------+---------------------------------------------+
|arr                                                    |arr2                                         |
+-------------------------------------------------------+---------------------------------------------+
|[prefix4-e, prefix5-f, prefix6-g, prefix7-h, prefix8-i]|[prefix4, prefix5, prefix6, prefix7, prefix8]|
+-------------------------------------------------------+---------------------------------------------+
   */

}
