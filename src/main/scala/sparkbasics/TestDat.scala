package sparkbasics

import org.apache.log4j.{Level, Logger}

object TestDat extends App {
  // INFO DISABLED
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("INFO").setLevel(Level.OFF)

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext

  import java.util.regex.Pattern

  import org.apache.spark.sql.RowFactory
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{StringType, StructField, StructType}
  import spark.implicits._

  val data = spark.read
    .text("data/data.dat")
    .toDF("val")
    .withColumn("id", monotonically_increasing_id())

  val count = data.count()
  val header = data.where('id === 1).collect().map(s => s.getString(0)).apply(0)
  val columns = header
    .replace("H|*|", "")
    .replace("|##|", "")
    .replace("|*|", ",")
    .split(",")
  val columnDelimiter = Pattern.quote("|*|")
  val correctData = data.where('id > 1 && 'id < count - 1)
    .select(regexp_replace('val, columnDelimiter, ",").as("val"))
  val splitIntoCols = correctData.rdd.map(s=>{
    val arr = s.getString(0).split(",")
    RowFactory.create(arr:_*)
  })
  val struct = StructType(columns.map(s=>StructField(s, StringType, true)))
  val finalDF = spark.createDataFrame(splitIntoCols,struct)

  finalDF.show()

  /*
+----------+---------+--------------------+------------------+----------+-----------+--------------------+
|AP_ATTR_ID|    AP_ID|             OPER_ID|           ATTR_ID|ATTR_GROUP|LST_UPD_USR|       LST_UPD_TSTMP|
+----------+---------+--------------------+------------------+----------+-----------+--------------------+
|    779045|      Sar|SUPERVISOR HIERARCHY|        Supervisor|         2|        128|2019.05.14 16:48:...|
|    779048|       KK|SUPERVISOR HIERARCHY|        Supervisor|         2|        116|2019.05.14 16:59:...|
|    779054|Nisha - A|               EXACT|CustomColumnRow120|         2|       1165|2019.05.15 12:11:...|
+----------+---------+--------------------+------------------+----------+-----------+--------------------+
   */
}
