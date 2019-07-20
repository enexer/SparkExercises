package sparkbasics

import init.InitSpark

object DateFormat extends InitSpark {

  import spark.implicits._

  //val df = spark.read.option("header", "true").option("delimiter", ";").csv("data/dataT1.csv")
  val df = Seq(("1783","1","13-01-2017"),("3291","7","28-12-2018")).toDF("cust_id","cust_profile","date_joined")
  df.show()

  import spark.implicits._
  // USING SPARK SQL
  val data1_df_sel = df.select('cust_id.alias("cust_number"), 'cust_profile, 'date_joined)
  data1_df_sel.createTempView("data1_df_sel")
  val data1_df_final = spark.sql("select * from (select cust_number, cust_profile," +
    " to_date(from_unixtime(UNIX_TIMESTAMP(date_joined,'dd-MM-yyyy')))" +
    " as date_joined2 from data1_df_sel) data1 where date_joined2 > '2018-01-01'")
  data1_df_final.show()


  // USING DATAFRAME
  import org.apache.spark.sql.functions._
  data1_df_sel
    .withColumn("ok",to_date(to_timestamp('date_joined, "dd-MM-yyyy")))
    //.where('ok > "2018-01-01")
    .show()

//
//    +-------+------------+-----------+
//    |cust_id|cust_profile|date_joined|
//    +-------+------------+-----------+
//    |   1783|           1| 13-01-2017|
//    |   3291|           7| 28-12-2018|
//    +-------+------------+-----------+
//
//    +-----------+------------+------------+
//    |cust_number|cust_profile|date_joined2|
//    +-----------+------------+------------+
//    |       3291|           7|  2018-12-28|
//    +-----------+------------+------------+
//
//    +-----------+------------+-----------+----------+
//    |cust_number|cust_profile|date_joined|        ok|
//    +-----------+------------+-----------+----------+
//    |       3291|           7| 28-12-2018|2018-12-28|
//    +-----------+------------+-----------+----------+

}
