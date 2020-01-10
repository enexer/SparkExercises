package sparkbasics

import init.InitSpark

object DatesFirstDayOfMonth extends InitSpark{

  /*
        minimal date of each month an year
   */
  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df =  spark.read.csv("data/dates2.csv").toDF("date")
  df.show()

  df.groupBy(year('date),month('date)).agg(min('date).as("result"))
    .select("result")
    .show()

  df.createTempView("df")
  spark.sql("select min(date) as result from df group by year(date), month(date)").show()
  /*

+----------+
|      date|
+----------+
|2019-08-08|
|2019-08-22|
|2019-08-23|
|2019-08-31|
|2019-08-29|
|2019-08-01|
|2019-08-04|
|2019-08-11|
|2019-08-15|
|2019-09-03|
|2019-08-27|
|2019-08-28|
|2019-08-06|
|2019-09-01|
|2019-08-07|
|2019-08-17|
|2019-09-04|
|2019-08-10|
|2019-08-12|
|2019-09-02|
+----------+

+----------+-----------+----------+
|year(date)|month(date)| min(date)|
+----------+-----------+----------+
|      2019|          8|2019-08-01|
|      2019|          9|2019-09-01|
+----------+-----------+----------+
   */
}
