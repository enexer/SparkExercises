package sparkbasics

import init.InitSpark
import org.apache.spark.sql.expressions.Window

object DateFormats extends InitSpark {


  val df = spark.range(5).as("id")
  df.show()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df_unix1 = df
    .select(
      unix_timestamp().as("unix"),
      lit( 24*60*60).as("x"),
      monotonically_increasing_id().as("id")
    )
  df_unix1.show()

  val df_unix2 = df_unix1
    .select(
      'unix,
      sum('x).over(Window.orderBy('id)).as("x"),
      'id
    )
  df_unix2.show()

  val df_unix3 = df_unix2.select(('unix + 'x).as("unix"))
  df_unix3.show()

  val df_unix4 = df_unix3.select(
    from_unixtime('unix, "YYYY-MM-DD"), // wrong format
    from_unixtime('unix, "yyyy-MM-dd").as("date"), // ok
    from_unixtime('unix, "dd-MM-yyyy") // ok
  )
  df_unix4.show()

  val df_unix5 = df_unix4.select(
    'date.as("before"),
    date_format('date, "dd-MM-yyyy").as("date")
  )
  df_unix5.show()


  val df_unix6 = df_unix5
  .select('date,
    date_add(from_unixtime(unix_timestamp('date,"dd-MM-yyyy"),"yyyy-MM-dd"),1).as("date_1day")
  ).orderBy('date_1day.desc)
  df_unix6.show()

  //df.write.json("qwe1123")
}
/*
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+

+----------+-----+---+
|      unix|    x| id|
+----------+-----+---+
|1563586816|86400|  0|
|1563586816|86400|  1|
|1563586816|86400|  2|
|1563586816|86400|  3|
|1563586816|86400|  4|
+----------+-----+---+

+----------+------+---+
|      unix|     x| id|
+----------+------+---+
|1563586816| 86400|  0|
|1563586816|172800|  1|
|1563586816|259200|  2|
|1563586816|345600|  3|
|1563586816|432000|  4|
+----------+------+---+

+----------+
|      unix|
+----------+
|1563673217|
|1563759617|
|1563846017|
|1563932417|
|1564018817|
+----------+

+-------------------------------+----------+-------------------------------+
|from_unixtime(unix, YYYY-MM-DD)|      date|from_unixtime(unix, dd-MM-yyyy)|
+-------------------------------+----------+-------------------------------+
|                    2019-07-202|2019-07-21|                     21-07-2019|
|                    2019-07-203|2019-07-22|                     22-07-2019|
|                    2019-07-204|2019-07-23|                     23-07-2019|
|                    2019-07-205|2019-07-24|                     24-07-2019|
|                    2019-07-206|2019-07-25|                     25-07-2019|
+-------------------------------+----------+-------------------------------+

+----------+----------+
|    before|      date|
+----------+----------+
|2019-07-21|21-07-2019|
|2019-07-22|22-07-2019|
|2019-07-23|23-07-2019|
|2019-07-24|24-07-2019|
|2019-07-25|25-07-2019|
+----------+----------+

+----------+----------+
|      date| date_1day|
+----------+----------+
|25-07-2019|2019-07-26|
|24-07-2019|2019-07-25|
|23-07-2019|2019-07-24|
|22-07-2019|2019-07-23|
|21-07-2019|2019-07-22|
+----------+----------+
 */
