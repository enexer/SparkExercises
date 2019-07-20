package sparkbasics

import init.InitSpark

object SplitDataFrameThenMakeDS extends InitSpark{

  import org.apache.spark.sql.functions._
  import spark.implicits._
  val data = List("11 a2 a3 a4",
                  "22 b2 b3 b4",
                  "33 c2 c3")
  val df = data.toDF("value")

  val df2 = df.select(split('value, "\\s+").as("www"))
  df.show()
  val df3 = df2.select(
            'www(0).as("col_0").cast("integer"),  // CAST
            'www(1).as("col_1"),
            'www(2).as("col_2"),
            'www(3).as("col_3"))
  df3.show()


  // case class field names shoul be equal to names from dataframe ^
  // data types should be equal too, use cast ^
  case class WikiViews(col_0:Int, col_1:String, col_2:String, col_3:String)
  val viewsDS = df3.as[WikiViews]
  viewsDS.printSchema()
  viewsDS.show()


}
