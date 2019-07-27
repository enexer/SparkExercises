package tasks1

import init.InitSpark
import org.apache.spark.sql.functions._

/**
  * Given north pole station data (saved as text file).
  * Calculate lowest temperature per station in given timeline.
  *
  * DATAFRAME
  */
object Task6NorthPole extends InitSpark {

  //ss.range(1).select(current_date()).show()

  val stationData = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("data/north_pole/data.csv")

  import spark.implicits._
  // 'date  or  $"date"  --> spark implicts


  /*
  * DATA FRAME API
  */
  val lowestTempPerStation = stationData
    .filter('date > "2015-01-01" && 'date < "2018-11-11")
    .groupBy('station)
    .agg(round(avg('temp), 3).as("temp"))  // round is not required
    .orderBy(asc("temp"))

  lowestTempPerStation.show()


  /*
  * SPARK SQL
  */
  stationData.createTempView("stationData")
  spark.sql("SELECT s.station, ROUND(AVG(s.temp),3) temp FROM stationData s " +
    "WHERE s.date > '2015-01-01' AND s.date < '2018-11-11'" +
    "GROUP BY s.station ORDER BY temp ASC").show()


}
