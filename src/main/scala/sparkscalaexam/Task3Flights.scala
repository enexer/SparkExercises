package sparkscalaexam

import init.InitSpark

/**
  * Read two CSV files from HDFS.
  * They contain data about flights and delays.
  * Desired output should be a 2 row csv file
  * with two columns average delay per state sorted by delay.
  *
  * USING DATAFRAMES
  */
object Task3Flights extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._ //$"col_name" / 'col_name
  val dfr = spark.read
    .option("header", "true")
    .option("inferSchema", "true")

  val ds1 = dfr.csv("data/flights/flights.csv")
  val ds2 = dfr.csv("data/flights/delays.csv")

  ds1.show(2)
  ds2.show(2)
  ds1.join(ds2)
    .where(ds1("flight_id") === ds2("flight_id"))
    .show(2)

  /*
  * USING DATA FRAME API
  */
  val delays = ds1.join(ds2)
    .where(ds1("flight_id") === ds2("flight_id"))
    .filter($"delay" > 0)
    .groupBy($"state")
    .agg(avg($"delay").as("delay"), variance('delay).as("variance")) // .as is equal to .alias
    .orderBy(desc("delay"))
    .limit(20)
  delays.show()

  /*
  * USING SPARK SQL
  */
  ds1.createTempView("flights")
  ds2.createTempView("delays")
  spark.sql("SELECT f.state, AVG(d.delay) delay FROM flights f JOIN delays d" +
    " ON f.flight_id=d.flight_id WHERE d.delay > 0 GROUP BY f.state ORDER BY delay DESC")
    .show()


}
