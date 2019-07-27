package tasks1

import init.InitSpark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

/**
  * Read two CSV files from HDFS.
  * They contain data about flights and delays.
  * Desired output should be a 2 row csv file
  * with two columns average delay per state sorted by delay.
  *
  * USING RDD
  */
object Task3FlightsRDD extends InitSpark {
  val flightsRDD = sc.textFile("data/flights/flights.csv").zipWithIndex().filter(s => s._2 != 0).map(s => s._1)
  val delaysRDD = sc.textFile("data/flights/delays.csv").zipWithIndex().filter(s => s._2 != 0).map(s => s._1)

  case class Flights(flight_id: Int, state: String)
  case class Delays(flight_id: Int, delay: Int)

  val flightsWithKey = flightsRDD
    .map(s => s.split(','))
    .map(s => {
      val key = s.apply(0).toInt
      val values = Flights(s.apply(0).toInt, s.apply(1))
      (key, values)
    })

  val delaysWithKey = delaysRDD
    .map(s => s.split(','))
    .map(s => {
      val key = s.apply(0).toInt
      val values = Delays(s.apply(0).toInt, s.apply(1).toInt)
      (key, values)
    })

  val joinedRDD = flightsWithKey.join(delaysWithKey).map(s => s._2)
  //println(joinedRDD.first())

  val finalRDD = joinedRDD
    .filter(s => s._2.delay > 0)
    .map(s => (s._1.state, s._2.delay))
    .groupByKey()
    .mapValues(s => (s.sum.toDouble / s.size.toDouble))
    .sortBy(s => s._2, false)

  //finalRDD.foreach(s => println(s))

  val schema = new StructType()
    .add("state", StringType)
    .add("delay", DoubleType)

  val finalDF = spark.createDataFrame(finalRDD.map(s => Row.fromTuple(s)), schema)
  finalDF.printSchema()
  finalDF.show()
}
