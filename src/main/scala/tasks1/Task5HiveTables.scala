package tasks1

import init.InitSpark
import org.apache.spark.sql.SparkSession

/**
  * Load Hive table containing financial data. Calculate average exchange rate for two currencies in a given period.
  */
object Task5HiveTables extends InitSpark {

  val ss: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

//  SparkSession.builder().enableHiveSupport().getOrCreate().sql("show tables")

  val hiveResult = ss.sql("SELECT AVG(ex_rate), currency" +
    "FROM exchange_rates WHERE" +
    "date > '10-10-2010' AND date < '10-10-2018'" +
    "AND currency IN ('USD','EUR')" +
    "GROUP BY currency")


}


//HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
//DataFrame df = hiveContext.sql("show tables");
//df.show();.