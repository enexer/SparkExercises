package tasks1

import init.InitSpark
import org.apache.spark.sql.Encoders

//export SPARK_MAJOR_VERSION=2
/**
  * Read CSV file from HDFS. It contains data about account balances of bank clients.
  * Columns of the csv file are provided in task description.
  * Desired output is a CSV file stored in HDFS (delimiter is also specified)
  * that contains average account balance of clients with balance above $2000 per state.
  */
object Task1Bank extends InitSpark {

  val df2 = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("data/bank/bank_clients.csv")

  //    USING SPARK SQL AND SAVE AS TXT/CSV FILE
  df2.createGlobalTempView("bank")
  //spark.sql("show tables").show()
  val ok = spark.sql("SELECT AVG(balance) AS balance FROM global_temp.bank WHERE balance>1000")
  ok.show()
  //    SAVE AS TXT ///////////////////////////
  val rdd = ok.rdd
    .map(s => s.getDouble(0))
  println(rdd.first())
  rdd.saveAsTextFile("results/task1/rdd")

  //    SAVE AS CSV
  val new_ds = spark.createDataset(rdd)(Encoders.scalaDouble)
    .withColumnRenamed("value", "balance")
  new_ds.show()
  new_ds.write.option("header", "true").csv("results/task1/dataset")


  //    USING DATA FRAME API AND SAVE AS CSV
  import org.apache.spark.sql.functions._

  df2
    .filter(df2("balance") > 2000)
    .agg(avg(df2("balance")).as("balance"))
    .write
    //.option("delimiter", ";")
    .option("header", "true")
    .csv("results/task1/dataframe")

}


//    df.printSchema()
//    df.show(1)
//
//    val df2 = df.drop("Last Updated", "Current Ver", "ContentRating")
//      .withColumnRenamed("Android Ver", "Android")
//
//    df2.show(1)
//    // f2.filter(df2("Size")<"19M").show()  // dziwne, dziala
//
//    df2
//      .filter(df2("Reviews") > 200)
//      .agg(avg(df2("Reviews")))
//      .write.csv("data/XDD")


//
//    val df3 = df2
//      .filter(df2("Rating") > 3.5)
//      .filter(df2("Reviews") > 100)
//      .groupBy("Category")
//      //.count()
//      .agg(
//      count(df2("Reviews")).as("count"),
//      avg(df2("Reviews")).as("avg")
//      )
//      .orderBy(desc("count"))
//
//    df3.show()


//    val df4 = df.select(df("Last Updated").cast(DataTypes.DateType))
//    df4.printSchema()
//    df4.show()


//println(java.sql.Date.valueOf("January 7, 2018"))
