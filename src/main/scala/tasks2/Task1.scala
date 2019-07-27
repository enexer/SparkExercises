package tasks2


object Task1 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val data = spark.read.json("data/tasks2/t1.json")
  data.show(10)

  // DF API
  data.na
    .drop()
    .filter('country =!= "Brazil")
    .filter('balance > 100 && 'balance < 700)
    .groupBy('country)
    .agg(avg('balance).as("avg"))
    .orderBy('avg.desc)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("data/tasks2/out/t1")


  // SPARK SQL
  data.createTempView("data")
  spark.sql("select country, avg(balance) avg from data where" +
    " id || first_name || last_name || email || country || balance is not null" +
    " and country != 'Brazil' and balance > 100 and balance < 700" +
    " group by country order by 2 desc")
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("data/tasks2/out/t1")


}
