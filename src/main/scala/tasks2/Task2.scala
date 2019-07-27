package tasks2

object Task2 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val dataA = spark.read
    .option("sep", "\t")
    .schema("id STRING, currency STRING, date STRING, value DECIMAL(10,2), code STRING")
    .csv("data/tasks2/t2a.txt")
  val dataB = spark.read
    .option("sep", "\t")
    .schema("id INT, code STRING")
    .csv("data/tasks2/t2b.txt")
    .select('code)

  dataA.join(broadcast(dataB),Seq("code")) // dfA broadcast join small dfB, join on col "code" as Seq to avoid duplicated joined columns
    .na.drop() // drop rows with nulls or NaN
    .select('currency.alias("CUR"),'date,'value,'code) // rename column using alias
    .filter(year('date) === 2012) // select only rows with date 2012
    .filter('code =!= "FR") // filter out rows with code FR
    .filter('CUR === "Euro" || 'CUR === "Dollar") // select currencies
    .groupBy('CUR)
    .agg(avg('value).as("avg")) // calculate avg
    .orderBy('avg.desc) // sort
    .coalesce(1) // result in 1 file
    .write
    .option("sep", "\t") // set separator, default semicolon
    .option("header","true") // add header
    .csv("data/tasks2/out/t2")

  /*
  CUR	avg
  Euro	6.045000
  Dollar	5.070968
   */
}
