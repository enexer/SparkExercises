package sparkbasics

import init.InitSpark
import org.apache.spark.sql.expressions.Window

/*
    https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
 */
object WindowFunctions2 extends InitSpark {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val data = Seq(
    ("thin", "cell phone", 6000),
    ("normal", "tablet", 1500),
    ("mini", "tablet", 5500),
    ("ultra thin", "cell phone", 5000),
    ("very thin", "cell phone", 6000),
    ("big", "tablet", 2500),
    ("bendable", "cell phone", 3000),
    ("foldable", "cell phone", 3000),
    ("pro", "tablet", 4500),
    ("pro2", "tablet", 6500)
  ).toDF("product", "category", "revenue")

  data.printSchema()
  data.show(1)

  data.createTempView("productRevenue")

  //   SPARK SQL
  val query = "SELECT  product,category,revenue FROM " +
    "(SELECT product,category,revenue," +
    "dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank" +
    "FROM productRevenue) tmp" +
    "WHERE  rank <= 2"
  spark.sql(query).show()


  // DATAFRAME API
  val windowSpec = Window
    .partitionBy('category)
    .orderBy('revenue.desc)
    .rangeBetween(Long.MinValue, Long.MaxValue)
  val dataFrame = spark.table("productRevenue")
  val revenue_difference = max('revenue).over(windowSpec) - 'revenue
  dataFrame.select('product,'category,'revenue, revenue_difference.alias("revenue_difference"))


}
