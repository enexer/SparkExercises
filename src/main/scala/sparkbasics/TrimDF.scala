package sparkbasics

object TrimDF extends InitializeSpark {

  import spark.implicits._
  val data = Seq("B ", "B", " B", "B  ", "C", "C ").toDF("val")
  import org.apache.spark.sql.functions._
  data.select(trim('val).as("val")).groupBy('val).count().show()

  // trim 2 columns
  val data2 = data.withColumn("val2",'val)
  val ok = data2.columns.map(data2(_)).map(trim(_))
  data2.select(ok: _*).show()

  // trim using spark SQL
  data.selectExpr("val","trim(val)").show()


}
