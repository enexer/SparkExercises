package analytics

import sparkbasics.InitializeSpark

object ReadJSON extends InitializeSpark{

  val ds = spark.read
    .option("multiLine","true")
    .option("mode", "DROPMALFORMED")
    .json("data4/WORKORDER.JS")

  ds.printSchema()
  ds.select("_corrupt_record").show()
}
