package analytics

import init.InitSpark
import org.apache.spark.sql.types.StructType

object Us12 extends InitSpark {
  // us12
  val ds1 = spark.read.option("header", "true")
    //.option("inferSchema", "true")
    .csv("Downloads/us12/1.csv")

  val ds11 = ds1
    .drop("equipment_status", "superord_equipment")
    .drop("hash_id")

  // us8
  val ds2 = spark.read.option("header", "true")
    //.option("inferSchema", "true")
    .csv("Downloads/us12/2.csv")
    .drop("hash_id")

  //  println(ds11.intersect(ds2).count())
  //  println(ds2.intersect(ds11).count())
  import spark.implicits._

  val a1 = ds11.filter('asset_code === "000000001000002036")
  //.show()
  val a2 = ds2.filter('asset_code === "000000001000002036") //.show()
  print("result: " + ds11.intersect(ds2).count() + ", " + ds11.count())
  //ds1.groupBy("group_concat(status_hehe)").count().show()
  //s2.where("asset_code like '000000001000002036'").show()


  val problem = ds11.except(ds2)
  problem.createTempView("problem")
  ds1.createTempView("ds1")
  spark.sql("select * from ds1 where asset_code IN (select asset_code from problem)")
    .show()






}
