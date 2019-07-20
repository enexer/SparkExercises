package sparkbasics

import init.InitSpark

object ImplictsDataset extends InitSpark{
  import spark.implicits._
  val rdd = sc.parallelize(List(1,2,3,4))
  val ds = spark.createDataset(rdd)//(Encoders.scalaInt)
  ds.printSchema()
  import spark.implicits._
  ds.filter('value > 2).show()
}
