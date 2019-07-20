package sparkbasics

import init.InitSpark

object RDDToDataFrame2 extends InitSpark{

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{DataTypes, StructType}


  //  1st method
  val data = Seq(Row(1,2,3), Row(4,5,6))
  val rdd = sc.parallelize(data)
  val schema = new StructType()
    .add("c1",DataTypes.IntegerType)
    .add("c2",DataTypes.IntegerType)
    .add("c3",DataTypes.IntegerType)

  val df = spark.createDataFrame(rdd,schema)
  df.printSchema()
  df.show()

  // change columns names
  val newNames = df.columns.map(s=>"new_"+s)
  val df3 = df.toDF(newNames: _*)
  df3.show()


  // 2nd method
  val data2 = Seq((1,2,3),(4,5,6))
  spark.createDataFrame(sc.parallelize(data2)).toDF("b1","b2","b3").show()
  spark.createDataFrame(Seq((1,2,3),(4,5,6))).toDF("x1","x2","x3").show()




}
