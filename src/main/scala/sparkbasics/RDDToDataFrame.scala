package sparkbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RDDToDataFrame {
  def main(args: Array[String]): Unit = {

    // INFO DISABLED
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    val conf: SparkConf = new SparkConf()
      .setAppName("testApp")
      .setMaster("local")

    val sc: SparkContext = new SparkContext(conf)
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val iris = ss.read.text("data/iris2.csv")
    iris.printSchema()
    iris.show()

    val schema = new StructType()
      .add("a", DataTypes.DoubleType)
      .add("b", DataTypes.StringType)
      .add("c", DataTypes.StringType)
      .add("d", DataTypes.StringType)
      .add("class", DataTypes.StringType)

    val encoder = RowEncoder(schema)

    val iris2 = iris.rdd.map(_=>Row.apply(1.0,"2","3","4","5"))
    val xx = ss.createDataFrame(iris2,schema)
    xx.show(5)
    xx.printSchema()



  }
}
