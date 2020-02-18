package sparkbasics

import init.InitSpark
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, RowFactory}

object ConcatRow extends InitSpark {


  val row1 = RowFactory.create("1","2")
  val schema1 = new StructType()
    .add("c0","string")
    .add("c1","string")

  val row2 = RowFactory.create("A","B")
  val schema2 = new StructType()
    .add("c2","string")
    .add("c3","string")


  val df1 = spark.createDataFrame(sc.parallelize(Seq(row1)),schema1)
  df1.show()

  val rdd = df1.rdd.map(s => Row.merge(s, row2))
  val schema = StructType(schema1 ++ schema2)

  val df = spark.createDataFrame(rdd,schema)
  df.printSchema()
  df.show()

  /*
    +---+---+
    | c0| c1|
    +---+---+
    |  1|  2|
    +---+---+

    root
    |-- c0: string (nullable = true)
    |-- c1: string (nullable = true)
    |-- c2: string (nullable = true)
    |-- c3: string (nullable = true)

    +---+---+---+---+
    | c0| c1| c2| c3|
    +---+---+---+---+
    |  1|  2|  A|  B|
    +---+---+---+---+
   */
}
