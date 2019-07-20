package sparkbasics

import init.InitSpark

object ReadCSVWithNewSchema extends InitSpark {

  spark.read.csv("data/iris3.csv").printSchema()
  spark.read.schema("a DOUBLE, b STRING, c STRING, d STRING, e STRING").csv("data/iris3.csv").printSchema()


  val schema =  "sepal_length DOUBLE," +
                "sepal_width DOUBLE," +
                "petal_length DOUBLE," +
                "petal_width DOUBLE," +
                "species STRING"
  val ds1 = spark.read.schema(schema).csv("data/iris3.csv")

  ds1.printSchema()
  ds1.show(1)

  import org.apache.spark.sql.types._

  val schema2 = StructType(Seq(
    StructField("sepal_length_",DataTypes.DoubleType),
    StructField("sepal_width_",DataTypes.DoubleType),
    StructField("petal_length_",DataTypes.DoubleType),
    StructField("petal_width_",DataTypes.DoubleType),
    StructField("species_",DataTypes.StringType)
  ))


  val ds2 = spark.read.schema(schema2).csv("data/iris3.csv")
  ds2.printSchema()
  ds2.show(1)


  val schema3 = new StructType()
    .add("sepal_length#", DoubleType)
    .add("sepal_width#", DoubleType)
    .add("petal_length#", DoubleType)
    .add("petal_width#", DoubleType)
    .add("species#", StringType)

  spark.read.schema(schema3).csv("data/iris3.csv").printSchema()
}

//root
//|-- _c0: string (nullable = true)
//|-- _c1: string (nullable = true)
//|-- _c2: string (nullable = true)
//|-- _c3: string (nullable = true)
//|-- _c4: string (nullable = true)
//
//root
//|-- a: string (nullable = true)
//|-- b: string (nullable = true)
//|-- c: string (nullable = true)
//|-- d: string (nullable = true)
//|-- e: string (nullable = true)
//
//root
//|-- sepal_length: double (nullable = true)
//|-- sepal_width: double (nullable = true)
//|-- petal_length: double (nullable = true)
//|-- petal_width: double (nullable = true)
//|-- species: string (nullable = true)
//
//+------------+-----------+------------+-----------+-------+
//|sepal_length|sepal_width|petal_length|petal_width|species|
//+------------+-----------+------------+-----------+-------+
//|         5.1|        3.5|         1.4|        0.2| setosa|
//+------------+-----------+------------+-----------+-------+
//only showing top 1 row
//
//root
//|-- sepal_length_: double (nullable = true)
//|-- sepal_width_: double (nullable = true)
//|-- petal_length_: double (nullable = true)
//|-- petal_width_: double (nullable = true)
//|-- species_: string (nullable = true)
//
//+-------------+------------+-------------+------------+--------+
//|sepal_length_|sepal_width_|petal_length_|petal_width_|species_|
//+-------------+------------+-------------+------------+--------+
//|          5.1|         3.5|          1.4|         0.2|  setosa|
//+-------------+------------+-------------+------------+--------+
//only showing top 1 row