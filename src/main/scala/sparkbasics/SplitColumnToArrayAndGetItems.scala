package sparkbasics

import init.InitSpark
import org.apache.spark.sql.DataFrame

object SplitColumnToArrayAndGetItems  extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val data = Seq("a.b.c","d.e.f","g.h.i").toDF("value")
  data.show()
  val se1 = data.selectExpr("split(value,'\\\\.') as selectExpr")
  se1.show()
  se1.printSchema()
  se1.select(se1(se1.columns.apply(0)).getItem(0)).show()
  val se2 = data.selectExpr("split(value,'//.') as selectExpr2")
  se2.show()
  se2.printSchema()
  se2.select(se2(se2.columns.apply(0)).getItem(0)).show()
  val se3 = data.selectExpr("split(value,'\\.') as selectExpr3")  // WTF?????
  se3.show()
  se3.printSchema()
  val data2 = data.select(split('value, "\\.").as("dfApi")).as("newAlias")
  data2.show()
  data2.printSchema()
  data.createTempView("dataTempView")
  data.createGlobalTempView("dataGlobalTempView")
  spark.sql("show tables").show()
  spark.sql("select split(value,'//.') from dataTempView").show()
  spark.sql("select split(value,'\\\\.') from global_temp.dataGlobalTempView as dataGlobalTempView").show()

  //---------------------------------------------------------//

  val df = Seq("a,b,c","d,e,f,x,x","g,h,i").toDF("value")
  val df2 = df.select(split('value,",").as("x"))
    .withColumn("1st",'x.getItem(0).cast("string"))
    .withColumn("2nd",'x.getItem(1).cast("string"))
    .withColumn("3rd",'x.getItem(2).cast("string"))
  df2.show()

  // same as above
  def splitCol(df: DataFrame, column: String, size: Int) = {
    val tempCol = "x"
    val arrayDelimiter = ","
    var d = df.select(split(df(column),arrayDelimiter).as(tempCol))
    for (i <- 0 until size){   //  OR to size-1
      d = d.withColumn("c_"+i,d(tempCol).getItem(i).cast("string"))
    }
    d.drop(tempCol)
  }

  def splitColAutoSize(df: DataFrame, column: String) = {
    val maxSize = df.map(s=>s.getString(0).split(",").length+1)
      .select(max('value))
      .first()
      .getInt(0)
    splitCol(df,column,maxSize)
  }

  splitCol(df,"value",3).show()

  val maxSize = df.map(s=>s.getString(0).split(",").length+1)
    .select(max('value))
    .first()
    .get(0)

  println("maxSize: "+maxSize)

  splitColAutoSize(df,"value").show()
}

/*
+-----+
|value|
+-----+
|a.b.c|
|d.e.f|
|g.h.i|
+-----+

+----------+
|selectExpr|
+----------+
| [a, b, c]|
| [d, e, f]|
| [g, h, i]|
+----------+

root
 |-- selectExpr: array (nullable = true)
 |    |-- element: string (containsNull = true)

+-------------+
|selectExpr[0]|
+-------------+
|            a|
|            d|
|            g|
+-------------+

+-----------+
|selectExpr2|
+-----------+
|    [a.b.c]|
|    [d.e.f]|
|    [g.h.i]|
+-----------+

root
 |-- selectExpr2: array (nullable = true)
 |    |-- element: string (containsNull = true)

+--------------+
|selectExpr2[0]|
+--------------+
|         a.b.c|
|         d.e.f|
|         g.h.i|
+--------------+

+------------+
| selectExpr3|
+------------+
|[, , , , , ]|
|[, , , , , ]|
|[, , , , , ]|
+------------+

root
 |-- selectExpr3: array (nullable = true)
 |    |-- element: string (containsNull = true)

+-------+
|  dfApi|
+-------+
|[a.b.c]|
|[d.e.f]|
|[g.h.i]|
+-------+

root
 |-- dfApi: array (nullable = true)
 |    |-- element: string (containsNull = true)

+--------+------------+-----------+
|database|   tableName|isTemporary|
+--------+------------+-----------+
|        |datatempview|       true|
+--------+------------+-----------+

+-----------------+
|split(value, //.)|
+-----------------+
|          [a.b.c]|
|          [d.e.f]|
|          [g.h.i]|
+-----------------+

+----------------+
|split(value, \.)|
+----------------+
|       [a, b, c]|
|       [d, e, f]|
|       [g, h, i]|
+----------------+

+---------------+---+---+---+
|              x|1st|2nd|3rd|
+---------------+---+---+---+
|      [a, b, c]|  a|  b|  c|
|[d, e, f, x, x]|  d|  e|  f|
|      [g, h, i]|  g|  h|  i|
+---------------+---+---+---+

+---+---+---+
|c_0|c_1|c_2|
+---+---+---+
|  a|  b|  c|
|  d|  e|  f|
|  g|  h|  i|
+---+---+---+

maxSize: 6
+---+---+---+----+----+----+
|c_0|c_1|c_2| c_3| c_4| c_5|
+---+---+---+----+----+----+
|  a|  b|  c|null|null|null|
|  d|  e|  f|   x|   x|null|
|  g|  h|  i|null|null|null|
+---+---+---+----+----+----+

 */
