package sparkbasics

import init.InitSpark

object OrderByMultipleFields extends InitSpark{

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import spark.implicits._

  val someData = Seq(
    Row(1, "C"),
    Row(2, "B"),
    Row(3, "A"),
    Row(5, "A"),
    Row(3, "Z"),
    Row(3, "X")
  )

  val someSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )

  val someDF = spark.createDataFrame(
    spark.sparkContext.parallelize(someData),
    StructType(someSchema)
  )

  someDF.printSchema()
  someDF.orderBy('word).show()

  someDF.sort('number.desc, 'word.desc).show()

  someDF.filter('number>1).show()


  // freq items
  someDF.stat.freqItems(Seq("number","word"), 0.2).show()


  def f1(a: String*): String ={
    val ok = a.reduce(_+_)
    ok
  }

  val list2 = List("a1","a2","a3")
  println(f1("as","asd","123"))
  println(f1(list2: _*))
}
/*
root
 |-- number: integer (nullable = true)
 |-- word: string (nullable = true)

+------+----+
|number|word|
+------+----+
|     5|   A|
|     3|   A|
|     2|   B|
|     1|   C|
|     3|   X|
|     3|   Z|
+------+----+

+------+----+
|number|word|
+------+----+
|     5|   A|
|     3|   Z|
|     3|   X|
|     3|   A|
|     2|   B|
|     1|   C|
+------+----+

+------+----+
|number|word|
+------+----+
|     2|   B|
|     3|   A|
|     5|   A|
|     3|   Z|
|     3|   X|
+------+----+

+----------------+---------------+
|number_freqItems| word_freqItems|
+----------------+---------------+
|    [2, 5, 1, 3]|[A, C, X, Z, B]|
+----------------+---------------+

asasd123
a1a2a3
 */