package sparkbasics

object GroupConcat extends InitializeSpark {

  import spark.implicits._

  spark.catalog.dropTempView("data123") // remove temp view
  val data = Seq((1, "a"),
                (1, "a"),
                (1, "b"),
                (1, "c"),
                (2, "e"),
                (2, "f"),
                (3, "g"),
                (4, "h"),
                (4, "i")).toDF("id", "value")

  data.createTempView("data123")
  spark.sql("select * from data123").show()
  spark.sql("select id, collect_list(value) from data123 group by id").show() //  group_concat in spark sql

  import org.apache.spark.sql.functions._

  // returns a list of objects with duplicates.
  data.groupBy('id).agg(collect_list('value)).show()
  // returns a set of objects with duplicate elements eliminated.
  data.groupBy('id).agg(collect_set('value)).show()
}

/*
+---+-----+
| id|value|
+---+-----+
|  1|    a|
|  1|    a|
|  1|    b|
|  1|    c|
|  2|    e|
|  2|    f|
|  3|    g|
|  4|    h|
|  4|    i|
+---+-----+

+---+-------------------+
| id|collect_list(value)|
+---+-------------------+
|  1|       [a, a, b, c]|
|  3|                [g]|
|  4|             [h, i]|
|  2|             [e, f]|
+---+-------------------+

+---+-------------------+
| id|collect_list(value)|
+---+-------------------+
|  1|       [a, a, b, c]|
|  3|                [g]|
|  4|             [h, i]|
|  2|             [e, f]|
+---+-------------------+

+---+------------------+
| id|collect_set(value)|
+---+------------------+
|  1|         [c, b, a]|
|  3|               [g]|
|  4|            [h, i]|
|  2|            [e, f]|
+---+------------------+
 */
