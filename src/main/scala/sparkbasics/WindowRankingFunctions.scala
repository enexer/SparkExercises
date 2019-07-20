package sparkbasics

import init.InitSpark
import org.apache.spark.sql.expressions.Window

object WindowRankingFunctions extends InitSpark{

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val data = Seq(
    (1,99),
    (1,99),
    (1,70),
    (1,20),
    (1,66),
    (2,55),
    (2,99),
    (2,87),
    (3,1),
    (3,-23),
    (3,-66)
  ).toDF("id","value")

  data.show()

  data.select('id,'value, rank().over(Window.partitionBy('id).orderBy('value))).show()
  // computes the rank of a row in an ordered group of rows and returns the rank as a NUMBER
  data.select('id,'value, dense_rank().over(Window.partitionBy('id).orderBy('value))).show()
  // percentile ranking of rows in a result set
  data.select('id,'value, percent_rank().over(Window.partitionBy('id).orderBy('value))).show()
  // NTILE() distributes the result set into specified number of ordered partitions
  data.select('id,'value, ntile(3).over(Window.orderBy('value.desc))).show()
  // sequential number of a row
  data.select('id,'value, row_number().over(Window.orderBy('value))).show()
  // sequential number of a row within a partition
  data.select('id,'value, row_number().over(Window.partitionBy('id).orderBy('value))).show()
}

/*
+---+-----+
| id|value|
+---+-----+
|  1|   99|
|  1|   99|
|  1|   70|
|  1|   20|
|  1|   66|
|  2|   55|
|  2|   99|
|  2|   87|
|  3|    1|
|  3|  -23|
|  3|  -66|
+---+-----+

+---+-----+--------------------------------------------------------------------------------+
| id|value|RANK() OVER (PARTITION BY id ORDER BY value ASC NULLS FIRST unspecifiedframe$())|
+---+-----+--------------------------------------------------------------------------------+
|  1|   20|                                                                               1|
|  1|   66|                                                                               2|
|  1|   70|                                                                               3|
|  1|   99|                                                                               4|
|  1|   99|                                                                               4|
|  3|  -66|                                                                               1|
|  3|  -23|                                                                               2|
|  3|    1|                                                                               3|
|  2|   55|                                                                               1|
|  2|   87|                                                                               2|
|  2|   99|                                                                               3|
+---+-----+--------------------------------------------------------------------------------+

+---+-----+--------------------------------------------------------------------------------------+
| id|value|DENSE_RANK() OVER (PARTITION BY id ORDER BY value ASC NULLS FIRST unspecifiedframe$())|
+---+-----+--------------------------------------------------------------------------------------+
|  1|   20|                                                                                     1|
|  1|   66|                                                                                     2|
|  1|   70|                                                                                     3|
|  1|   99|                                                                                     4|
|  1|   99|                                                                                     4|
|  3|  -66|                                                                                     1|
|  3|  -23|                                                                                     2|
|  3|    1|                                                                                     3|
|  2|   55|                                                                                     1|
|  2|   87|                                                                                     2|
|  2|   99|                                                                                     3|
+---+-----+--------------------------------------------------------------------------------------+

+---+-----+----------------------------------------------------------------------------------------+
| id|value|PERCENT_RANK() OVER (PARTITION BY id ORDER BY value ASC NULLS FIRST unspecifiedframe$())|
+---+-----+----------------------------------------------------------------------------------------+
|  1|   20|                                                                                     0.0|
|  1|   66|                                                                                    0.25|
|  1|   70|                                                                                     0.5|
|  1|   99|                                                                                    0.75|
|  1|   99|                                                                                    0.75|
|  3|  -66|                                                                                     0.0|
|  3|  -23|                                                                                     0.5|
|  3|    1|                                                                                     1.0|
|  2|   55|                                                                                     0.0|
|  2|   87|                                                                                     0.5|
|  2|   99|                                                                                     1.0|
+---+-----+----------------------------------------------------------------------------------------+

+---+-----+------------------------------------------------------------------+
| id|value|ntile(3) OVER (ORDER BY value DESC NULLS LAST unspecifiedframe$())|
+---+-----+------------------------------------------------------------------+
|  1|   99|                                                                 1|
|  1|   99|                                                                 1|
|  2|   99|                                                                 1|
|  2|   87|                                                                 1|
|  1|   70|                                                                 2|
|  1|   66|                                                                 2|
|  2|   55|                                                                 2|
|  1|   20|                                                                 2|
|  3|    1|                                                                 3|
|  3|  -23|                                                                 3|
|  3|  -66|                                                                 3|
+---+-----+------------------------------------------------------------------+

+---+-----+----------------------------------------------------------------------+
| id|value|row_number() OVER (ORDER BY value ASC NULLS FIRST unspecifiedframe$())|
+---+-----+----------------------------------------------------------------------+
|  3|  -66|                                                                     1|
|  3|  -23|                                                                     2|
|  3|    1|                                                                     3|
|  1|   20|                                                                     4|
|  2|   55|                                                                     5|
|  1|   66|                                                                     6|
|  1|   70|                                                                     7|
|  2|   87|                                                                     8|
|  1|   99|                                                                     9|
|  1|   99|                                                                    10|
|  2|   99|                                                                    11|
+---+-----+----------------------------------------------------------------------+

+---+-----+--------------------------------------------------------------------------------------+
| id|value|row_number() OVER (PARTITION BY id ORDER BY value ASC NULLS FIRST unspecifiedframe$())|
+---+-----+--------------------------------------------------------------------------------------+
|  1|   20|                                                                                     1|
|  1|   66|                                                                                     2|
|  1|   70|                                                                                     3|
|  1|   99|                                                                                     4|
|  1|   99|                                                                                     5|
|  3|  -66|                                                                                     1|
|  3|  -23|                                                                                     2|
|  3|    1|                                                                                     3|
|  2|   55|                                                                                     1|
|  2|   87|                                                                                     2|
|  2|   99|                                                                                     3|
+---+-----+--------------------------------------------------------------------------------------+
 */