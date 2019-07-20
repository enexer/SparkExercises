package sparkbasics

object DFStatFunctions extends InitializeSpark {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val rows = Seq.tabulate(100) { i =>
    if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
  }
  val df = spark.createDataFrame(rows).toDF("a", "b")
  // find the items with a frequency greater than 0.4 (observed 40% of the time) for columns
  // "a" and "b"
  val freqSingles = df.stat.freqItems(Seq("a", "b"), 0.4)
  freqSingles.show()
//  +-----------+-------------+
//  |a_freqItems|  b_freqItems|
//  +-----------+-------------+
//  |    [1, 99]|[-1.0, -99.0]|
//  +-----------+-------------+
  // find the pair of items with a frequency greater than 0.1 in columns "a" and "b"
  val pairDf = df.select(struct("a", "b").as("a-b"))
  val freqPairs = pairDf.stat.freqItems(Seq("a-b"), 0.1)
  freqPairs.select(explode($"a-b_freqItems").as("freq_ab")).show()
//  +----------+
//  |   freq_ab|
//  +----------+
//  |  [1,-1.0]|
//    |   ...    |
//  +----------+


}
