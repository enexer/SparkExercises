package sparkbasics

import init.InitSpark

object GroupWithListOfCounts extends InitSpark {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df = spark.read.csv("data/list_group.csv").toDF("url","browser")
  df.show()

  df.withColumn("num", lit(1))
    .groupBy('url, 'browser)
    .agg(sum('num).as("num"))
   //// .orderBy('num.desc)
    .select('url, format_string("(%s)",concat_ws(",", 'browser, 'num)).as("dst"))
    .groupBy('url)
    .agg(sort_array(collect_list('dst))).toDF("URL","FrequentlyUsedBrowser")
    .orderBy('url)
    .show(false)

  val ok = "asd"
  unix_timestamp(lit(ok))
  /*
  +---+-------+
  |url|browser|
  +---+-------+
  |  A| Chrome|
  |  B| Chrome|
  |  C|Firefox|
  |  A| Chrome|
  |  A|Firefox|
  |  A|  Opera|
  |  A| Chrome|
  |  B| Chrome|
  |  B|Firefox|
  |  C|    Tor|
  +---+-------+

  +---+------------------------------------+
  |URL|FrequentlyUsedBrowser               |
  +---+------------------------------------+
  |A  |[(Chrome,3), (Firefox,1), (Opera,1)]|
  |B  |[(Chrome,2), (Firefox,1)]           |
  |C  |[(Firefox,1), (Tor,1)]              |
  +---+------------------------------------+

   */

}
