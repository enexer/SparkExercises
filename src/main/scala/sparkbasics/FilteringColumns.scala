package sparkbasics

import init.InitSpark
import org.apache.spark.sql.Row

object FilteringColumns extends InitSpark {

  import spark.implicits._

  val df= Seq(("1","A","3","4"),("1","2","?","4"),("1","2","3","4")).toDF()
  df.show()

  val first = df.first()
  val size = first.length
  val diffStr = "#"
  val targetStr = "1"

   def rowToArray(row: Row): Array[String] = {
     val arr = new Array[String](row.length)
     for (i <- 0 to row.length-1){
       arr(i) = row.getString(i)
     }
     arr
   }

  def compareArrays(a1: Array[String], a2: Array[String]): Array[String] = {
    val arr = new Array[String](a1.length)
    for (i <- 0 to a1.length-1){
      arr(i) = if (a1(i).equals(a2(i)) && a1(i).equals(targetStr)) a1(i) else diffStr
    }
    arr
  }

  val diff = df.rdd
    .map(rowToArray)
    .reduce(compareArrays)

  val cols = (df.columns zip diff).filter(!_._2.equals(diffStr)).map(s=>df(s._1))

  df.select(cols:_*).show()

  /*
  +---+---+---+---+
  | _1| _2| _3| _4|
  +---+---+---+---+
  |  1|  A|  3|  4|
  |  1|  2|  ?|  4|
  |  1|  2|  3|  4|
  +---+---+---+---+

  +---+
  | _1|
  +---+
  |  1|
  |  1|
  |  1|
  +---+
   */

}
