package sparkbasics

import init.InitSpark

import scala.util.Random

object RDDtoDFAndJoin extends InitSpark {


  val d1 = (1 to 10).map(s => ("" + s + "", Random.nextInt(99)))
  val rdd1 = sc.parallelize(d1)

  val d2 = (1 to 10).map(s => ("" + s + "", randomStr(6)))
  val rdd2 = sc.parallelize(d2)

  println(rdd1.first() + "" + rdd2.first())

  import spark.implicits._

  val df1 = rdd1.toDF("id","val")
  val df2 = rdd2.toDF("id","val")

  df1.join(df2).where(df1("id")===df2("id"))
    //.select("id")
    .show()


  df1.createTempView("df1")
  df2.createTempView("df2")
  spark.sql("select * from df1 join df2 on df1.id=df2.id order by df1.id asc").show()


  def randomStr(length: Int): String = {
    (for (_ <- 0 to length) yield Random.nextPrintableChar()+"").reduce(_ + _)
  }

}
