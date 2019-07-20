package sparkbasics

import init.InitSpark

object BroadcastExample extends InitSpark{

  val rdd = sc.parallelize(Seq(1,2,3,4))
  val bc = sc.broadcast(3.14)
  rdd.map(_+bc.value).collect().foreach(println(_))
  bc.destroy()
}
