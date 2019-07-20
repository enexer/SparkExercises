package sparkbasics

import init.InitSpark

object RDDWithOwnType extends InitSpark {

  import spark.implicits._

  case class Shape(var x: Int, var y: Int, var z: Int)

  val data = Seq(Shape(1, 1, 1), Shape(1, 2, 2), Shape(3, 3, 3))
  val rdd = sc.parallelize(data)

  println(rdd.first().toString)

  rdd.map(s => {
    // Shape.apply(s.x+10,s.y,s.z)
    s.x += 100;
    s.y += 1000;
    s.z += 100000
    s
  }).collect().foreach(s => println(s))


  val ds = data.toDS()
  ds.map(s=> {
    s.x += 100;
    s.y += 1000;
    s.z += 100000
    s
  }).collect().foreach(s => println(s))


  println(ds.map(_.z).reduce((a, b) => a + b))

}
