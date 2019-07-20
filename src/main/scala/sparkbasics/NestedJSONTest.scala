package sparkbasics

import init.InitSpark

object NestedJSONTest extends InitSpark{

  import spark.implicits._


  val data = spark.read.json("data/github.json")
  data.groupBy("payload.pull_request.user.login").count().orderBy('count.desc).show()
}
