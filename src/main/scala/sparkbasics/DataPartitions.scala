package sparkbasics

object DataPartitions extends InitializeSpark {

  val data = 1 to 999999 by 2
  val rdd = sc.parallelize(data)
  printX(rdd.partitions.length)

  val rdd2 = rdd.repartition(2)
  printX(rdd2.partitions.length)

  val rdd3 = rdd2.coalesce(1)
  printX(rdd3.partitions.length)
}
