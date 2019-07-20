package sparkbasics

import org.apache.spark.storage.StorageLevel

object DataPersistance extends InitializeSpark {

  val rdd = sc.parallelize(Seq(1, 2, 3))
  println(rdd.getStorageLevel)

  val rdd2 = rdd.persist(StorageLevel.DISK_ONLY)
  println(rdd2.getStorageLevel)
  rdd2.unpersist()  // cannot persist persisted data

  val rdd3 = rdd2.persist(StorageLevel.MEMORY_ONLY)
  println(rdd3.getStorageLevel)
  rdd3.unpersist()

  val rdd4 = rdd3.persist(StorageLevel.MEMORY_AND_DISK)
  println(rdd4.getStorageLevel)


}

