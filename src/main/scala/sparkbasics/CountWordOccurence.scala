package sparkbasics

object CountWordOccurence extends InitializeSpark {

  val rdd = sc.parallelize(List("ok ok", "no ok", "yes yes ok"))

  rdd
    .flatMap(_.split("\\s+"))    //  split elements from lines using flatMap "1 2" => "1", "2"
    .map((_, 1L)) // add 1L for counting
    .reduceByKey(_ + _) // count each word occurrence
    .sortBy(_._2, false) // sort by occurrence descending
    .foreach(println(_))
}
