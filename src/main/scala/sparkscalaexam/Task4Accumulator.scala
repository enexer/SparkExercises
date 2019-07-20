package sparkscalaexam

import java.util.Scanner

import init.InitSpark
import org.apache.spark.util.LongAccumulator

/**
  * Word count with accumulator.
  * Count elements contains str using accumulator.
  */
object Task4Accumulator extends InitSpark {

  var accumulator: LongAccumulator = sc.longAccumulator("sparkAccumulator")

  // "lines with words"
  sc.parallelize(List("ok ok ok", "ww wqe we", "qwe oooo"))
    .flatMap(s => s.split("\\s+")) //  "\\s+"
    .filter(s => s.contains("o")) // filer out
    .map(_ => accumulator.add(1)).first()

  // "list of numbers"
  sc.parallelize(List(1, 2, 3, 4))
    .map(_ => accumulator.add(1))
    .map(_ => accumulator.add(1))
    .map(_ => accumulator.add(1))
    .first()

  // println(accumulator.value)

  ///sc.wait

  scala.io.StdIn.readLine()
}