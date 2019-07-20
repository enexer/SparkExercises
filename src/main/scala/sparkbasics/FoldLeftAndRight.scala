package sparkbasics

import init.InitSpark
import org.apache.spark.sql.types.{StructField, StructType}

object FoldLeftAndRight extends InitSpark {


  //  val nums = List(2, 3, 4)
  //  println(nums.foldLeft(1)((agg, next) => (agg * next).toInt))
  //  val z = nums.foldRight(1)((a,b)=>a/b.toInt)
  //  println(z)
  //  println(Seq("a","b","c").foldLeft("1")((a, b) => a + b))

  import spark.implicits._
  import org.apache.spark.sql.functions._


  // PROBLEM : REPLACE SPACES IN DATA -----------------------------------------------
  val sourceDF = Seq(
    ("  p a   b l o", "Paraguay", "x x x"),
    ("Neymar", "B r    asil", "d d d")
  ).toDF("name", "country", "oko")

  sourceDF.show()

  // using only spark SQL functions ///////
  // You must use function on each column
  sourceDF.select(regexp_replace('name," ", "").as("name"),
    regexp_replace('country," ", "").as("country"))
    .show()
  ////////////////////////////////////

  // using FoldLeft + sql function ///////
  // sql func on all available columns********
  sourceDF.columns.foldLeft(sourceDF) { (memoDF, colName) =>
    memoDF.withColumn(colName, regexp_replace(col(colName), "\\s+", "")
    )
  }.show()

  //---------------------------------------------------------------------------------




  // PROBLEM :  REPLACE SPACES IN COLUMN NAMES ----------------------------------------------
  val sourceDF2 = Seq(
    ("funny", "joke")
  ).toDF("A b C", "de F")


  // edit structType and create new DF
  val st = sourceDF2.schema.map(s=>{
    val newName =  s.name.replace(" ","_")
    StructField(newName,s.dataType,s.nullable,s.metadata)
  })
  val st2 = StructType(st)
  spark.createDataFrame(sourceDF2.rdd, st2).show()

  // USING FoldLeft

  val actualDF = sourceDF2
    .columns
    .foldLeft(sourceDF2) { (memoDF, colName) =>
      memoDF
        .withColumnRenamed(
          colName,
          colName.toLowerCase().replace(" ", "_")
        )
    }.show()
}
/*
+-------------+-----------+-----+
|         name|    country|  oko|
+-------------+-----------+-----+
|  p a   b l o|   Paraguay|x x x|
|       Neymar|B r    asil|d d d|
+-------------+-----------+-----+

+------+--------+
|  name| country|
+------+--------+
| pablo|Paraguay|
|Neymar|  Brasil|
+------+--------+

+------+--------+---+
|  name| country|oko|
+------+--------+---+
| pablo|Paraguay|xxx|
|Neymar|  Brasil|ddd|
+------+--------+---+

+-----+----+
|A_b_C|de_F|
+-----+----+
|funny|joke|
+-----+----+

+-----+----+
|a_b_c|de_f|
+-----+----+
|funny|joke|
+-----+----+
 */