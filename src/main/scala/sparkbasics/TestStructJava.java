package sparkbasics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

import org.apache.spark.sql.functions.*;

public class TestStructJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        //----------
        StructType schema = new StructType(new StructField[]{
                new StructField("_1", DataTypes.StringType, false, Metadata.empty()),
                new StructField("_2", DataTypes.StringType, false, Metadata.empty()),
                new StructField("_3", DataTypes.StringType, false, Metadata.empty())
        });

        JavaRDD<String> rdd1 = spark
                .range(5)
                .javaRDD()
                .map(s -> s+",b,c");

        JavaRDD<Row> rdd2 = rdd1.map(s -> s.split(","))
                .map(s -> RowFactory.create((Object[]) s));

        Dataset<Row> df = spark.createDataFrame(rdd2, schema);

        df.show();

        gitSystem.out.println(Arrays.asList(df.columns()).contains("_1"));
        System.out.println(Arrays.asList(df.columns()).contains("2"));
        System.out.println(Arrays.asList(df.columns()).contains("_2"));



        df.filter(df.col("").equalTo(1.0)).count();


        /*

+---+---+---+
| _1| _2| _3|
+---+---+---+
|  0|  b|  c|
|  1|  b|  c|
|  2|  b|  c|
|  3|  b|  c|
|  4|  b|  c|
+---+---+---+

         */

    }
}
