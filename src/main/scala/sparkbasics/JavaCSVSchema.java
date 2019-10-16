package sparkbasics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.functions.*;

public class JavaCSVSchema {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaAccumulator");
        SparkContext sc = new SparkContext(sparkConf);
        SparkSession ss = SparkSession.builder().getOrCreate();

        String schema1 = "sepal_length DOUBLE," +
                        "sepal_width DOUBLE," +
                        "petal_length DOUBLE," +
                        "petal_width DOUBLE," +
                        "species STRING";

        Dataset<Row> ds = ss.read().schema(schema1).csv("data/iris3.csv");
        ds.printSchema();
        ds.show(5);

        StructType schema2 = new StructType()
                .add("sepal_length#", DoubleType)
                .add("sepal_width#", DoubleType)
                .add("petal_length#", DoubleType)
                .add("petal_width#", DoubleType)
                .add("species#", StringType);

        ss.read().schema(schema2).csv("data/iris3.csv").printSchema();


        // SQL.FUNCTIONS STATIC IMPORT
        ss.read()
                .schema(schema2)
                .csv("data/iris3.csv")
                .select(col("sepal_length#").cast("int"))
                .printSchema();


        Column col = org.apache.spark.sql.functions.max(new Column("asd"));
        col.cast("int");


    }
}
/*
root
 |-- sepal_length: double (nullable = true)
 |-- sepal_width: double (nullable = true)
 |-- petal_length: double (nullable = true)
 |-- petal_width: double (nullable = true)
 |-- species: string (nullable = true)

+------------+-----------+------------+-----------+-------+
|sepal_length|sepal_width|petal_length|petal_width|species|
+------------+-----------+------------+-----------+-------+
|         5.1|        3.5|         1.4|        0.2| setosa|
|         4.9|        3.0|         1.4|        0.2| setosa|
|         4.7|        3.2|         1.3|        0.2| setosa|
|         4.6|        3.1|         1.5|        0.2| setosa|
|         5.0|        3.6|         1.4|        0.2| setosa|
+------------+-----------+------------+-----------+-------+
only showing top 5 rows

root
 |-- sepal_length#: double (nullable = true)
 |-- sepal_width#: double (nullable = true)
 |-- petal_length#: double (nullable = true)
 |-- petal_width#: double (nullable = true)
 |-- species#: string (nullable = true)

root
 |-- sepal_length#: integer (nullable = true)
 */

