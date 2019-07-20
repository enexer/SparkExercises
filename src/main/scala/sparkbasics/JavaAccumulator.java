package sparkbasics;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.util.Scanner;

public class JavaAccumulator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaAccumulator");
        SparkContext sc = new SparkContext(sparkConf);
        SparkSession ss = SparkSession.builder().getOrCreate();

        LongAccumulator acc = sc.longAccumulator("accumulator123");
        Dataset<Long> ds = ss.range(100);
        ds.foreach((ForeachFunction<Long>) s->acc.add(1L));
        System.out.println(acc.value());
        ds.foreach((ForeachFunction<Long>) s->acc.add(2L));
        System.out.println(acc.value());
        new Scanner(System.in).nextInt();
    }
}
