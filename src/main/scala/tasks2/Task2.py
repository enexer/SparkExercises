import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext("local", "app")
sc.setLogLevel('OFF')
spark = SparkSession(sc)

dataA = spark.read \
    .option("sep", "\t") \
    .schema("id STRING, currency STRING, date STRING, value DECIMAL(10,2), code STRING") \
    .csv("data/tasks2/t2a.txt")

dataB = spark.read \
    .option("sep", "\t") \
    .schema("id INT, code STRING") \
    .csv("data/tasks2/t2b.txt") \
    .select("code")

dataA.join(f.broadcast(dataB), 'code') \
    .na.drop() \
    .select(f.col('currency').alias('CUR'), 'date', 'value', 'code') \
    .filter(f.year(f.col('date')) == 2012) \
    .filter(f.col('code') != 'FR') \
    .filter((f.col('CUR') == "Euro") | (f.col('CUR') == "Dollar")) \
    .groupBy(f.col('CUR')) \
    .agg(f.avg(f.col('value')).alias('avg')) \
    .orderBy(f.col('avg').desc()) \
    .coalesce(1) \
    .write \
    .option('sep", ''\t') \
    .option('header', 'true') \
    .csv('data/tasks2/out/t2')

# +------+--------+
# |   CUR|     avg|
# +------+--------+
# |  Euro|6.045000|
# |Dollar|5.070968|
# +------+--------+
