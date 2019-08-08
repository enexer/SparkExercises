import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext('local', 'app')
sc.setLogLevel('OFF')
spark = SparkSession(sc)

data = spark.read.json('data/tasks2/t1.json')

data.na.drop() \
    .filter(f.col('country') != 'Brazil') \
    .filter((f.col('balance') > 100) & (f.col('balance') < 700)) \
    .groupBy(f.col('country')) \
    .agg(f.avg(f.col('balance')).alias('avg')) \
    .orderBy(f.col('avg').desc()) \
    .write \
    .option('header', 'true') \
    .option('delimiter', '\t') \
    .csv('data/tasks2/out/t1')
