from pyspark import SparkContext
from pyspark.sql import *
import pyspark.sql.functions as f

sc = SparkContext('local', 'app')
sc.setLogLevel('OFF')
spark = SparkSession(sc)

spark.readStream \
    .format('socket') \
    .option('host', 'localhost') \
    .option('port', 9999) \
    .load() \
    .agg(f.collect_list(f.col('value')).alias('list')) \
    .select(f.explode(f.col('list'))) \
    .writeStream \
    .format('console') \
    .outputMode('complete') \
    .option('checkpointLocation', 'data/checkpoint') \
    .start() \
    .awaitTermination()


# $ ncat -lk 9999
#
# first
# second
# third

# -------------------------------------------
# Batch: 0
# -------------------------------------------
# +---+
# |col|
# +---+
# |   |
# +---+
#
# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +-----+
# |  col|
# +-----+
# |     |
# |first|
# +-----+
#
# -------------------------------------------
# Batch: 2
# -------------------------------------------
# +------+
# |   col|
# +------+
# |      |
# | first|
# |second|
# +------+
#
# -------------------------------------------
# Batch: 3
# -------------------------------------------
# +------+
# |   col|
# +------+
# |      |
# | first|
# |second|
# | third|
# +------+