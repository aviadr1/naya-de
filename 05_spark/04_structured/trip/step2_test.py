from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import *
import configuration as c

spark = SparkSession.builder.getOrCreate()

spark_df = spark.read.parquet(c.From_Kafka_To_Hdfs_Parquet_path)

print()
print('total rows:', spark_df.count())
print()

spark_df.show(5)
# spark_df.printSchema()

