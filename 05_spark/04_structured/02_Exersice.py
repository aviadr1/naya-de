'''
Run the verse generator and send it to Kafka.
Each file should have the following structure: {'chapter': i, 'verse': j, 'text': text}

1. Add new column with number of “God” in verse.
2. Filter only verses with number of “God” greater than 1 .
3. Filter only verses that contain “and” .
4. Add new column with number of words in verse.
5. Add new column If the number of words greater than 14,”long”,if between  7 to 14, “medium”, otherwise “small”
6. Remove the column with number of “God” in verse.

'''


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from textblob import TextBlob
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType

# conection between  spark and kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

bootstrapServers = "Cnt7-naya-cdh63:9092"
topics = "bible"


spark = SparkSession\
        .builder\
        .appName("Readbible")\
        .getOrCreate()

# ReadStream from kafka
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", topics)\
    .load()

df_kafka = df_kafka.select(col("value").cast("string"))




df_kafka \
    .writeStream \
    .format("console") \
    .start()\
    .awaitTermination()
