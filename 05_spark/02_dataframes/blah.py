from pyspark.sql import SparkSession
from utils import *

spark = SparkSession.builder \
    .master("yarn") \
    .config("spark.executor.instances", "4") \
    .getOrCreate()

# Read a csv file
dessert = spark.read.csv("file://" + dessert_filename, header=True, inferSchema=True)\
    .drop('id')\
    .withColumnRenamed('day.of.week', 'weekday') \
    .withColumnRenamed('num.of.guests', 'num_of_guests')\
    .withColumnRenamed('dessert', 'purchase')\
    .withColumnRenamed('hour', 'shift')

dessert.show(5)
dessert.printSchema()