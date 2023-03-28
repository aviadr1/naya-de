'''
Add to dessert a new column called 'no purchase' with the negative of 'purchse'.
'''
from utils import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Read a csv file
dessert = spark.read.csv(dessert_filename,
                         header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

#################################################

dessert = dessert.withColumn('no_purchase', ~dessert.purchase)
dessert.show(5)