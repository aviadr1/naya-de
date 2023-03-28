'''
How many groups purchased a dessert on Mondays?
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

col = (dessert.weekday == 'Monday') & (dessert.purchase)
result_df = dessert.where(col).cache()
print('\n\n')
print("number of rows where dessert was purchased on Mondays:")
print(result_df.count())
print('\n')
result_df.show(3)

#option2
# print(dessert\
#       .where((dessert.weekday == 'Monday') & (dessert.purchase))\
#       .show(3))
