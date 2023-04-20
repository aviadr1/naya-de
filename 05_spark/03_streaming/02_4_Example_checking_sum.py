'''
In this example we are going to count the number of occurences of the word "God" in the bible,
 by reviewing a stream of verses.
'''
import os
import re
from random import random
from time import sleep
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

from utils import *

sc = SparkContext.getOrCreate()
# ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)


while True:
    # read csv file with spark
    df = spark.read.csv('file://' + str(output_folder / 'god_existence/'))
    df.createOrReplaceTempView("alin")
    df=spark.sql("select sum(_c0) as running_sum from alin ")
    df.show()



