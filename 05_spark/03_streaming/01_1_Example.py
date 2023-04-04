'''
Write an application that sums all the numbers that a user is entering the console
and prints that (changing) sum.
For simplicity, you may assume that the user sends only integers.
'''
import os
import re
from pathlib import Path
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

from utils import *

sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)

print('writing to', sum_of_files_path)


def save_as_csv(rdd):
    if not rdd.isEmpty():
        spark.createDataFrame(rdd, schema=['num'])\
            .write\
            .csv('file://' + str(sum_of_files_path), 
                 header=False,
                 mode='append')

# 1. first, YOU need to run the program `nc -lk 9999` in the terminal to create a socket
# 2. then YOU need to type numbers in the terminal
# 3. NOW, this PROGRAM can read from socket
my_stream = ssc.socketTextStream("localhost", 9999)

# change to integer and save in csv file
sum_counts = my_stream\
    .map(lambda num: (int(num),))\
    .foreachRDD(save_as_csv)

# start the program
ssc.start()
ssc.awaitTermination() # to stop THIS PROGRAM, press ctrl+c  


