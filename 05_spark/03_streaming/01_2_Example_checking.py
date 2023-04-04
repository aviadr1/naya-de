from random import random
from time import sleep
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

from utils import *

sc = SparkContext.getOrCreate()
# ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)


print('writing to', sum_of_files_path)

# this file will be created by running the 00_Example.py program first
while True:

    df = spark.read.csv('file://' + sum_of_files_path)
    my_sum = df\
        .agg({'_c0': 'sum'}) \
        .collect()[0]['sum(_c0)']

    # df = spark.read.csv('file://' + sum_of_files_path, schema=['number'])
    # my_sum = df.agg({'number': 'sum'}).collect()[0]['sum(number)']

    print(my_sum)
    sleep(random())




#Once in a while during your work it is useful to clean the directory.
# dir_name = '/tmp/sum_of_nums_files/output_file'
# for f_name in os.listdir(dir_name):
#     os.remove(dir_name + '/' + f_name)
# os.listdir(dir_name)