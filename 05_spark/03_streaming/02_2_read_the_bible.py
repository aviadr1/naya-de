'''
Read the bivle and print it
'''
import os
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from utils import *

sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)

dir_name = output_folder / 'bible'
dir_name.mkdir(exist_ok=True, parents=True)

# create directory name
if not os.path.exists(str(dir_name)):
    os.mkdir(str(dir_name))

# Each RDD in our DStream is a result of an internal file reading,
# probably implemented by the method textFile().
# We already know that each element in such RDD is a line string from the file.
# Therefore we should treat our stream accordingly.
print('reading from', dir_name, '...')
my_stream = ssc.textFileStream("file://" + str(dir_name) + '/')

# print the data
my_stream.pprint()

# start the program
ssc.start()
ssc.awaitTermination()


