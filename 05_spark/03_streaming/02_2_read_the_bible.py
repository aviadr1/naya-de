'''
Read the bivle and print it
'''
import os
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)

dir_name = 'my_stream_directory'
stream_directory = "file://" + os.getcwd() + '/' + dir_name +'/'

# create directory name
if not os.path.exists(dir_name):
    os.mkdir(dir_name)

# Each RDD in our DStream is a result of an internal file reading,
# probably implemented by the method textFile().
# We already know that each element in such RDD is a line string from the file.
# Therefore we should treat our stream accordingly.
my_stream = ssc.textFileStream(stream_directory)

# print the data
my_stream.pprint()

# start the program
ssc.start()
ssc.awaitTermination()


