'''
In this example we are going to count the number of occurrences of the word "God" in the bible,
by reviewing a stream of verses.
'''
import os
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
from utils import *

ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)

dir_name = output_folder / 'bible'
dir_name.mkdir(exist_ok=True, parents=True)
god_folder = output_folder / 'god_existence'
god_folder.mkdir(exist_ok=True, parents=True)

# Each RDD in our DStream is a result of an internal file reading, probably implemented by the method textFile().
# We already know that each element in such RDD is a line string from the file. Therefore we should treat our stream accordingly.
print('reading from', dir_name, '...')
my_stream = ssc.textFileStream("file://" + str(dir_name) + '/')

def count_god(rdd):
    '''
    This function receives an RDD of strings (bible verses),
    counts the number of appearances of the word "God", and then
    adds this information to a designated CSV file.
    '''
    if not rdd.isEmpty():
        cnt = rdd\
            .flatMap(lambda line: line.split())\
            .filter(lambda word: word=='God')\
            .count()
        rdd_sum = sc.parallelize([(cnt,)])
        spark.createDataFrame(rdd_sum,
                              schema=['cnt'])\
            .write\
            .csv('file://' + str(god_folder),
                 header=False,
                 mode='append')

# Create for each rdd that we get
# my_stream.pprint()
my_stream.foreachRDD(count_god)


# start the program
ssc.start()
ssc.awaitTermination()



#/tmp/god_existence/output_file
# rm -r /tmp/god_existence/output_file
