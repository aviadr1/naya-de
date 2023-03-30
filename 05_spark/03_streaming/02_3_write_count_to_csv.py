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
ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)

dir_name = 'my_stream_directory'
stream_directory = "file://" + os.getcwd() + '/' + dir_name +'/'

# create directory name
if not os.path.exists(dir_name):
    os.mkdir(dir_name)

# Each RDD in our DStream is a result of an internal file reading, probably implemented by the method textFile().
# We already know that each element in such RDD is a line string from the file. Therefore we should treat our stream accordingly.
my_stream = ssc.textFileStream(stream_directory)

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
            .csv('file:///tmp/god_existence/output_file',
                 header=False,
                 mode='append')

# Create for each rdd that we get
my_stream.pprint()
my_stream.foreachRDD(count_god)


# start the program
ssc.start()
ssc.awaitTermination()



#/tmp/god_existence/output_file
# rm -r /tmp/god_existence/output_file
