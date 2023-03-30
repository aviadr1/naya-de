"""
Run the verse generator again to create the files on your local file system, but this time
with no limit (limit=None).

1. Use pyspark streaming to process each file and save it as a json file in a different
library (let's call it /tmp/json_local_file/output_file') on your local system. Each file should
have the following structure: {'chapter': i, 'verse': j, 'text': text}
"""
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

outout_dir = "file://tmp/json_local_files"

# create directory name
if not os.path.exists(dir_name):
    os.mkdir(dir_name)

# Each RDD in our DStream is a result of an internal file reading,
# probably implemented by the method textFile().
# We already know that each element in such RDD is a line string from the file.
# Therefore we should treat our stream accordingly.
my_stream = ssc.textFileStream(stream_directory)

def write_json(rdd):
    '''
    This function receives an RDD of strings (bible verses),
    counts the number of appearances of the word "God", and then
    adds this information to a designated json file.
    '''

    if not rdd.isEmpty():
        jsn = rdd\
            .map(lambda line : (line.split( )[0].split(":")[0], line.split( )[0].split(":")[1] , " ".join(line.split( )[1:])))

        spark.createDataFrame(jsn, schema=['chapter','verse','text']) \
            .write.json('file:///tmp/json_local_file/output_file', mode='append')

# Create for each rdd that we get
my_stream.foreachRDD(write_json)
my_stream.pprint()


# start the program
ssc.start()
ssc.awaitTermination()


