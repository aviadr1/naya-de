'''

2. Monitor this staging folder (/my_stream_directory/) and use pyspark streaming to upload its
content to a single json file (at /user/naya/json_hdfs_bible.json) in HDFS.

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
target_file = r'hdfs://Cnt7-naya-cdh63:8020/user/naya'


my_stream = ssc.textFileStream(stream_directory)

def parse_verse(full_text):
    splitted_full_text = full_text.split()
    chapter, verse = map(int, splitted_full_text[0].split(':'))
    text = ' '.join(splitted_full_text[1:])
    return chapter, verse, text

def append_verse_to_json(rdd):
    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd.map(parse_verse),
                           schema=['chapter', 'verse', 'text'])
        df.write.json(target_file,
                      mode='append')


my_stream.foreachRDD(append_verse_to_json)
my_stream.pprint()

ssc.start()
ssc.awaitTermination()