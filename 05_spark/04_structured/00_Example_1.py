'''
1. ReadStream From console (socket)
2. Split the word in line
3. Filter to get only names with "i"
4. Change the first letter to uppercase
5.  Add the number of letters in any word
6. Filter words with greater than 3 letters
7. drop the word count
4. send the answer to console
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf,StringType

spark = SparkSession\
        .builder\
        .appName("Counter")\
        .getOrCreate()

# 1. first, YOU need to run the program `nc -lk 9999` in the terminal to create a socket
# 2. then YOU need to type numbers in the terminal
# 3. NOW, this PROGRAM can read from socket
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

print(type(socketDF))
print(socketDF)

# Split the lines into words
words = socketDF.select(
   explode(
       split(socketDF.value, " ")
   ).alias("word")
)
#https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/

# Filter to get only names with "i"
words = words.filter(words.word.like('%i%'))

# Change the first letter to uppercase
def capitalize(s):
  return s[0].upper() + s[1:]

capitalize_udf = udf(f=capitalize, returnType=StringType())


words = words\
    .withColumn('word', capitalize_udf('word'))\
#
# # Add the number of letters in any word
words = words.withColumn('wordCount', size(split(col('word'), ''))-1)

# Take words with greater than 3 letters
words = words.where(words.wordCount>3)

# drop the word count
words = words.drop('wordCount')

# send to console
words \
    .writeStream \
    .format("console") \
    .start()\
    .awaitTermination()
