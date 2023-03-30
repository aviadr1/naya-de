'''
write a script that loads the json hdfs file (containing the data streamed up to that point)
and make a histogram of the number of verses per chapter.

'''



from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)


target_file = 'hdfs://Cnt7-naya-cdh63:8020/user/naya'

df = spark.read.json(target_file)
df.count()

df_pd = df.toPandas()
df_pd_gby = df_pd.groupby('chapter').size() #number of elements in this list
print(df_pd_gby)
