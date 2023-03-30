"""
write a script that loads the json local file (containing the data
streamed up to that point) and make a histogram of the number of verses per
chapter.
"""



from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


df_schema = StructType([
    StructField('chapter', StringType(), True),
    StructField('verse', StringType(), True),
    StructField('text', StringType(), True)
])

df = spark.read.schema(df_schema).json("/tmp/json_local_file/output_file/")
# print("print schema")
# df.printSchema()

# print("print df where chapter=1")
# df\
#     .filter(df.chapter == "1")\
#     .orderBy(df.verse)\
#     .show(2)

# print("print group by chapter and count")
# df\
#     .groupBy(df.chapter)\
#     .count()\
#     .orderBy(df.chapter)\
#     .show(2)


pandas_df = df.toPandas()

pandas_df['chapter'] = pd.to_numeric(pandas_df['chapter'])
pandas_df['verse'] = pd.to_numeric(pandas_df['verse'])

# print("print info")
# pandas_df.info()


print("print plot pd")
print(pandas_df.groupby('chapter')['verse'].count())



