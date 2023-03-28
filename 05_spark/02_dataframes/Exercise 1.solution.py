# DataFrames fundamentals
'''
Read the file "people" into a Dataframe and answer the following questions:

1. Create a new Dataframe with the data of the males only and call it males.
2. How many males are in the table?
3. What is the mean height and weight of the males?
4. What is the height of the tallest female who is older than 40?
5. Create a new Dataframe with two columns for the age and the average weight of the people in this age.
'''
from utils import people_filename
from term_colors import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# 0. get the data
print(h1('0. get the data'))
df = spark.read.csv(people_filename, header=True, inferSchema=True)
df.show(5)
print('\n\n')

# 1. Create a new Dataframe with the data of the males only and call it males.
print(h1('1. Create a new Dataframe with the data of the males only and call it males.'))
males = df.where(df.Sex == 'm').cache()
males.show(5)
print('\n\n')

# 2. How many males are in the table?
print(h1('2. How many males are in the table?'))
print(males.count())
print('\n\n')

# 3. What is the mean height and weight of the males?
print(h1("3. What is the mean height and weight of the males?"))
males\
    .agg({'Height': 'mean', 'Weight': 'mean'})\
    .show()
print('\n\n')

# 4 What is the height of the tallest female who is older than 40?
print(h1("4. What is the height of the tallest female who is older than 40?"))
print(df \
    .where(df.Sex == 'f') \
    .where(df.Age > 40) \
    .agg({'Height': 'max'}) \
    .collect()
)
print('\n\n')

# 5. Create a new Dataframe with two columns for the age and the average weight of the people in this age.
print(h1('5. Create a new Dataframe with two columns for the age and the average weight of the people in this age.'))
df.groupby(
    'Age'
    ).agg(
    {'Weight': 'mean'}
    ).sort(
    'Age'
    ).select(
    'Age',
    F.round('avg(Weight)', 2)
    ).show()
print('\n\n')
