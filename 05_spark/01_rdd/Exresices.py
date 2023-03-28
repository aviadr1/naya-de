'''
 Read the file "english words" into an RDD and answer the following questions:

1. How many words are listed in the file?
2. What is the most common first letter?
3. What is the longest word in the file?
4. How many words include all 5 vowels?
'''

from pathlib import Path
from pyspark import SparkContext
from term_colors import *
sc = SparkContext.getOrCreate()

folder = Path(__file__).parent
english_words_filename = str(folder / "english words.txt")

# Read a text file
text = sc\
    .textFile(english_words_filename)

print(h1('first 10 lines:'))
print(text.take(10))
print('\n')

# 1. How many words are listed in the file?
print(h1('number of words:'))
print(text.count())
print('\n')

# 2. What is the most common first letter?
text_rdd2 = text\
    .map(lambda x: x.lower())\
    .groupBy(lambda word: word[0])\
    .mapValues(len)\
    .sortBy(lambda word:word[1], ascending= False)

print(h1('most common first letter:'))
print(text_rdd2.take(10))
print('\n')


#option 2
# text_rdd = text.map(lambda word: (word[0].lower(),1))\
#     .reduceByKey(lambda a,b:a+b)\
#     .sortBy(lambda word:word[1], ascending= False)
#
# print(text_rdd.take(10))



# 3. What is the longest word in the file?

word_len_max = text.map(lambda x: len(x)).max()
print(h1('longest word length:'))
print(word_len_max)
longest_word = text.filter(lambda word: len(word)==word_len_max)
print(h1('longest words:'))
print(longest_word.take(60))
print('\n')

# 4. How many words include all 5 vowels?
text_rdd= text.filter(lambda word: set("aeiuo")<set(word))
print(h1('words with all 5 vowels:'))
print(text_rdd.take(10))
print('\n')









