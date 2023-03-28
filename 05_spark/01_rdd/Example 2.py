'''
How many words are in the book?
'''
from pathlib import Path
from pyspark import SparkContext
from term_colors import *
sc = SparkContext.getOrCreate()

folder = Path(__file__).parent
mobydick_filename = str(folder / "melville-moby_dick.txt")

# Read a text file
text = sc\
    .textFile(mobydick_filename)
print('\n\n')

print(h1('first 30 lines:'))
print(text.take(30))
print('\n')

# We wish to clean all the non-letter characters using map(),
# so we write an auxiliary function called clean_word.
def clean_word(word):
    return ''.join([ch for ch in word if ch.isalpha()])

# We want to make an RDD of separate words,
# so we will go through the several processing steps.
# First, we split the lines into words using flatMap().

words_rdd = text\
    .flatMap(lambda line: line.split())


print(h1('first 30 words:'))
print(words_rdd.take(30))
print('\n')

words_rdd_clean = text\
    .flatMap(lambda line: line.split())\
    .map(clean_word)
print(h1('first 30 words after cleaning:'))
print(words_rdd_clean.take(30))
print('\n')


words_rdd_clean_w = text\
    .flatMap(lambda line: line.split())\
    .map(clean_word)\
    .filter(len) # The filtering with len utilizes the fact that when checking an integer, then Python regards 0 as False and anything else as True.
print(h1('first 30 words after cleaning and filtering:'))
print(words_rdd_clean_w.take(30))
print('\n')

# We can apply the action count() to simply find the number of elements in the RDD words.
print(h1('number of words:'))
print(words_rdd.count())