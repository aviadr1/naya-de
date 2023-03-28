'''
What is the most common word in the book?
'''
from pathlib import Path
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

folder = Path(__file__).parent
mobydick_filename = str(folder / "melville-moby_dick.txt")

# Read a text file
text = sc\
    .textFile(mobydick_filename)

# We wish to clean all the non-letter characters using map(), so we write an auxiliary function called clean_word.
def clean_word(s):
    return ''.join([ch for ch in s if ch.isalpha()])

# We want to make an RDD of separate words,
# so we will go through the several processing steps. First, we split the lines into words using flatMap().
# The filtering with len utilizes the fact that when checking an integer, then Python regards 0 as False and anything else as True.
words_rdd = text\
    .flatMap(lambda line: line.split())\
    .map(clean_word)\
    .filter(len)


# We can use the groupBy() method to get a new pair RDD as described (poorly) in the documentation.
# Let's explore the groupBy() method before we apply it to the words RDD.
word_iterator = words_rdd\
    .groupBy(lambda word: word)

# Pair RDDs support the mapValues() method, which is a conveniency around map() which conserves the keys of the original RDD.
word_count = word_iterator\
    .mapValues(len)

# Finally, the order of word_count is not guaranteed, so we have to sort its elements.
word_count = word_count\
    .sortBy(lambda word_count: word_count[1], ascending=False)

print(word_count.take(10))
print('\n')

### alternatively, we can do it in a slightly different way without groupby

# map each word to a tuple with the word as the key and 1 as the value
word_count = words_rdd.map(lambda word: (word, 1))

# reduce by key to count the occurrences of each word
word_count = word_count.reduceByKey(lambda a, b: a + b)

word_count = word_count.sortBy(lambda word_count: word_count[1], ascending=False)

# print the word counts
print(word_count.take(10))