'''
 Read the file "english words" into an RDD and answer the following questions:

1. How many words are palindrome?
2. What is the most common last letter?
3. What is the 10 shortest word in the file?
4. How many words include one of 5 vowels?
5. How many words for each first letter?
6. How many words start with 'a'
7. How many words contain the name "pen" ?
8. How many words contain the name "pen"  and her last letter is "L"?
9. Which words contain only 2 letter?
10. Which words contain only 2 letter and thw word length is greater than 2 ?
'''

from pathlib import Path
from pyspark import SparkContext
from term_colors import *
sc = SparkContext.getOrCreate()

folder = Path(__file__).parent
english_words_filename = str(folder / "english words.txt")
text = sc.textFile(english_words_filename).cache()

print(h1('first 5 lines:'))
print(text.take(5))
print('\n')
      
### 1. How many words are palindrome?
print(h1('1. How many words are palindrome?'))
print(text.filter(
    lambda word: word == word[::-1]
    ).count()
    )
print('\n')

# 2. What is the most common last letter?
print(h1('2. What is the most common last letter?'))
print(text.groupBy(
    lambda word : word[-1]
    ).mapValues(
    len
    ).sortBy(
    lambda word:word[1], ascending= False
    ).take(3)
)
print('\n')

# 3. What is the 10 shortest word in the file?
print(h1('3. What is the 10 shortest word in the file?'))
print(text.sortBy(
    lambda word: len(word)
    ).map(
    lambda word: word.lower()
    ).distinct(
    ).filter(
    lambda word: len(word)>=2
    ).take(10)
)
print('\n')

# 4. How many words include one of 5 vowels?
vowels = {'a','e','i','o','u'}
print(h1('4. How many words include one of 5 vowels?'))
rdd = text.filter(
    ## exactly one vowel
    lambda word: len(vowels & set(word.lower())) == 1
    ).cache()
print(rdd.count(), rdd.take(10))
print('\n')

# 5. How many words for each first letter?
print(h1('5. How many words for each first letter?'))
print(text.groupBy(
    lambda word: word[0].lower()
    ).mapValues(
    len
    ).collect()
)
print('\n')

# 6. How many words start with 'a'
print(h1('6. How many words start with "a"?'))
rdd = text.filter(lambda word: word[0].lower() == 'a').cache()
print(rdd.count(), rdd.take(5))
print('\n')

# 7. How many words contain the name "pen" ?
print(h1('7. How many words contain the name "pen" ?'))
rdd=text.filter(lambda word: 'pen' in word.lower()).cache()
print(rdd.count(), rdd.take(10))
print('\n')

# 8. How many words contain the name "pen"  and her last letter is "L"?
print(h1('8. How many words contain the name "pen"  and her last letter is "L"?'))
rdd = text.filter(lambda word: 'pen' in word.lower() and word[-1] == 'l').cache()
print(rdd.count(), rdd.take(10))
print('\n')

# 9. Which words contain only 2 letter?
print(h1('9. Which words contain only 2 letter?'))
print(text.filter(lambda word: len(set(word)) == 2).take(10))
print('\n')

# 10. Which words contain only 2 letter and thw word length is greater than 2 ?
print(h1('10. Which words contain only 2 letter and thw word length is greater than 2 ?'))
print(text.filter(lambda word: len(set(word))==2 and len(word)>2).take(10))
print('\n')
