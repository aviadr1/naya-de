from textblob import TextBlob


print(TextBlob('1: i love you').sentiment.polarity)
print(TextBlob('2: i do not love you').sentiment.polarity)
print(TextBlob('3: i hate you').sentiment.polarity)
print(TextBlob('4:go away').sentiment.polarity)