from textblob import TextBlob

texts = [
    '0: you are my god and the best person who ever lived, you are just amazingly good and great',
    '1: Hey there my lover, i love you so much, you are wonderful',
    '2: go away forever, I will ignore you, lets not talk. I will never speak with you again',
    '3: who are you? i do not love you, I am apathetic about you and I feel neutral',
    '4: I hate you hate you hate!!! i hate you, you are terrible and you should definitely not be my friend',
]
for text in texts:
    blob = TextBlob(text)
    print(text, blob.sentiment.polarity)
