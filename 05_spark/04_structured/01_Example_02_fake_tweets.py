#
# for this example you need to install the faker library
#   pip install faker
#
import json
from kafka import KafkaProducer
from time import sleep
from faker import Faker
import time
import random
from pprint import pprint

fake = Faker()

sweet_words = [
    'danish','cheesecake','sugar',
    'Lollipop','wafer','Gummies',
    'sesame','Jelly','beans',
    'pie','bar','Ice','oat', 'cream', 'chocalte', 
    'taste', 'smell', 'is', 'a', 'the', 'now',
    'yummy', 'digusting', 'tasty', 'revolting', 'like', 
    'good', 'bad', 'agreed', 'myself', 'for',
]

producer = KafkaProducer(
    bootstrap_servers='Cnt7-naya-cdh63:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

while True:
    user_events = {}
    user_events['tweet_created_at'] = fake.date_time().ctime()
    user_events['tweet_id'] = str(fake.ssn())
    user_events['text'] = fake.sentence(ext_word_list=sweet_words, nb_words=10)
    user_events['user_acount_created_at'] = fake.date_time().ctime()
    user_events['user_id'] = str(fake.ssn())
    user_events['name'] = fake.name()
    user_events['followers_count'] = int(fake.random_number())
    user_events['friends_count'] = int(fake.random_number())
    user_events['listed_count'] = int(fake.random_number())
    # user_events['location'] = fake.city()
    
    pprint(user_events)
    print()
    producer.send('TweeterData', user_events)
    producer.flush()
    time.sleep(random.random() * 4)

