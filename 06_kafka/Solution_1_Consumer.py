'''
Use example 1, but for each consumed message, print the following information:
1. message offset,
2. exact date and time of it
3. the text of the message.
'''

from kafka import KafkaConsumer
from datetime import datetime

# In this example we will illustrate a simple producer-consumer integration
topic3 = 'kafka-tst-03'
brokers = ['cnt7-naya-cdh63:9092']

# First we set the consumer, and we use the KafkaConsumer class to create a generator of the messages.
consumer = KafkaConsumer(
    topic3,
    client_id='offset and timestamp consumer',
    bootstrap_servers=brokers
    )

for message in consumer:
    dt_object = datetime.fromtimestamp(message.timestamp/1000)
    print("the value is: " ,str(message.value))
    print("the offset is: ",str(message.offset))
    print("the time is: ",dt_object)

#The offset is a simple integer number that is used by Kafka to maintain the current position of a consumer.
#https://www.learningjournal.guru/courses/kafka/kafka-foundation-training/offset-management/