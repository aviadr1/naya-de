from kafka import KafkaProducer

from time import sleep
import time

# Topics/Brokers
topic1 = 'kafka-tst-01'
brokers = ['cnt7-naya-cdh63:9092']


producer = KafkaProducer(bootstrap_servers=brokers)

# The send() method creates the topic
producer.send(topic1, value=b'Hello, World!!!!')
producer.flush()

# # One more example
producer.send(topic1, value=b'This is a Kafka-Python basic tutorial')
producer.flush()


producer.send(topic1, value=f'Aviad was here {time.time()}'.encode('utf-8'))
producer.flush()