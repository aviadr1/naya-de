
from kafka import KafkaConsumer

# In this example we will illustrate a simple producer-consumer integration

bootstrapServers = "cnt7-naya-cdh63:9092"
topic3 = 'stook'

# First we set the consumer,
consumer = KafkaConsumer(topic3,bootstrap_servers=bootstrapServers)

# print the value of the consumer
# we run the consumer to fetch the message scoming from topic1.
for message in consumer:
    print(str(message.value))