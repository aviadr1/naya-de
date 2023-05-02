from kafka import KafkaConsumer

# In this example we will illustrate a simple producer-consumer integration

topic1 = 'kafka-tst-01'
brokers = ['cnt7-naya-cdh63:9092']

use_past_data = False
# First we set the consumer,
# and we use the KafkaConsumer class to create a generator of the messages.
if use_past_data:
    consumer = KafkaConsumer(
        topic1,
        bootstrap_servers=brokers,
        auto_offset_reset='earliest', #
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
        )
else:
    consumer = KafkaConsumer(
        topic1,
        bootstrap_servers=brokers
    )

# print the value of the consumer
# we run the consumer generator to fetch the message scoming from topic1.
for message in consumer:
    print(str(message.value))