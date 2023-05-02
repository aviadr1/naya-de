'''
In this example we will monitor a log file and write its content the messages to a
different target file. The pipeline is as follows:
1. An external generator (implemented by the log_generator notebook) will write logs to a log file.
2. A Kafka producer will monitor the log file and send to Kafka every new data.
3. A kafka consumer will consume the messages and write them to a destination file.
'''

#
# this is the consumer part
#

from kafka import KafkaConsumer

# In this example we will illustrate a simple producer-consumer integration

topic3 = 'kafka-tst-03'
brokers = ['cnt7-naya-cdh63:9092']
target_file = '/home/naya/tutorial/kafka/sinkFiles/tarFile.log'

# First we set the consumer, and we use the KafkaConsumer class to create a generator of the messages.
# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
consumer = KafkaConsumer(
    topic3,
    client_id = "consumer to file",
    bootstrap_servers=brokers,
    auto_offset_reset='earliest', #
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

# Write the data to target file
with open(target_file, 'w') as f:
    for message in consumer:
        print(message.value)
        f.write(format(message.value))
        f.flush()

#configuration parameter to decide where to start consuming messages from.
#earliest - start consuming from the point where it stopped consuming before
#latest - starts consuming from the latest offsets in the assigned partitions.


#auto.commit.interval.ms
# The property auto.commit.interval.ms
# specifies the frequency in milliseconds that the consumer offsets are auto-committed to Kafka.
# This only applies if enable.auto.commit is set to true.
# Kafka consumer will auto commit the offset of the last message received in response to its poll() call.