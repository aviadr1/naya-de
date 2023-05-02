'''
In this example we will monitor a log file and write its content the messages to a
different target file. The pipeline is as follows:
1. An external generator (implemented by the log_generator notebook) will write logs to a log file.
2. A Kafka producer will monitor the log file and send to Kafka every new data.
3. A kafka consumer will consume the messages and write them to a destination file.
'''
#
# this file reads a log file and sends it to a kafka topic.
# the log file is generated by the previous snippet.
#

from kafka import KafkaProducer

from time import sleep
import json
# Topics/Brokers
topic3 = 'kafka-tst-03'
brokers = ['cnt7-naya-cdh63:9092']
source_file = '/home/naya/tutorial/kafka/srcFiles/srcFile.log'

# First we set the producer.
producer = KafkaProducer(bootstrap_servers = brokers)


# Send the data
with open(source_file, 'r') as f:
    while True:
        lines = f.readlines()  # returns list of strings
        if not lines:
            sleep(1)
            f.seek(f.tell())
        else:
            print(lines)
            producer.send(topic=topic3, value=json.dumps(lines).encode('utf-8'))
            producer.flush()
            sleep(3)


# f.readlines()=
# Return all lines in the file, as a list where each line is an item in the list object

#seek() function is used to change the position of the File Handle to a given specific position.
# File handle is like a cursor, which defines from where the data has to be read or written in the file