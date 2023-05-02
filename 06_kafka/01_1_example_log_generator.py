'''
In this example we will monitor a log file and write its content the messages to a
different target file. The pipeline is as follows:
1. An external generator (implemented by the log_generator notebook) will write logs to a log file.
2. A Kafka producer will monitor the log file and send to Kafka every new data.
3. A kafka consumer will consume the messages and write them to a destination file.
'''

#
# this file constantly writes to a log file, 
# which is then read by the producer in the next snippet.
#

from random import randint, random
from os import getcwd, remove
from time import time, sleep
from datetime import datetime

# This generator yields 3-tuples of the form (counter, hostname, ts).
def event_generator():
    counter = 1
    while True:
        hostname = f'host{randint(1, 5)}'
        ts = datetime.now().strftime('%a %b %d %H:%M:%S %Y')
        print(counter, hostname, ts)
        yield counter, hostname, ts
        counter += 1



log_path = '/home/naya/tutorial/kafka/srcFiles/srcFile.log'
with open(log_path, 'w') as f:
    for counter, hostname, ts in event_generator():
        f.write(f'Event #{counter}|{hostname}|{ts}\n')
        sleep(5*random())
        f.flush()




# Optionally, one would like to clear the log file once in a while...
# remove(log_path)