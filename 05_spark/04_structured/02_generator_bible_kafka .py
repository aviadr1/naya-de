'''
Create bible generator
'''

from time import sleep
from datetime import datetime
from random import random
from kafka import KafkaProducer
from pathlib import Path
import json
import re
import os

producer = KafkaProducer(bootstrap_servers='Cnt7-naya-cdh63:9092',
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
base_dir = Path(__file__).parent
dir_name =  base_dir / 'output/my_stream_directory'
dir_name.mkdir(parents=True, exist_ok=True)

# Read from csv file

def verse_generator(f_name, limit=5):
    '''
    This generator iterates the (arbitrary) lines of the file Bible.txt
    and yields its verses, as determined by lines beginning with a regex of the
    form [chapter: verse]
    '''
    if limit is None:
        limit = 10**6
    with open(f_name) as f:
        verse=''
        for i, line in enumerate(f):            # Skip empty lines
            if line=='\n':
                continue
            if limit is not None:               # Keep output limit
                if i>limit:
                    break
            if re.findall('^\d+:\d+', line):    # If beginning of a verse
                sleep(random()/10)
                yield verse                     # Yield previous verse
                verse = line[:-1]               # Start a new verse (ignore '\n')
            else:
                if verse:                       # If continuation of a verse
                    verse += line[:-1]
                else:                           # Header / title / comments
                    continue




for verse in verse_generator(base_dir / 'Bible.txt', 200):
    print(verse)
    if verse:
        producer.send('bible', {'chapter': verse.split(":")[0], 'verse': verse.split(":")[1].split(" ")[0], 'text': " ".join(verse.split(" ")[1:])})
    sleep(3)


# Finally, an auxiliary code for removing older files from the directory
# for f_name in os.listdir(dir_name):
#     os.remove(dir_name + '/' + f_name)
# os.listdir(dir_name)