'''
Create bible generator
'''

from time import sleep
from datetime import datetime
from random import random
import re
import os


dir_name = 'my_stream_directory'

def verse_generator(f_name, limit=5):
    '''
    This generator iterates the (arbitrary) lines of the file Bible.txt
    and yields its verses, as determined by lines beginning with a regex of the
    form [chapter: verse]
    '''
    if limit is None:
        limit = 10**6
    with open(r'/tmp/pycharm_project_981/05_spark/spark_streaming/Bible.txt') as f: # open book
        verse=''
        for i, line in enumerate(f):            # The enumerate() function adds a counter to an iterable
            if line=='\n':                      # if empty lines
                continue                        # Skip the empty lines
            if limit is not None:               # if we set a limit
                if i>limit:                     #and the index is bigger
                    break                       # then stop the generator
            if re.findall('^\d+:\d+', line):    # If beginning of a verse regex d(digit):d(digit)
                sleep(random()/10)                      #sleep(random()/10)
                yield verse                     # Yield previous verse mean : give me the data from the last point you writed
                verse = line[:-1]               # Start a new verse (ignore '\n')
            else:
                if verse:                       # If continuation of a verse
                    verse += line[:-1]
                else:                           # Header / title / comments
                    continue

if not os.path.exists(dir_name):
    os.mkdir(dir_name)

for verse in verse_generator(r'/tmp/pycharm_project_981/05_spark/spark_streaming/Bible.txt', 2000):
    ts = str(datetime.now()).replace(' ', '_').replace(':', '_')  # legal file name
    f_name = 'bible_' + ts
    f_path = dir_name + '/' + f_name
    print(verse)
    with open(f_path, 'w') as f:
        f.write(verse)
    sleep(random())


# # Finally, an auxiliary code for removing older files from the directory
# for f_name in os.listdir(dir_name):
#     os.remove(dir_name + '/' + f_name)
# os.listdir(dir_name)

