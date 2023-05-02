'''
Repeat the 3rd example, but add a staging step to the HDFS.
On top of the insertion to MySQL, every 10 events a csv file with the last 10
events should be uploaded to /tmp/staging/kafka/ in HDFS.
The file name should reflect the timestamp of the first message in the file.
'''
from kafka import KafkaConsumer
import json
from datetime import datetime
import re
import pyarrow as pa
import mysql.connector as mc
import time

hdfs_event_counter = 0
hdfs_stg_dir = '/tmp/staging/kafka/'
hdfs_flush_iteration = 10
topic3 = 'kafka-tst-03'
brokers = ['cnt7-naya-cdh63:9092']

# MySQL
host = 'localhost'
mysql_port = 3306
mysql_database_name = 'srcdb'
mysql_table_name = 'src_events'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'

# connector to hdfs
fs = pa.hdfs.connect(
    host='cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    # driver='libhdfs',
    extra_conf=None)


# Set the consumer
consumer = KafkaConsumer(
    topic3,
    client_id='write to MySQL + HDFS consumer',
    group_id='File_MySQL_HDFS',
    bootstrap_servers=brokers,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

insert_statement = """
    INSERT INTO srcdb.kafka_pipeline(event_id, hostname, event_ts) 
    VALUES ('{}', '{}', '{}');"""

# connector to mysql
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,  # <--
    database=mysql_database_name)

def mysql_event_insert(mysql_conn, event_id, hostname, event_ts):
    mysql_cursor = mysql_conn.cursor()
    sql = insert_statement.format(event_id, hostname, event_ts)
    #     print(sql)
    mysql_cursor.execute(sql)
    mysql_cursor.close()


# if not exist staging change it
if fs.exists(hdfs_stg_dir):
    fs.rm(hdfs_stg_dir, recursive=True)

fs.mkdir(hdfs_stg_dir, parents=True)

for message in consumer:
    # Write to MySQL
    events = json.loads(message.value)
    for event in events:
        event_id, event_host, event_ts = event.split('|')
        event_ts = datetime.strptime(event_ts.strip(), '%a %b %d %H:%M:%S %Y')
        # mysql_event_insert(mysql_conn, event_id, event_host, event_ts)


##########################################################################################
    # HDFS Section
    event_id = re.sub(" ", "", event_id)
    event_ts = str(event_ts).replace(" ", "_").replace(":", "").replace("-", "_")

    # first event --> create file
    if hdfs_event_counter == 0:
        hdfs_file_name = event_ts + '.csv'
        print('opening local file:', hdfs_file_name)
        f = open(hdfs_file_name, 'w')

    # event handling --> write to file
    if 0 <= hdfs_event_counter <= hdfs_flush_iteration:
        f.write(','.join([event_id, event_host, event_ts]) + '\n')
        f.flush()
        hdfs_event_counter += 1
        print('.', end='', flush=True)

    # last event --> upload to HDFS
    if hdfs_event_counter == hdfs_flush_iteration:
        hdfs_event_counter = 0
        print()
        print('10 iterations passed. Putting file in HDFS:', hdfs_stg_dir + hdfs_file_name)
        with open(hdfs_file_name,'rb') as f:
            fs.upload('hdfs://cnt7-naya-cdh63:8020/{}'.format(hdfs_stg_dir + hdfs_file_name), f)
        
