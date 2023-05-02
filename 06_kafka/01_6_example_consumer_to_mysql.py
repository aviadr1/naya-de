'''
In this example we repeat the last example,
but this time the messages are ingested and written to a table in MySQL.
'''
from datetime import datetime
from kafka import KafkaConsumer
import json
import mysql.connector as mc
from time import sleep

# MySQL
host = 'localhost'
mysql_port = 3306
mysql_database_name = 'srcdb'
mysql_table_name = 'src_events'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'

#kafka
topic3 = 'kafka-tst-03'
brokers = ['cnt7-naya-cdh63:9092']

# connector to mysql
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,
    database='srcdb')

# create a cursor
mysql_cursor = mysql_conn.cursor()

# Create table if needed

mysql_create_tbl_events = '''create table if not exists srcdb.kafka_pipeline
    (event_id varchar(10) primary key,
     hostname varchar(30),
     event_ts datetime);'''


# Insert data template
insert_statement = """
    INSERT INTO kafka_pipeline(event_id, hostname, event_ts)
    VALUES ('{}', '{}', '{}');"""


def mysql_event_insert(mysql_conn, event_id, hostname, event_ts):
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(mysql_create_tbl_events)
    sql = insert_statement.format(event_id, hostname, event_ts)
    print(sql)
    mysql_cursor.execute(sql)
    mysql_cursor.close()


# Set the consumer
consumer = KafkaConsumer(
    topic3,
    client_id='consumer to mysql',
    bootstrap_servers=brokers,
    auto_offset_reset='earliest', # auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

for message in consumer:
    events = json.loads(message.value)
    sleep(1)
    print(events)

    for event in events:
        event_id, event_host, event_ts = event.split('|')
        event_ts = datetime.strptime(event_ts.strip(), '%a %b %d %H:%M:%S %Y')
        # print(event_id, event_host, event_ts)
        mysql_event_insert(mysql_conn, event_id, event_host, event_ts)




# # During the work on MySQL database one may wsh to recreate the kafka_pipeline table.
# mysql_cursor.execute('drop table kafka_pipeline')

#https://stackoverflow.com/questions/44927687/not-clear-about-the-meaning-of-auto-offset-reset-and-enable-auto-commit-in-kafka