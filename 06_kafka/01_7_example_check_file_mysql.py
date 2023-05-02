'''
In this example we repeat the last example,
but this time the messages are ingested and written to a table in MySQL.
'''
from datetime import datetime
from kafka import KafkaConsumer
import json
import mysql.connector as mc
import pandas as pd

# MySQL
host = 'localhost'
mysql_port = 3306
mysql_database_name = 'classicmodels'  #'srcdb'
mysql_table_name = 'orders'           #'src_events'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'


# connector to mysql
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,  # <--
    database=mysql_database_name)


read_with_pandas = True
query = "select * from srcdb.kafka_pipeline"

# create a cursor
# mysql_cursor = mysql_conn.cursor()

# print the data
if read_with_pandas:
    df = pd.read_sql(query, con=mysql_conn)
    print(df.head())
    print('...')
    print(df.tail())
else:
    # create a cursor
    mysql_cursor = mysql_conn.cursor()
    try :
        mysql_cursor.execute(query)
        events = mysql_cursor.fetchall()
        print(events)    
    finally:
        mysql_cursor.close()