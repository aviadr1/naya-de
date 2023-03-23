'''
create a new database and upload data into it from a local CSV file.
'''

from pyhive import hive
import ibis.impala.ddl
from pathlib import Path


# We start by introducing the relevant parameters.
impala_host = 'Cnt7-naya-cdh63'
impala_port = 21050
impala_database = 'classicmodels'
impala_username = 'hdfs'
impala_password = 'naya'

# HDFS details
hdfs_host = 'Cnt7-naya-cdh63'
hdfs_port = 9870

# hive
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_database = 'classicmodels'
hive_mode = 'CUSTOM'

# Next, we create a Connection instance to our file system (HDFS in this example).
hdfs = ibis.hdfs_connect(
    host=hdfs_host,
    port=hdfs_port,
    protocol='webhdfs',
    use_https='default',
    auth_mechanism='NOSASL',
    verify=True)

# Finally, we connect our Impala client.
client = ibis.impala.connect(
    host=impala_host,
    port=impala_port,
    database=impala_database,
    user=impala_username,
    password=impala_password,
    pool_size=8,
    hdfs_client=hdfs)

# Create Hive connection
hive_cnx = hive.Connection(
    host=hdfs_host,
    port=hive_port,
    username=hive_username,
    password=hive_password,
    database=hive_database,
    auth=hive_mode)

# Creating the database with impala
client.create_database('blackfriday',
                       path='/user/hive/warehouse/blackfriday',
                       force=True)


client.set_database('blackfriday')

# Creating the table on hive
print("list table before created",client.list_tables(like=None, database='blackfriday'))

cursor = hive_cnx.cursor()
if 'bf_sales' not in client.list_tables(database='blackfriday'):
    cursor.execute('CREATE TABLE blackfriday.bf_sales ( \
        User_ID INT, \
        Product_ID STRING, \
        Gender STRING, \
        Age STRING, \
        Occupation TINYINT, \
        City_Category STRING, \
        Stay_In_Current_City_Years INT, \
        Marital_Status TINYINT, \
        Product_Category_1 INT, \
        Product_Category_2 INT, \
        Product_Category_3 INT, \
        Purchase FLOAT) \
      ROW FORMAT DELIMITED \
      FIELDS TERMINATED BY \',\' \
      LOCATION \'/user/hive/warehouse/blackfriday.db\' \
      TBLPROPERTIES("skip.header.line.count"="1")')
cursor.close()

# We use INVALIDATE METADATA to make the table available through Impala.
client.raw_sql('INVALIDATE METADATA', results=False)

print("list table after created", client.list_tables(like=None, database='blackfriday'))


# In order to use the load_data() method of the Ibis client,
# we first need to upload the local file into HDFS. We use
# pyarrow for that.

import pyarrow as pa

fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    driver='libhdfs',
    extra_conf=None)

# please make shure that you have blackfurday.csv in the same directory as this file
cwd = Path(__file__).parent
print('current working directory', cwd)
with open(cwd / 'BlackFriday.csv','rb') as f:
    fs.upload('hdfs://Cnt7-naya-cdh63:8020/tmp/BlackFriday.csv', f, buffer_size=None)

# or with os

# import os
# os.system('hdfs dfs -put BlackFriday.csv /tmp/BlackFriday.csv')

# Loading the data into the table
client.load_data(table_name='bf_sales',
                 path='/tmp/BlackFriday.csv',
                 database='blackfriday',
                 overwrite=True,
                 partition=None)


# Testing

query = '''
SELECT User_ID, SUM(Purchase) AS Sum_Purchase
FROM blackfriday.bf_sales
GROUP BY User_ID
ORDER BY Sum_Purchase
LIMIT 5
'''
result = client.sql(query).execute(limit=None)
print(result)