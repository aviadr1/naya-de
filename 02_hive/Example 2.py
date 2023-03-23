'''
1. Show all databases in hive
    2. Show all tables in classicmodels database
3. Get 10 records from costumers table
4. Get customernumber, customername, phone from customers table
'''


from pyhive import hive

hdfs_host = 'Cnt7-naya-cdh63'

hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_database = 'classicmodels'
hive_mode = 'CUSTOM'

hive_cnx = hive.Connection(
    host=hdfs_host,
    port=hive_port,
    username=hive_username,
    password=hive_password,
    database=hive_database, # we connect to classicmodels
    auth=hive_mode)


cursor = hive_cnx.cursor()
cursor.execute('SHOW TABLES')
print(cursor.fetchall())
cursor.close()

import pandas as pd
df = pd.read_sql(
    'SELECT * FROM customers', hive_cnx, 
    # chunksize=10000
    )
print(df.head().iloc[:, :3]) # 3 columns 5 rows

