'''
    1. Show all databases in hive
2. Show all tables in classicmodels database
3. Get 10 records from costumers table
4. Get customernumber, customername, phone from customers table
'''

from pyhive import hive

# We start by introducing the relevant parameters.
# HDFS details
hdfs_host = 'Cnt7-naya-cdh63'

# HIVE details
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_database = 'classicmodels'
hive_mode = 'CUSTOM'

# Next,  Create Hive connection.
hive_cnx = hive.Connection(
    host=hdfs_host,
    port=hive_port,
    username=hive_username,
    password=hive_password,
    auth=hive_mode
)


cursor = hive_cnx.cursor()
cursor.execute('SHOW DATABASES')
print(cursor.fetchall())
cursor.close()