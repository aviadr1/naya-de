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
    database=hive_database,
    auth=hive_mode)


cursor = hive_cnx.cursor()
cursor.execute('SELECT * FROM customers LIMIT 10')
print(cursor.fetchall())  # fetchall is good for small-data, with big data you need to limit the amount of rows using .fetchmany(10000)
cursor.close()