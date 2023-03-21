'''

In this exercise we will create a CSV file with specific data from the customers table
by following these steps:

1. Write an SQL query for customers whose number is between 124 and 131 (or any reasonable alternative)
2. Fetch the data from the table into a list of records.
3. Instantiate a DataFrame with the data and use the to_csv() method to creat the file.
'''
import pandas as pd
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
###################     1     ####################

sql = '''
SELECT customernumber, customername, phone 
FROM customers 
WHERE customernumber BETWEEN 124 AND 131 
ORDER BY 1 DESC  
LIMIT 5
'''

print("cursor creating")
cursor = hive_cnx.cursor()
print("whait for me , im doing havy mapreduce")
cursor.execute(sql)
print("taking all the data from hive to local")
data = cursor.fetchall()

cursor.close()

csv_file = '/home/naya/tutorial/sqoop/scripts/customers_sample.csv'
df = pd.DataFrame(data,
                  columns=['customernumber', 'customername', 'phone'])
df.to_csv(csv_file)

print(pd.read_csv(csv_file).head())