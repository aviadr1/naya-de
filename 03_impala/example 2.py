
import ibis
import ibis.impala
import ibis.impala.client
import ibis.impala.api
import ibis.impala.ddl
from ibis.impala.client import HS2Error

# We start by introducing the relevant parameters.
impala_host = 'Cnt7-naya-cdh63'
impala_port = 21050
impala_database = 'classicmodels'
impala_username = 'hdfs'
impala_password = 'naya'

# HDFS details
hdfs_host = 'Cnt7-naya-cdh63'
hdfs_port = 9870

# Next, we create a Connection instance to our file system (HDFS in this example).
hdfs = ibis.hdfs_connect(
    host=hdfs_host,
    port=hdfs_port,
    use_https='default',
    protocol='webhdfs',
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

# We can also set the database to a specific one.
db = client.set_database('classicmodels')

# Print list tables
print(client.list_tables(like='p*'))

# Print schema for employees table
print(client.get_schema('employees'))

# Print all Tables in schema
print(client.list_tables())

# get table and print the schema
employees = client.get_schema("employees")
print(employees)

# Print the metadata
employees = client.describe_formatted("employees")
print(employees)

#print if have partitioned
try:
    employees = client.list_partitions("employees")
    print(employees)
except HS2Error:
    print('employees is not partitioned')

# Print statistics of table
employees = client.compute_stats("employees")
print(employees)

# Print the columns statistics
employees = client.column_stats("employees")
print(employees)


# Close the client
client.close()