"""
List all databases in impala
"""
import ibis.impala.ddl


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

# We can explore the database through the client instance. Some examplary methods are listed below.
print(client.list_databases())