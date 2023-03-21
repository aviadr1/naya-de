'''
Managing permissions with chmod() & chown()
In this example we manage permissions for our files
'''

import os
import pyarrow as pa

fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

# Change access permissions for the file classicmodels.sql in the staging directory.
fs.chmod('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/classicmodels.sql', 775)

# Change access permissions for a file in the staging directory in order
# to allow for naya user to get this file only
fs.chown('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/classicmodels.sql', owner='naya', group='naya')

# Finally, we can chaeck the details of the files either by the info() method.
a=fs.info('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/classicmodels.sql')
print(a)

# Alternatively we can do it using the detail=True argument of the ls() method.
file_details = fs.ls('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/classicmodels.sql', detail=True)
for detail in file_details:
    for k, v in detail.items():
        print(k, ':', v)
