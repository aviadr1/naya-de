'''
Use the delete method to delete some of the files from the staging directory.
'''

import os
import pyarrow as pa

fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)
# Del if exist /tmp/sqoop/staging.
fs.rm('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging', recursive=True)


# Finally, using ls() we verify the content of the folder
sql_dir_files = fs.ls('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop', detail=False)
print(sql_dir_files)