'''
upload files created locally to the HDFS staging area.
download data from HDFS to our local system
'''
import os
import pyarrow as pa

fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

#Note: will delete only if exist /tmp/sqoop/staging
if fs.exists('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging'):
    fs.rm('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging', recursive=True)


#First we use mkdir() to create a staging area in HDFS under /tmp/sqoop/staging.
fs.mkdir('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging', create_parents=True)

#Next, we use upload() to copy all *.sql files created locally to the staging area.

local_path = '/home/naya/tutorial/sqoop/scripts/'
extension = '.sql'
sql_files = [f for f in os.listdir(local_path) if f.endswith(extension)]
print(sql_files)

#,'rb'
for f_name in sql_files:
    with open(local_path + f_name,'rb') as f:
        dst = f'hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/{f_name}'
        print('uploading', f_name, 'to', dst)
        fs.upload(dst, f)

# Finally, using ls() we verify the content of the folder
sql_dir_files = fs.ls('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging', detail=False)
print(sql_dir_files)

#We can also download data from HDFS to our local system using the download() method.
print("Let's transfer files from hdfs to local OS:")
fs.download(path='hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/classicmodels.sql',
            stream='/home/naya/tutorial/sqoop/classicmodels.sql',
            buffer_size=None)

# Now we can check if the file was transfer to a local directory
local_files = [f for f in os.listdir('/home/naya/tutorial/sqoop') if f.endswith(extension)]
for f_name in local_files:
    print(f_name)
