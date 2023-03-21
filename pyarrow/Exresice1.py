'''
Get the amount of used space on your file system.
'''
import pyarrow as pa

fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

used_space = fs.disk_usage('hdfs://Cnt7-naya-cdh63:8020/')
print(str(round(used_space / pow(1024,2), 0)) + ' MB')
print(used_space)

# 2. Get reported free space of the file system in bytes and modify this output into a human readable format.
path='hdfs://Cnt7-naya-cdh63:8020/'
cap=fs.get_capacity()
disk_usage=fs.disk_usage(path)
free_space=cap-disk_usage

print(str(round(free_space / pow(1024, 2), 0)) + ' MB')
print(free_space )