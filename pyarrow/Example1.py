'''
Get the capacity of file system on a human readable format.
'''



import pyarrow as pa

fs = pa.hdfs.HadoopFileSystem(

    host='Cnt7-naya-cdh63', #or internal-ip
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

cap = fs.get_capacity()


print(cap)
print(str(round(cap / pow(1024,2), 0)) + ' MB')




