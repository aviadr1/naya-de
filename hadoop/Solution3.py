"""
1. Connect to a local HDFS with parameters
2. Get reported free space of the file system in bytes and modify this output into a human readable format
3. Get disk usage information about /user directory in HDFS in bytes and modify this output into a human readable format
4. Get detailed information about a HDFS directory /user/hive/warehouse in human readable format
5. Create a directory in HDFS: first check if a directory exists and through the message about it,
if a directory don't exist create it. Use a global scope's variable. Check the script with an existing path too.
6. Rename a file in HDFS and check if the file was renamed.
7. Delete the whole directory /tmp/sqoop/staging and it's files and check directory again.
8. Create a new file in any directory on your OS and write any row into it (hint: use input() method). Upload this file
to HDFS directory.

"""

from pathlib import Path
import pyarrow as pa

# 1. Connect to a local HDFS with parameters such: host, port, user, kerberos ticket for authentication if exists,
# driver and other configurations.
fs = pa.hdfs.HadoopFileSystem(
    host="Cnt7-naya-cdh63", port=8020, user="hdfs", kerb_ticket=None, extra_conf=None
)

# 2. Get reported free space of the file system in bytes and modify this output into a human readable format.
path_hdfs = "hdfs://Cnt7-naya-cdh63:8020/"
cap = fs.get_capacity()
disk_usage = fs.disk_usage(path_hdfs)
free_space = cap - disk_usage

print(str(round(free_space / pow(1024, 2), 0)) + " MB")
print(free_space)

# 3. Get disk usage information about /user directory in HDFS in bytes and modify this output
# into a human readable format.
print(
    str(round(fs.disk_usage("hdfs://Cnt7-naya-cdh63:8020/user") / pow(1024, 2), 0))
    + " MB"
)

# 4. Get detailed information about a HDFS directory /user/hive/warehouse in human readable format.
file_details = fs.ls("hdfs://Cnt7-naya-cdh63:8020//user/hive/warehouse", detail=True)
for detail in file_details:
    for k, v in detail.items():
        print(k, ":", v)
    print()
    print()

# 5. Create a directory in HDFS: first check if a directory exists and through the message about it,
# if a directory don't exist create it.
# Use a global scope's variable. Check the script with an existing path too.
path = "/tmp/sqoop/staging"
if fs.exists(path):
    print("The directory {} already exists!".format(path))
else:
    fs.mkdir("hdfs://Cnt7-naya-cdh63:8020{}".format(path), create_parents=True)

# 6. Rename a file in HDFS and check if the file was renamed.
print('files in staging before rename:', fs.ls("hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging", detail=False))
if not fs.exists("/tmp/sqoop/staging/ordsTable.sql"):
    with open("hadoop/ordsTable.py", "rb") as f: 
        fs.upload("/tmp/sqoop/staging/ordsTable.sql", f)

fs.rename(
    "hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/ordsTable.sql",
    "hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/ordsTable_new.sql",
)
print('files in staging after rename:', fs.ls("hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging", detail=False))

# 7. Delete the whole directory /tmp/sqoop/staging and it's files and check directory again.
fs.rm(f"hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging", recursive=True) # hdfs dfs -rm -rf /tmp/sqoop/staging 
tmp_dirs = fs.ls("hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop", detail=False)
for dir in tmp_dirs:
    print(dir)

# 8. Create a new file in any directory on your OS and write any row into it (hint: use input() method).
#    Upload this file to HDFS directory.

Path("/home/naya/tutorial/sqoop/scripts/file.txt").touch()
file_content = input("Insert a row into a file.txt: ")
file = open("/home/naya/tutorial/sqoop/scripts/file.txt", "w+")
file.write(file_content)
file.close()
with open("/home/naya/tutorial/sqoop/scripts/file.txt", "rb") as f:
    fs.upload(
        "hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/development/file.txt",
        f,
        buffer_size=None,
    )
