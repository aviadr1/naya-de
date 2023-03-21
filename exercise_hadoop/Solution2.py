#2. Create/clean the staging folder in HDFS
# We are going to use the temporary folder /tmp/staging/exercise1 for storing the tables before we upload them into
# HDFS.
# Make sure this folder exists (if not - create it) and that it is empty.
'''
- Use pyarrow to create a HaddopFileSystem instance (usually called fs) to control HDFS functionalities.
- Use simple Python scripting with the exists(), mkdir() and delete() methods.
'''
import pyarrow as pa

fs = pa.hdfs.connect(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

path_staging = '/tmp/staging'
if fs.exists(path_staging) :
    print(f"{path_staging} exists")
    fs.delete(path_staging,recursive=True)
    print(f"{path_staging} deleted")
fs.mkdir(path_staging)
print(f"{path_staging} created")
