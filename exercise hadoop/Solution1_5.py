import hadoop_config as c
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

#2. Create/clean the staging folder in HDFS

if c.fs.exists(c.path_staging) :
    c.fs.delete(c.path_staging,recursive=True)
    print(f"{c.path_staging} deleted")
    c.fs.mkdir(c.path_staging)
    print(f"{c.path_staging} created")

#3. Make the list of tables in the database
cursor = c.cnx.cursor()
cursor.execute("Show tables ;")
tables = cursor.fetchall()
tables = [x[0] for x in tables]
print(tables)

#4. Upload the tables to the staging folder as Parquet files
for table in tables:
    path_table = '/tmp/staging/' + table + '.parquet'
    print(f"starting {table}")
    df = pd.read_sql(f"select * from {table};",con=c.cnx)
    df_for_hdfs =pa.Table.from_pandas(df)
    with c.fs.open(path_table, "wb") as fw:
        pq.write_table(df_for_hdfs, fw)
    print(f"finish {table}")

c.cnx.close()

##################################################
#create db
c.client.create_database('audiostore',path='/user/hive/warehouse/audiostore',force=True)
c.client.set_database('audiostore')
c.client.raw_sql('INVALIDATE METADATA', results=False)




for table in tables[2:11]:
    #tell me when you start
    print("starting ",table , end="...")
    #change dir owner
    c.fs.chown(c.stg,group='naya',owner='impala')
    c.fs.chown(c.impala_path, group='naya', owner='impala')
    #create tables in impala- only schema first
    pf = c.client.parquet_file(hdfs_dir = f'{c.stg}', like_file = f'{c.stg}{table}.parquet')
    c.client.create_table(table,
                        schema=pf.schema(),
                        database='audiostore',
                        external=True,
                        force=True,
                        format='parquet',
                        location=f'{c.impala_path}{table}.parquet',
                        like_parquet=None)

    c.client.raw_sql('INVALIDATE METADATA', results=False)
    #5. Load the data into Impala
    c.client.load_data(table_name =table,
                     path = f'{c.stg}{table}.parquet',
                     database='audiostore',
                     overwrite=True, partition=None
                    )
    print("finish")


# Testing=======================================================================

query = '''
SELECT * FROM customer LIMIT 10
'''
result = c.client.sql(query).execute(limit=None)
print(result)