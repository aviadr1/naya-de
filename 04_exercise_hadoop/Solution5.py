import hadoop_config as c
import Solution3 as s

c.client.create_database('audiostore',path='/user/hive/warehouse/audiostore',force=True)
c.client.set_database('audiostore')
c.client.raw_sql('INVALIDATE METADATA', results=False)
#INVALIDATE METADATA Statement
#https://impala.apache.org/docs/build/html/topics/impala_invalidate_metadata.html


stg = '/tmp/staging/'
impala_path = 'hdfs://Cnt7-naya-cdh63:8020/user/hive/warehouse/audiostore/'

for table in s.tables[2:11]:
    #tell me when you start
    print("starting ",table , end="...")
    #change dir owner
    c.fs.chown(stg,group='naya',owner='impala')
    c.fs.chown(impala_path, group='naya', owner='impala')
    #create tables in impala- only schema first
    pf = c.client.parquet_file(hdfs_dir = f'{stg}', like_file = f'{stg}{table}.parquet')
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
                     path = f'{stg}{table}.parquet',
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