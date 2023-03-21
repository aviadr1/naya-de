"""
4. Upload the tables to the staging folder as Parquet files
"""
import Solution3 as s
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


fs = pa.hdfs.connect( host='Cnt7-naya-cdh63', port=8020,user='hdfs', kerb_ticket=None,extra_conf=None)

for table in s.tables:
    path_table = '/tmp/staging/' + table + '.parquet'
    #print(f"starting {table}")
    df = pd.read_sql(f"select * from {table};",con=s.cnx)
    df_for_hdfs =pa.Table.from_pandas(df)
    with fs.open(path_table, "wb") as fw:
        pq.write_table(df_for_hdfs, fw)
    #print(f"finish {table}")

s.cnx.close()
