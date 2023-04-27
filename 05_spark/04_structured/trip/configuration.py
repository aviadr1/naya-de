import pyarrow as pa

# ========================================================================================================= #
# ================================= /hive/ HDFS =============================================== #
host='Cnt7-naya-cdh63'
port=8020
user='hdfs'


hdfs_host = 'Cnt7-naya-cdh63'
hdfs_owner ='naya'
hdfs_group='supergroup'

hdfs_root = f"hdfs://{hdfs_host}:{port}/user/aviad/de_proj/"
source_path = hdfs_root + 'traffic_parquet'

fs = pa.hdfs.HadoopFileSystem(
    host=host,
    port=port,
    user=user,
    kerb_ticket=None,
    extra_conf=None)

fs.mkdir(hdfs_root, create_parents=True)
fs.chown(path=hdfs_root, owner=hdfs_owner, group=hdfs_group)
# fs.chmod(path=hdfs_root, permissions='g+wx')


# ========================================================================================================= #
# ========================================Kafka Connections =============================================== #
# ========================================================================================================= #
bootstrapServers = "cnt7-naya-cdh63:9092"
topic1 = 'From_API_To_Kafka'
topic2 = 'From_Kafka_To_Hdfs_Archive_Json'
topic3 = 'From_Kafka_To_Spark_MYSQL'
topic4 = 'From_Kafka_To_Hdfs_Parquet'
# ========================================================================================================= #
# ========================================chicago taxi api =============================================== #
# ========================================================================================================= #
green_taxi_events_api="https://data.cityofnewyork.us/resource/gi8d-wdg5.json"



# ======== Format DataFrame to json file and Write it to HDFS  ==================== #
# kafka_to_hdfs_json_path = 'hdfs://Cnt7-naya-cdh63:8020/user/alin/de_proj/hdfsarchive/'
# kafka_to_hdfs_json_checkpoint_path = 'hdfs://Cnt7-naya-cdh63:8020/user/alin/de_proj/hdfsarchive.checkpoint/'
kafka_to_hdfs_json_path = hdfs_root + 'hdfsarchive/'
kafka_to_hdfs_json_checkpoint_path = hdfs_root + 'hdfsarchive.checkpoint/'


# ======== Format DataFrame to parquet file and Write it to HDFS  ==================== #
From_Kafka_To_Hdfs_Parquet_path = hdfs_root + "traffic_parquet/"
From_Kafka_To_Hdfs_parquet_path_checkpointLocation = hdfs_root + "traffic_parque.checkpoint/"
# ========================================================================================================= #
# =================================sql stocks connection =============================================== #
# ========================================================================================================= #
mysql_host = 'localhost'
mysql_port = 3306
mysql_database_name = 'MyTaxisdb'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'
mysql_table_name = 'TAXIs'


# ====== Settings to HUE Connection ===================== #
hue_port = 8889
hue_username = 'hdfs'
hue_password = 'naya'
# ====== Settings to Hive Connection ===================== #
hdfs_host = 'Cnt7-naya-cdh63'
hdfs_port = 9870
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_mode = 'CUSTOM'
hive_database= 'taxi'
# ====== Settings to impala Connection ===================== #
impala_host = 'Cnt7-naya-cdh63'
impala_port = 21050
impala_database = 'taxi_hive_db'
impala_username = 'hdfs'
impala_password = 'naya'



