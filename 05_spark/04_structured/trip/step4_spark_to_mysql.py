from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as f
from pyspark.sql import types as t
import configuration as c
import mysql.connector as mc

# ================== integrate wth kafka======================================================#
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell"

# ================== connection between  spark and kafka=======================================#
# ==============================================================================================
spark = SparkSession.builder.appName("From_Kafka_To_mysql").getOrCreate()


# ==============================================================================================
# =========================================== ReadStream from kafka===========================#
socketDF =   (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", c.bootstrapServers)
    .option("Subscribe", c.topic3)
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
)
# ==============================================================================================
# ==============================Create schema for create df from json=========================#
schema = (
    t.StructType()
    .add("vendorid", t.StringType())
    .add("lpep_pickup_datetime", t.StringType())
    .add("lpep_dropoff_datetime", t.StringType())
    .add("store_and_fwd_flag", t.StringType())
    .add("ratecodeid", t.StringType())
    .add("pickup_longitude", t.StringType())
    .add("pickup_latitude", t.StringType())
    .add("dropoff_longitude", t.StringType())
    .add("dropoff_latitude", t.StringType())
    .add("passenger_count", t.StringType())
    .add("trip_distance", t.StringType())
    .add("fare_amount", t.StringType())
    .add("extra", t.StringType())
    .add("mta_tax", t.StringType())
    .add("tip_amount", t.StringType())
    .add("tolls_amount", t.StringType())
    .add("improvement_surcharge", t.StringType())
    .add("total_amount", t.StringType())
    .add("payment_type", t.StringType())
    .add("trip_type", t.StringType())
)

# ==============================================================================================
# ==========================change json to dataframe with schema==============================#
taxiTripsDF = (
    socketDF.select(f.col("value").cast("string"))
    .select(f.from_json(f.col("value"), schema).alias("value"))
    .select("value.*")
)

# ====# 1: Remove spaces from column names====================================================#
taxiTripsDF = (
    taxiTripsDF.withColumnRenamed("vendorid", "vendorid")
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("passenger_count", "passenger_count")
    .withColumnRenamed("trip_distance", "trip_distance")
    .withColumnRenamed("ratecodeid", "ratecodeid")
    .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")
    .withColumnRenamed("payment_type", "PaymentType")
)

# ============================================================================================#
# =================================MYSQL============================================#
# ============================================================================================##

taxiTripsDF.printSchema()
# connector to mysql


def procss_row(events):
    # connector to mysql
    mysql_conn = mc.connect(
        user="naya",
        password="NayaPass1!",
        host="localhost",
        port=3306,
        autocommit=True,  # <--
        database="MyTaxisdb",
    )

    insert_statement = """
    INSERT INTO MyTaxisdb.TAXIs_route(
        vendorid,	pickup_datetime,	dropoff_datetime,   store_and_fwd_flag,	
        ratecodeid, passenger_count,    trip_distance,	    PaymentType
)
        VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'); """

    mysql_cursor = mysql_conn.cursor()
    sql = insert_statement.format(
        events["vendorid"],
        events["pickup_datetime"],
        events["dropoff_datetime"],
        events["store_and_fwd_flag"],
        events["ratecodeid"],
        events["passenger_count"],
        events["trip_distance"],
        events["PaymentType"],
    )
    # print(sql)
    mysql_conn.commit()
    mysql_cursor.execute(sql)
    mysql_cursor.close()
    pass


Insert_To_MYSQL_DB = (
    taxiTripsDF.writeStream.foreach(procss_row).outputMode("append").start()
)
Insert_To_MYSQL_DB.awaitTermination()
