import configuration as c
import mysql.connector as mc

#########################################################
####  Note: FIRST! create database MyTaxisdb in MYSQL ###
#########################################################

# create a MySQL DB
mysql_conn = mc.connect(
    user=c.mysql_username,
    password=c.mysql_password,
    host=c.mysql_host,
    port=c.mysql_port,
    autocommit=True
    )

mysql_cursor = mysql_conn.cursor() 
mysql_cursor.execute("CREATE DATABASE IF NOT EXISTS MyTaxisdb")
mysql_cursor.close()
mysql_conn.close()


# ===========================connector to mysql====================================#
mysql_conn = mc.connect(
    user=c.mysql_username,
    password=c.mysql_password,
    host=c.mysql_host,
    port=c.mysql_port,
    autocommit=True,
    database=c.mysql_database_name,
)

# =======================# Creating the events table==============================#
mysql_cursor_taxi = mysql_conn.cursor()
mysql_create_tbl_events = """create table if not exists TAXIs_route (
                            vendorid varchar (4) ,
                            pickup_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                            dropoff_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                            store_and_fwd_flag varchar (4) ,
                            ratecodeid varchar (4) ,
                            passenger_count varchar (4) ,
                            trip_distance double ,
                            PaymentType varchar (4) 
                             )"""

mysql_cursor_taxi.execute(mysql_create_tbl_events)
