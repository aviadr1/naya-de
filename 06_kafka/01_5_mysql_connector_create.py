#
# to prepare writing kafka messages to mysql, we need to create a table in mysql.
#

import mysql.connector as mc
#===========================connector to mysql====================================#
# MySQL
host = 'localhost'
mysql_port = 3306
mysql_database_name = 'srcdb'
mysql_table_name = 'src_events'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'

truncate = False

# connector to mysql
mysql_conn = mc.connect(user=mysql_username,
                        password=mysql_password,
                        host=host, port=mysql_port,
                        autocommit=True)

mysql_cursor= mysql_conn.cursor()

# create database if needed
mysql_cursor.execute("create database if not exists srcdb;")

if truncate:
   mysql_cursor.execute("TRUNCATE TABLE srcdb.kafka_pipeline")
   print('truncated table!')

# Create table if needed
mysql_create_tbl_events = '''create table if not exists srcdb.kafka_pipeline
    (event_id varchar(10) primary key,
     hostname varchar(30),
     event_ts datetime);'''

mysql_cursor.execute(mysql_create_tbl_events)

mysql_cursor.close()

print('db ready!')