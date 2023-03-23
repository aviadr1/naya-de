'''
In this exercise we will upload the audiostore database from MySQL to Impala. We will do it in the following steps:

1. Locate/create the database in MySQL
2. Create/clean the staging folder in HDFS
3. Make the list of tables in the database
4. Upload the tables to the staging folder as Parquet files
5. Load the data into Impala
6. Query your data

'''
############################   1   #########################
# Make sure you have the audiostore database in MySQL
import os

'''
- This can run from the shell, or alternatively run it from Python with os.system().
- First you have to create a database, then run the audiostoreDB.sql script.
'''


############################  2   #########################

# We are going to use the temporary folder /tmp/staging/ for storing the tables before we upload them into
# HDFS. Make sure this folder exists (if not - create it) and that it is empty.

'''
- Use pyarrow to create a HaddopFileSystem instance (usually called fs) to control HDFS functionalities.
- Use simple Python scripting with the exists(), mkdir() and delete() methods.
'''
import pyarrow as pa

# ###########################  3   #########################
# In the next step we would like to iterate the tables in
# order to upload them as Parquet files, so in this step you should get the list of tables in the database.
'''
- Use any RDBMS API to connect to the database. SQLAlchemy is a good option, but consider using a MySQL connector (like the official mysql-connector-python, which is already installed (explained better at W3Schools).
- Execute an SQL query to get all the names of the tables in the database, and then use the fetchall() method to turn the result into a list.
'''

import mysql.connector as mc

# ###########################  4   #########################
# Import all the tables from the audiostore database into the staging folder using the Parquet format.

'''
- There is going to be a for-loop on the list of tables. Each table will be converted to a Parquet file, which will be uploaded to our HDFS staging area.
- I couldn't find a direct option to make this transition, so I've added an intermediate staging step through pandas.DataFrame. Since pandas is so popular, it is easier to first read the tables as pandas.DataFrame (using read_sql()) and then write them as Parquet (using write_table()).
- Parquet does not support complex datetime types, so you might have to manipulate your data so that Parquet will "eat it".
- Parquet files have no standard encoding, so you should consider them as a stream of binary files.
'''

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


# ###########################  5   #########################
# In order for Impala to query these tables, one has to
# define them as a database. Create a new database called my_audiostore at /user/hive/warehouse/ and load the data
# from teh staging folder into it.

import ibis
'''
- We should start by defining an Impala client
- The create_database() method adds .db to the name of the database.
- For each table we would like to do the following:
        - Create the table in Impala (how do we get the schema???)
        - Load the data into Impala
- You might have permission issues. Solve them...'''


# ###########################  6   #########################
# Finally - make a query on the newly created tables - which is the most profitable album?