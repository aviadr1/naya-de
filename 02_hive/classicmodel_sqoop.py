
-9.1- Import ALL the classicmodels database tables from MySQL to Hive

Steps:
(1) Create a database named classicmodels in hive (verify on HDFS)
hive> create database classicmodels;


(2) Prepare a variable (array) to hold all classicmodels tables in a OS variable-run in (base)
$ tblList=$(mysql -unaya -pNayaPass1! classicmodels -NB -e 'show tables')
$ echo $tblList

(3) Prepare a variable to hold the SQOOP command to run dynamically with the $tblList variable
$ sqoopCmd='sudo -u hdfs sqoop import
-Dorg.apache.sqoop.splitter.allow_text_splitter=true
--connect "jdbc:mysql://localhost:3306/classicmodels"
--username naya
--password NayaPass1!
--driver com.mysql.jdbc.Driver
--warehouse-dir "/user/hive/warehouse/classicmodels.db"
--hive-import
--create-hive-table
--hive-table classicmodels.$v
--table $v'

$ echo $sqoopCmd

(4) Run the process using for loop and capture all process lines to a log file for each table (stdout + stderr)
$ for v in $tblList; do eval $sqoopCmd 2>&1 | tee ~/tutorial/sqoop/sqoop_$v.log; done