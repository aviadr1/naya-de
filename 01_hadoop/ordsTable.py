# Import the table 'ords' - 3M records) to HDFS
#run in (base)


# sudo -u hdfs sqoop import \
# --connect "jdbc:mysql://localhost:3306/classicmodels" \
# --username naya \
# --password NayaPass1! \
# --driver com.mysql.jdbc.Driver \
# --warehouse-dir "/tmp/sqoop/staging/ordsTable.sql" \
# --fields-terminated-by ',' \
# --table "ords"

