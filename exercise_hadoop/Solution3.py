"""
#3. Make the list of tables in the database
"""
import hadoop_config as c
import os
# show databases
# print(os.system("mysql -unaya -pNayaPass1!  -e 'use audiostore;show tables ;' "))

# cnx = mc.connect(
#     user="naya",
#     password="NayaPass1!",
#     host="localhost",
#     port=3306,
#     autocommit=True,
#     database="audiostore")
#
cursor= c.cnx.cursor()
cursor.execute('show tables;')
tables1=cursor.fetchall()
tables=[t[0] for t in tables1]
cursor.close()

#print(tables1)
print(tables)

