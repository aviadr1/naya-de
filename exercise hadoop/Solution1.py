'''
"1. Locate/create the database in MySQL"
'''

import os
# show databases
os.system("mysql -unaya -pNayaPass1!  -e 'show databases;' ")

# # drop database if exists
os.system("sudo mysql -unaya -pNayaPass1!  -e 'DROP DATABASE IF EXISTS audiostore;' ")


# # create database audiostore
os.system("sudo mysql -unaya -pNayaPass1!  -e 'create database audiostore;' ")

# run databases.sql
os.system("sudo mysql -unaya -pNayaPass1!  audiostore<AudioStoreDB.sql ")
