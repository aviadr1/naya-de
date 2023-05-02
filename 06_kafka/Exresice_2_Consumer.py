'''
for the FIRST example (log/producer/mysql), please add a staging step to the HDFS.
On top of the insertion to MySQL, every 10 events a csv file with the last 10 events
should be uploaded to /tmp/staging/kafka/ in HDFS.
The file name should reflect the timestamp of the first message in the file.
'''