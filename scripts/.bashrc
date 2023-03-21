# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# Settings for cnt7-naya-cdh63 
#export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/conf/yarn-conf
export ZOOKEEPER_HOME=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/zookeeper
export KAFKA_HOME=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/kafka
export KAFKA_CONF=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/etc/kafka/conf.dist
export KAFKA_JARS=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars
export KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/etc/kafka/conf.dist/connect-log4j.properties
#export SPARK_HOME=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/

export LD_LIBRARY_PATH=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/lib/native
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera
export ARROW_LIBHDFS_DIR=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib64


# AVIAD (NC-DE-04-HiveImpala-T01-Docker-V3.1.txt)
export HIVE_HOME=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/bin/
alias beeline="$HIVE_HOME/beeline -u jdbc:hive2://localhost:10000/ -n hdfs"
### -- Fix impala-shell Python version to Python 2.7 (not supported by Python 3.x)
alias impala-shell="env -i impala-shell"


 #>>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/naya/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/naya/miniconda3/etc/profile.d/conda.sh" ]; then
        . "/home/naya/miniconda3/etc/profile.d/conda.sh"
    else
        export PATH="/home/naya/miniconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
