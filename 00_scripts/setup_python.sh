#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";

### Adds the envionment variables to bahsrc
$DIR/setup_bashrc.sh
source ~/.bashrc

### setup the latest git
$DIR/uprade_git.sh

sudo yum install netcat

# change the default conda environment to be based on python 3.6.6 
# which is compatible with the installation of hadoop, spark and kafka 
conda update conda
conda install python=3.6.6 pyhive=0.6.1 thriftpy2 sqlalchemy=1.3.1 ibis-framework=0.14.0 mysql-connector-python pyspark=2.4.3 
conda install -c conda-forge matplotlib
conda install -c conda-forge tweepy
pip install textblob
conda config --append channels conda-forge
conda install kafka-python=1.4.6
conda install mysql-connector-python




