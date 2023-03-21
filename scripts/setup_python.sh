#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";

### Adds the envionment variables to bahsrc
$DIR/setup_bashrc.sh
source ~/.bashrc

### setup the latest git
$DIR/uprade_git.sh

sudo yum install netcat

conda update conda
### if you want to install the environment on your own, uncomment the following lines
# conda create -n hadoop37 python=3.7.16 pyarrow pyhive ibis-framework mysql-connector-python pandas sqlalchemy thriftpy2 pyspark
# conda activate hadoop37
# conda install -c conda-forge matplotlib tweepy kafka-python
# pip install textblob jupyterlab jupyter notebook
### otherwise you can install from the environment.yml
conda env create -f $DIR/environment.yml
conda activate hadoop37


