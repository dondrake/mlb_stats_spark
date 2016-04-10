#!/bin/bash

# EDIT THE VARIABLES BELOW TO MATCH YOUR SETUP
SPARK_HOME=~/spark/spark-1.6.1-bin-hadoop2.6
export PATH=$SPARK_HOME/bin:$PATH

# Used to store the results of the Spark WebUI after
# the jobs complete.  Used in conjunction with the Spark
# History server.
export EVENT_LOG_DIR=file://$HOME/spark-events

if [[ ! -d $EVENT_LOG_DIR ]]; then
    mkdir -p $EVENT_LOG_DIR
fi

LOCALDISK1="/Volumes/Drake Backup Disk/"
LOCALDISK2="/Volumes/Don Drake 312-560-1574"

LOCALDIR=""
if [[ -d $LOCALDISK1 ]]; then
    LOCALDIR=$LOCALDISK1/spark.tmp/mov
elif [[ -d $LOCALDISK2 ]]; then
    LOCALDIR=$LOCALDISK2/spark.tmp/mov
else
    LOCALDIR=/tmp
fi

echo "using $LOCALDIR"
# for spark
export PYSPARK_DRIVER_PYTHON=`which python`
export PYSPARK_PYTHON=`which python`
export PATH=$SPARK_HOME/bin:$PATH
