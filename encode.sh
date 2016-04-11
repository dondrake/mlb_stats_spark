#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

source $SRCDIR/checkVirtualEnv.sh
source $SRCDIR/spark_config.sh

time spark-submit  \
--verbose \
--master local[*] \
--driver-memory 9G \
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.eventLog.enabled=true" \
--conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
--conf "spark.local.dir=$LOCALDIR" \
--py-files $SRCDIR/CreateStatsRDD.py \
$SRCDIR/EncodeCategorical.py 
RETVAL=$?

echo "status: $RETVAL"

exit $RETVAL

