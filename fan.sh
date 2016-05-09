#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

source $SRCDIR/checkVirtualEnv.sh
source $SRCDIR/spark_config.sh

NOW=$(date +"%Y%m%d%H%M%S")

time spark-submit  \
--verbose \
--master local[*] \
--driver-memory 13G \
--packages com.databricks:spark-csv_2.10:1.4.0 \
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.eventLog.enabled=true" \
--conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
--conf "spark.local.dir=$LOCALDIR" \
$SRCDIR/FanDuelGame.py $* 2> $SRCDIR/fanduel.err
RETVAL=$?

echo "status $RETVAL"
exit $RETVAL
