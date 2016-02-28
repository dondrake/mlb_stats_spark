#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

source $SRCDIR/checkVirtualEnv.sh
source $SRCDIR/spark_config.sh

time spark-submit  \
--verbose \
--master local[*] \
--driver-memory 9G \
--packages com.databricks:spark-csv_2.10:1.3.0 \
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.eventLog.enabled=true" \
--conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
--conf "spark.storage.memoryFraction=0.9" \
--conf "spark.local.dir=$LOCALDIR" \
--py-files $SRCDIR/Game.py \
$SRCDIR/UpdateWeather.py > $SRCDIR/weather.out 2> $SRCDIR/weather.err
RETVAL=$?

echo "status: $RETVAL"

exit $RETVAL
