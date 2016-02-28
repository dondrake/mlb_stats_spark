#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

source $SRCDIR/checkVirtualEnv.sh
source $SRCDIR/spark_config.sh

time spark-submit  \
--master local[*] \
--driver-memory 9G \
--executor-memory 1G \
--packages com.databricks:spark-csv_2.10:1.3.0 \
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.eventLog.enabled=true" \
--conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
--py-files $SRCDIR/scrape.py,$SRCDIR/AbstractDF.py,$SRCDIR/Game.py,$SRCDIR/GameEvents.py,$SRCDIR/BattingStats.py,$SRCDIR/PitchingStats.py,$SRCDIR/GamePlayers.py \
$SRCDIR/CreateStatsRDD.py -b 20130401
RETVAL=$?
#$SRCDIR/CreateStatsRDD.py -b 20120404
#$SRCDIR/CreateStatsRDD.py -b 20130401
#$SRCDIR/CreateStatsRDD.py -b 20140405

echo "status $RETVAL"
exit $RETVAL

