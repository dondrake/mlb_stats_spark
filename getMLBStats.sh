#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

source $SRCDIR/checkVirtualEnv.sh
source $SRCDIR/spark_config.sh

which python 

spark-submit  \
--master local[*] \
--conf "spark.eventLog.enabled=true" \
--conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
--py-files $SRCDIR/scrape.py \
$SRCDIR/DownloadMLB.py  $*
RETVAL=$?

exit $RETVAL

