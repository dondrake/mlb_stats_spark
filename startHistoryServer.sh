#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

source $SRCDIR/spark_config.sh

$SPARK_HOME/sbin/start-history-server.sh $EVENT_LOG_DIR
