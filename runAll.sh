#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

source $SRCDIR/checkVirtualEnv.sh
source $SRCDIR/spark_config.sh

cd $SRCDIR
git pull

/bin/date
echo "running getMLBStats.sh"
$SRCDIR/getMLBStats.sh
RETVAL=$?
if [[ $RETVAL -gt 0 ]]; then
    echo "getMLBStats failed."
    exit 1
fi

/bin/date
echo "running createStats.sh"
$SRCDIR/createStats.sh
RETVAL=$?
if [[ $RETVAL -gt 0 ]]; then
    echo "createStats.sh failed."
    exit 1
fi

/bin/date
echo "running updateWeather.sh"
$SRCDIR/updateWeather.sh
RETVAL=$?
if [[ $RETVAL -gt 0 ]]; then
    echo "updateWeather.sh failed."
    exit 1
fi

/bin/date
echo "running movingAverage.sh"
$SRCDIR/movingAverage.sh
RETVAL=$?
if [[ $RETVAL -gt 0 ]]; then
    echo "movingAverage.sh failed."
    exit 1
fi

/bin/date
echo "running encode.sh"
$SRCDIR/encode.sh
RETVAL=$?
if [[ $RETVAL -gt 0 ]]; then
    echo "encode.sh failed."
    exit 1
fi

/bin/date
echo "running runml.sh"
$SRCDIR/runml.sh
RETVAL=$?
if [[ $RETVAL -gt 0 ]]; then
    echo "runml.sh failed."
    exit 1
fi

/bin/date
