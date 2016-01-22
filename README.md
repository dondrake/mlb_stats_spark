# mlb_stats_spark
Set of Spark applications that download MLB events and statistics for creating a fantasy baseball predictive model

# MORE TO COME
+ This is a work in progress as I migrate code from my private repo to this public Apache licensed open source project.

# Requirements
+ Simply download a pre-compiled version of Spark from http://spark.apache.org/downloads.html  Spark 1.5.2 and Spark 1.6 should work just fine.
+ Pyspark is utilized, and Python v2.7.x is supported. No plans at this time to support Python 3.x.
+ Virtualenv's are utilized to download and manage all third-party modules. Virtualenv needs to be installed prior to running.  The first time a script is executed, all third-party modules are installed.

# Running
+ runAll.sh will download the previous days MLB statistics and run end-to-end processing.
  + getMLBStats.sh - downloads all raw statistics from http://gd2.mlb.com/components/game/mlb/
    + by default, it will download yesterday's statistics.  Passing -b YYYMMDD and/or -e YYYYMMDD to download specific date ranges
  + createStats.sh - recreates all player-level statistics from GameEvents downloaded 
    + generates both JSON datasets and Parquet datasets
