#!/usr/bin/env python

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext, Row, HiveContext
from datetime import datetime
import shutil
import getopt
import sys
import json
from CreateStatsRDD import CreateStatsRDD

class EncodeCategorical(object):

    categories = ['game_weekday', 'player_id', 'stadium_id', 'game_time_hour', 'game_time_month', 'game_weekday', 'is_divisional', 'game_year', 'rl', 'fd_position', 'bats', 'status', 'isHome', 'player_division', 'opponent_division', 'player_league', 'opponent_league', 'player_team_abbrev', 'opponent_team_abbrev', 'has_dome', 'icon', 'blowing_in', 'blowing_out', 'precipType'  ]

    # TODO remove useJSON
    def __init__(self, sqlContext, rdd, cols, useJSON, json_filename):
        self.cols = cols
        self.rdd = rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        self.useJSON = useJSON
        self.sqlContext = sqlContext
        self.json_filename = json_filename
        self.d = {}
        self.numVals = {}

    def createVals(self):
        if self.useJSON:
            with open(self.json_filename, 'r') as f:
                self.d = json.load(f)
            return
        self.d = {}
        for col in self.cols:
            print "GETTING VALS for col", col
            vals = self.rdd.select(col).distinct().orderBy(col).collect()
            print "vals=", vals

            self.numVals[col] = len(vals)
            self.d[col] = {}
            i = 0
            for row in vals:
                drow = row.asDict()
                print "row=", drow
                self.d[col][drow[col]] = i
                i += 1

        with open(self.json_filename, 'w') as f:
            json.dump(self.d, f) 

    def transform(self):

        self.createVals()
        print "d=", self.d

        mapCols = self.cols
        d = self.d

        def convertMap(x):
            x = x.asDict()
            for col in mapCols:
                x[col] = d[col][x[col]]
                #print "x=", x[col], "col=", col, "new=", x["Z_" + col]
            return Row(**x)

        self.rdd = self.rdd.map(convertMap)
        return self.rdd
        

    def convert(self):
        self.transform()
        print "numVals=", self.numVals
        return (self.rdd, self.numVals)


def rmTree(dirname):
    try:
        shutil.rmtree(dirname)
    except OSError:
        pass


def encodeRDD(sqlContext, srcFile, encFile, numValFile, encodeList, useJSON, json_val_filename):
    rdd = sqlContext.parquetFile(srcFile)

    ec = EncodeCategorical(sqlContext, rdd, encodeList, useJSON, json_val_filename)
    (newrdd, numVals) = ec.convert()

    rdd = sqlContext.createDataFrame(newrdd)
    rdd.printSchema()
    dirname = encFile

    rmTree(dirname)
    rdd.saveAsParquetFile(dirname)

    numValRdd = sc.parallelize([numVals]).coalesce(1).toDF()
    dirname = numValFile
    rmTree(dirname)
    numValRdd.save(dirname, 'json')

def getCommandLine():
    useJSON = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'r')
    except getopt.GetoptError:
        print sys.argv[0] + " [-r] use existing rdd."
        sys.exit

    for opt, arg in opts:
        if opt == '-r':
            useJSON = True

    return useJSON

if __name__ == '__main__':
    useJSON = getCommandLine()
    print "Starting.", datetime.now()
    sc = SparkContext()
    sqlContext = HiveContext(sc)


    rddDir = CreateStatsRDD.rddDir

    print "Encode Pitchers"
    pitcherCategories = list(EncodeCategorical.categories)
    pitcherCategories.append('is_starter')
    encodeRDD(sqlContext, rddDir + "pitching_features.parquet", rddDir + "pitching_features.enc.parquet", rddDir + "pitching_features.numvals.json", pitcherCategories, useJSON, rddDir + "pitching_encoded.json")

    print "Encode Hitters"
    encodeRDD(sqlContext, rddDir + "batting_features.parquet", rddDir + "batting_features.enc.parquet", rddDir + "batting_features.numvals.json", EncodeCategorical.categories, useJSON, rddDir + "batting_encoded.json")

    sc.stop()
