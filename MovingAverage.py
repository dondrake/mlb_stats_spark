#!/usr/bin/env python

import json
import shutil
from datetime import datetime, date, timedelta
import xml.etree.ElementTree as ET
import sys
import os
from operator import itemgetter
from collections import defaultdict
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext, Row, HiveContext
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType, DoubleType, ByteType, ShortType
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
from AbstractDF import AbstractDF, RDDBuilder
import numpy as np
from CreateStatsRDD import CreateStatsRDD
from Game import Games, Game
from GamePlayers import GamePlayer
from UpdateWeather import UpdateWeather, Weather


SRCDIR = os.path.dirname(os.path.realpath(__file__))

allDates = None

class MovingLengths(object):
    def __init__(self):
        self.movingLengths = []
        self.maxMovingLength = 0
        self.playerStats = set([])
        self.setMax()

    def setMax(self):
        for i in self.movingLengths:
            if i != 'SEASON' and i > self.maxMovingLength:
                self.maxMovingLength = i

class PitcherMovingLengths(MovingLengths):
    def __init__(self):
        MovingLengths.__init__(self)
        self.movingLengths = [14, 30, 60]
        self.playerStats = set(['fd_points', 'earned_runs', 'hr', 'innings_pitched', 'num_hits', 'num_runs', 'num_pitches', 'num_walks', 'hr', 'num_strikes' ,'win', 'strikeouts', 'batters_faced' ])
    
        self.setMax()

class HitterMovingLengths(MovingLengths):
    def __init__(self):
        MovingLengths.__init__(self)
        self.movingLengths = [4, 7, 14, 30]
        self.playerStats = set(['fd_points', 'ab', '_1b', '_2b', '_3b', 'hr', 'rbi', 'r', 'bb', 'sb', 'hbp', 'out', 'total_hits' ])
    
        self.setMax()

pitcherMoving = PitcherMovingLengths()
hitterMoving = HitterMovingLengths()

movingLength = None

#playerStats = set(['fd_points', 'earned_runs', 'hr', 'innings_pitched', 'num_hits', 'num_runs', 'num_pitches'])
#movingLengths = [3, 7, 14, 30, "SEASON"]
#movingLengths = [7, 14, 30]
#maxMovingLength=0
#for i in movingLengths:
#    if i != 'SEASON' and i > maxMovingLength:
#        maxMovingLength = i

def pitcherRowToObj(row):
    d = row.asDict()
    if 'num_pitches' in d:
        if d['num_pitches'] == 0:
            return
    elif d['ab'] == 0 and d['bb'] == 0 and d['fd_points'] == 0.0:
        return

    obj = [ d[k] for k in movingLength.playerStats]
    #print "playerStats=", movingLength.playerStats

    #print "pitcherRowToObj: START", d
    #print "playerId=", d['player_id']
    #print "d['game_date']=", d['game_date']

    # TODO - don't make this a linear search, also stop after d['game_date'
    # go through all the asofdates:
    for gdate in allDates.value[d['game_date'].year]:
        dateString = gdate.strftime("%Y%m%d")
        #print "dateString=", dateString
        if d['game_date'] < gdate:
            key = str(d['player_id']) + "_" + dateString + "_SEASON" 
            #print "Adding key", key, "dateString=", dateString, " movingL=SEASON"
            yield (key, obj)
        if (d['game_date'] >= ( gdate - timedelta(days=movingLength.maxMovingLength)) and d['game_date'] < gdate):
            for movingLen in movingLength.movingLengths:
                if (d['game_date'] >= ( gdate - timedelta(days=movingLen)) and d['game_date'] < gdate):
                    key = str(d['player_id']) + "_" + dateString + "_" + str(movingLen)
                    #print "Adding key", key, "dateString=", dateString, " movingL=", movingLen
                    yield (key, obj)

def calcGroup(tuple):
    key, iter = (tuple)
    (playerId, gdate, movingLen) = key.split('_')
    #print "****"
    #print "key=", key
    #print "playerId=", playerId
    #print "date=", gdate
    #print "movingLen=", movingLen
    #print "calcGroup iter=", iter
    length = sum(1 for el in iter)
    #print "length=", length
    a = np.array([ r for r in iter])

    #print "playerStats=", movingLength.playerStats
    #print "len a=", len(a)
    #print "a=", a
    avg = np.mean(a, axis=0)
    std = np.std(a, axis=0)
    total = np.sum(a, axis=0)
    var = np.var(a, axis=0)
    l1norm = np.linalg.norm(a, ord=1, axis=0)
    l2norm = np.linalg.norm(a, ord=2, axis=0)
    #print "****"

    metrics = {}
    metrics['player_id'] = int(playerId)
    metrics['game_date'] = datetime.strptime(gdate, '%Y%m%d').date()
    #for agg in ((avg, 'avg'), (std, 'std') ):
    for agg in ((avg, 'avg'), (std, 'std'), (var, 'var'), (l1norm, 'l1norm'), (l2norm, 'l2norm')):
        index = 0
        for f in movingLength.playerStats:
            field = 'mv_' + movingLen + '_' + agg[1] + '_' + f
            metrics[field] = float(agg[0][index])
            index += 1
    
    index = 0
    sums = {}
    for f in movingLength.playerStats:
        sums[f] = total[index]
        index += 1

    if '_1b' in  movingLength.playerStats:
        # hitter
        tb = sums['_1b'] + 2*sums['_2b'] + 3*sums['_3b'] + 4*sums['hr']
        field = 'mv_' + movingLen + '_' + 'tb'
#        metrics[field] = float(tb)
        if sums['ab'] > 0:
            slugging = tb / float(sums['ab'])
        else:
            slugging = 0.0
        field = 'mv_' + movingLen + '_' + 'slugging'
        metrics[field] = float(slugging)

        # on base percentage
        obp_num = sums['_1b'] + sums['_2b'] + sums['_3b'] + sums['hr'] + sums['bb'] + sums['hbp']
        # TODO NEED SACRIFICE FLIES
        obp_den = sums['ab'] + sums['bb'] + sums['hbp']
        if obp_den > 0:
            obp = obp_num / float(obp_den)
        else:
            obp = 0.0
        field = 'mv_' + movingLen + '_' + 'obp'
        metrics[field] = float(obp)

        field = 'mv_' + movingLen + '_' + 'ops'
        metrics[field] = float(obp + slugging)
    else:
        # pitcher
        #self.playerStats = set(['fd_points', 'earned_runs', 'hr', 'innings_pitched', 'num_hits', 'num_runs', 'num_pitches', 'num_walks', 'hr', 'num_strikes' ,'win', 'strikeouts', 'batters_faced'])
        if sums['innings_pitched'] > 0:
            whip = (sums['num_walks'] + sums['num_hits']) / float(sums['innings_pitched'])
        else:
            whip = 0.0
        field = 'mv_' + movingLen + '_' + 'whip'
        metrics[field] = float(whip)
        
        babip_num = sums['num_hits'] - sums['hr']
        # TODO NEED SACRIFICE FLIES
        babip_den = sums['batters_faced'] - sums['strikeouts'] - sums['hr'] 
        if babip_den > 0:
            babip = babip_num / float(babip_den)
        else:
            babip = 0.0
        field = 'mv_' + movingLen + '_' + 'babip'
        metrics[field] = float(babip)

        if sums['innings_pitched'] > 0:
            k_per_nine = 9.0 * sums['strikeouts'] / sums['innings_pitched']
            bb_per_nine = 9.0 * sums['num_walks'] / sums['innings_pitched']
        else:
            k_per_nine = 0.0
            bb_per_nine = 0.0
        field = 'mv_' + movingLen + '_' + 'k_per_nine'
        metrics[field] = float(k_per_nine)

        field = 'mv_' + movingLen + '_' + 'bb_per_nine'
        metrics[field] = float(bb_per_nine)
    #print "metrics=", metrics
    return (playerId + '_' + gdate, metrics)

def mergeMetrics(x, y):
    if x is None:
        print "x is None"
        return y
    if y is None:
        print "y is None"
        return x
    z = x.copy()
    z.update(y.copy())
    return z


class MovingAverage(object):
    #TODO remove asOfDate, it's not used
    def __init__(self, sqlContext, asOfDate=None):
        self.sqlContext = sqlContext
        if asOfDate is None:
            self.asOfDate = date.today()
        else:
            self.asOfDate = asOfDate
        print "asOfDate=", self.asOfDate
        # name of directory holding pitcher_games.parquet or hitter_games.parquet
        self.player_games_dir = None
        self.moving_average_results_file = None

    def rmtree(self, dir):
        try:
            shutil.rmtree(dir)
        except OSError, e:
            print "OSERROR:", str(e)
            pass

    def run(self):
        global allDates
        print "running."

        if self.player_games_dir is None:
            raise Exception("player_games is not set.")
        if self.moving_average_results_file is None:
            raise Exception("moving_average_results_file is not set.")

        print "loading player_games_dir = ", CreateStatsRDD.rddDir + "/" + self.player_games_dir
        pg = sqlContext.load(CreateStatsRDD.rddDir + "/" + self.player_games_dir)
        pg.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        pg.registerTempTable("player_games")

        games = sqlContext.read.parquet(CreateStatsRDD.rddDir + "/" + Games.table_name + ".parquet")
        games.registerTempTable("games")
        
        gameDates = sqlContext.sql("select distinct game_date from games ").map(lambda x: x[0]).coalesce(8).collect()
        print "len gameDates=", len(gameDates)
        print "gameDates=", gameDates
        yeargames = defaultdict(list)
        for g in gameDates:
            # for some reason, the above lamdda converts the date object to datetime, 
            # we need to coerce it back here.
            #yeargames[g.year].append(g.date())
            # It no longer does that?
            yeargames[g.year].append(g)

        print "yeargames=", yeargames

        allDates = sc.broadcast(yeargames)
        print "allDates=", allDates

        #sql = "select * from player_games where game_date < '{0}'".format(self.asOfDate)
        sql = "select * from player_games "
        print "sql=", sql
        playerStats = sqlContext.sql(sql)
        playerStats.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

        #print "count=", playerStats.count()
        #print playerStats.take(10)
        p = playerStats.flatMap(pitcherRowToObj).repartition(20).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        #p = playerStats.flatMap(pitcherRowToObj).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        #print "2count=", p.count()
        #print p.take(10)
        
        # reduceByKey here??
        g = p.groupByKey().persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        #print "gcount=", g.count()
        #print g.take(10)

        s = g.map(calcGroup).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        #print "scount=", s.count()
        #print s.take(10)

        t = s.reduceByKey(mergeMetrics).values().persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        print "t="
        print t.take(10)

        pg.unpersist()
        p.unpersist()
        g.unpersist()
        s.unpersist()

        # TODO
        # create the schema here

        #Merge all of the columns through createDataFrame, can be sparse for players
        # who don't play everyday
        srdd = sqlContext.createDataFrame(t, samplingRatio=0.1)
        srdd.printSchema()

        #clear nulls for players who dont play everyday
        u = srdd.fillna(0.0)
        print "u=", u.take(10)

        print "saving."
        parquetFile = CreateStatsRDD.rddDir + "/" + self.moving_average_results_file
        self.rmtree(parquetFile)
        u.saveAsParquetFile(parquetFile)
        print "saved:", parquetFile

        t.unpersist()

class PitcherMovingAverage(MovingAverage):

    def __init__(self, sqlContext, asOfDate=None):
        MovingAverage.__init__(self, sqlContext, asOfDate)
        self.player_games_dir = "pitcher_games.parquet"
        self.moving_average_results_file = "pitcher_moving_averages.parquet"
        global movingLength
        movingLength = pitcherMoving

class HitterMovingAverage(MovingAverage):

    def __init__(self, sqlContext, asOfDate=None):
        MovingAverage.__init__(self, sqlContext, asOfDate)
        self.player_games_dir = "batter_games.parquet"
        self.moving_average_results_file = "batter_moving_averages.parquet"
        global movingLength
        movingLength = hitterMoving

def commonTransform(row):
    #print "row=", row
    row = row.copy()

    # Game time hour
    row['game_time_hour'] = int(row['home_time'].split(':')[0])
    # Game month
    row['game_time_month'] = row['game_time_et'].month
    # weekday
    row['game_weekday'] = row['game_time_et'].weekday()

    # is divisional game?
    row['is_divisional'] = int(row['home_division'] == row['away_division'] and row['home_league'] == row['away_league'])

    row['is_cross_league'] = int(row['home_league'] != row['away_league'])

    row['isHome'] = False
    # Put Game Stats into player_team and opponent_team
    if row['team_id'] == row['home_team_id']:
        row['isHome'] = True
        row['player_team_id'] = row['home_team_id']
        row['player_team_abbrev'] = row['home_abbrev']
        if (row['home_won'] + row['home_loss']) > 0:
            row['player_team_win_pct'] = row['home_won'] / float(row['home_won'] + row['home_loss'])
        else:
            row['player_team_win_pct'] = 0.0
        row['player_league'] = row['home_league']
        row['player_division'] = row['home_league'] + '_' + row['home_division']

        row['opponent_team_id'] = row['away_team_id']
        row['opponent_team_abbrev'] = row['away_abbrev']
        if (row['away_won'] + row['away_loss']) > 0:
            row['opponent_team_win_pct'] = row['away_won'] / float(row['away_won'] + row['away_loss'])
        else:
            row['opponent_team_win_pct'] = 0.0
        row['opponent_league'] = row['away_league']
        row['opponent_division'] = row['away_league'] + '_' + row['away_division']
    else:
        row['isHome'] = False
        row['player_team_id'] = row['away_team_id']
        row['player_team_abbrev'] = row['away_abbrev']
        if (row['away_won'] + row['away_loss']) > 0:
            row['player_team_win_pct'] = row['away_won'] / float(row['away_won'] + row['away_loss'])
        else:
            row['player_team_win_pct'] = 0.0
        row['player_league'] = row['away_league']
        row['player_division'] = row['away_league'] + '_' + row['away_division']

        row['opponent_team_id'] = row['home_team_id']
        row['opponent_team_abbrev'] = row['home_abbrev']
        if (row['home_won'] + row['home_loss']) > 0:
            row['opponent_team_win_pct'] = row['home_won'] / float(row['home_won'] + row['home_loss'])
        else:
            row['opponent_team_win_pct'] = 0.0
        row['opponent_league'] = row['home_league']
        row['opponent_division'] = row['home_league'] + '_' + row['home_division']

    # remove unecessary team fields
    del row['team_id']
    del row['home_code']
    del row['home_abbrev']
    del row['home_name']
    del row['home_won']
    del row['home_loss']
    del row['home_division']
    del row['home_league']
    del row['home_team_id']
    del row['away_code']
    del row['away_abbrev']
    del row['away_name']
    del row['away_won']
    del row['away_loss']
    del row['away_division']
    del row['away_league']
    del row['away_team_id']

    # others
    del row['game_time_et']
    #del row['game_date']
    del row['home_time']

    return row

class CreateFeatures(object):
    def __init__(self, sqlContext):
        self.sqlContext = sqlContext
        self.rddDir = CreateStatsRDD.rddDir

    def rmtree(self, dir):
        try:
            shutil.rmtree(dir)
        except OSError, e:
            print "OSError: ", str(e)
            pass

    def createBatterFeatures(self, sqlContext, games, gamePlayers, stadium, weather):

        batter_mov_avg = sqlContext.read.parquet(self.rddDir + "/" + "batter_moving_averages.parquet")
        batter_mov_avg.registerTempTable("bma")
        batter_mov_avg.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        print "batter_mov_avg=", batter_mov_avg.take(2)

        bg = sqlContext.read.parquet(self.rddDir + "/batter_games.parquet")
        bg.registerTempTable("bg")
        bg.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

        batter_games = sqlContext.sql("select *  \
            from games g    \
            join stadium on (g.stadium_id = stadium.stadium_id) \
            join game_players gp on (g.game_date >= gp.effective_start_dt \
                and g.game_date < gp.effective_stop_dt \
                and (g.home_team_id = gp.team_id or g.away_team_id = gp.team_id) ) \
            join bma on (g.game_date = bma.game_date \
                and gp.player_id = bma.player_id ) \
            join weather w on (g.game_id = w.game_id) \
            left outer join bg on (g.game_id = bg.game_id \
                and gp.player_id = bg.player_id and bg.player_id = bma.player_id ) \
            where gp.fd_position != 'P' \
        ")

        test_sql = sqlContext.sql("select *  \
            from stadium \
")
#            from games g    \
#            join stadium on (g.stadium_id = stadium.stadium_id) \
#            join game_players gp on (g.game_date >= gp.effective_start_dt \
#                and g.game_date < gp.effective_stop_dt \
#                and (g.home_team_id = gp.team_id or g.away_team_id = gp.team_id) ) \
#            join bma on (g.game_date = bma.game_date \
#                and gp.player_id = bma.player_id ) \
#            join weather w on (g.game_id = w.game_id) \
#            left outer join bg on (g.game_id = bg.game_id \
#                and gp.player_id = bg.player_id and bg.player_id = bma.player_id ) \
#            where gp.fd_position != 'P' \
#        ")

        print "test_sql schema=", test_sql.printSchema()
        c = test_sql.count()
        print "test_sql count = ", c
        print "test_sql = ", test_sql.collect
        print "batter_games schema=", batter_games.printSchema()
        print "columns=", batter_games.columns
        c = batter_games.count()
        print "batter_games count=", c,  " "
        unique_cols = []
        unique_cols.extend(Game().getSelectFields(games))
        unique_cols.extend(GamePlayer().getSelectFields(gamePlayers))
        unique_cols.extend([batter_mov_avg[name] for name in batter_mov_avg.columns if (name not in ['game_id', 'game_date']) ])
        unique_cols.extend([bg[name] for name in bg.columns if ( name in  ['fd_points', '_1b', '_2b', '_3b', 'hr', 'rbi', 'r', 'bb', 'sb', 'hbp', 'out']) ])
        unique_cols.extend([stadium[name] for name in stadium.columns if (name in ['has_dome', 'capacity', 'elevation']) ])
        unique_cols.extend(Weather().getSelectFields(weather))
        print "unique_cols = ", unique_cols
        unique_cols = sorted(unique_cols, key=lambda x: x._jc.toString())

        # TODO - need to get the correct team info from games table

        print "batter_games schema=", batter_games.schema
        print "unique_cols=", unique_cols
        batting_features = batter_games.select(*unique_cols)
        print "batting_features=", batting_features.schema
        #print "batting_features=", batting_features.show()
        print "0batting_features take=", batting_features.take(10)

        def transformBatters(row_object):
            row = row_object.asDict()
            row = commonTransform(row)

            return Row(**row)

        batting_features = batting_features.map(transformBatters).coalesce(16)
        print "batting_features take=", batting_features.take(10)
        batting_features = sqlContext.createDataFrame(batting_features, schema=None, samplingRatio=0.4)
        batting_features.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        print "batting_features.schema=", batting_features.printSchema()
        print "batting_features=", batting_features.show()

        self.rmtree(self.rddDir + "/" + "batting_features.parquet")
        batting_features.saveAsParquetFile(self.rddDir + "/" + "batting_features.parquet")

        bcsv = batting_features.rdd.coalesce(1).toDF()
        self.rmtree(self.rddDir + "/" + "batting_features.csv")
        print "bcsv schema=", bcsv.printSchema()
        bcsv.save(self.rddDir + "/" + "batting_features.csv", "com.databricks.spark.csv")
        with open(self.rddDir + "/" + "batting_features.csv/_header", "w") as text_file:
            text_file.write(",".join(bcsv.columns))

        batting_features.unpersist()
        batter_mov_avg.unpersist()
        bg.unpersist()

    def createPitcherFeatures(self, sqlContext, games, gamePlayers, stadium, weather):

        pitcher_mov_avg = sqlContext.read.parquet(self.rddDir + "/" + "pitcher_moving_averages.parquet")
        pitcher_mov_avg.registerTempTable("pma")
        pitcher_mov_avg.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        print "pitcher_mov_avg=", pitcher_mov_avg.take(2)

        pg = sqlContext.read.parquet(self.rddDir + "/" + "pitcher_games.parquet")
        pg.registerTempTable("pg")
        pg.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

        pitcher_games = sqlContext.sql("select *  \
            from games g    \
            join stadium on (g.stadium_id = stadium.stadium_id) \
            join game_players gp on (g.game_date >= gp.effective_start_dt \
                and g.game_date < gp.effective_stop_dt \
                and (g.home_team_id = gp.team_id or g.away_team_id = gp.team_id) ) \
            join pma on (g.game_date = pma.game_date \
                and gp.player_id = pma.player_id ) \
            join weather w on (g.game_id = w.game_id) \
            left outer join pg on (g.game_id = pg.game_id \
                and gp.player_id = pg.player_id and pg.player_id = pma.player_id ) \
        ").cache()
#            where pg.is_starter = TRUE \

        unique_cols = []
        unique_cols.extend(Game().getSelectFields(games))
        unique_cols.extend(GamePlayer().getSelectFields(gamePlayers))
        unique_cols.extend([pitcher_mov_avg[name] for name in pitcher_mov_avg.columns if (name != 'game_id') ])
        unique_cols.extend([pg[name] for name in pg.columns if (name in ['earned_runs', 'strikeouts', 'innings_pitched', 'fd_points', 'is_starter', 'win']) ])
        unique_cols.extend([stadium[name] for name in stadium.columns if (name in ['has_dome', 'capacity', 'elevation']) ])
        unique_cols.extend(Weather().getSelectFields(weather))

        unique_cols = sorted(unique_cols, key=lambda x: x._jc.toString())

        print "pitcher_games schema=", pitcher_games.schema
        print "unique_cols=", unique_cols
        pitching_features = pitcher_games.select(*unique_cols)
        print "pitching_features=", pitching_features.schema
        #print "pitching_features=", pitching_features.show()

        def transformPitchers(row_object):
            row = row_object.asDict()
            row = commonTransform(row)

            return Row(**row)

        pitching_features = pitching_features.map(transformPitchers).coalesce(16).toDF()
        pitching_features.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        print "pitching_features=", pitching_features.columns
        print "pitching_features=", pitching_features.show()
        print "pitching_features count=", pitching_features.count()

        self.rmtree(self.rddDir + "/" + "pitching_features.parquet")
        pitching_features.saveAsParquetFile(self.rddDir + "/" + "pitching_features.parquet")

        # pitching_features can't save to CSV directly, it causes the python process
        # to die without an error message
        # For now, just re-read the parquet and save it
        pcsv = sqlContext.read.parquet(self.rddDir + "/" + "pitching_features.parquet").rdd.coalesce(1).toDF()
        print "pcsv count=", pcsv.count()
        print "pcsv rows=", pcsv.take(4)
        self.rmtree(self.rddDir + "/" + "pitching_features.csv")
        pcsv.save(self.rddDir + "/" + "pitching_features.csv", "com.databricks.spark.csv")
        with open(self.rddDir + "/" + "pitching_features.csv/_header", "w") as text_file:
            text_file.write(",".join(pcsv.columns))

        pitching_features.unpersist()
        pitcher_mov_avg.unpersist()

    def run(self):
        
        games = sqlContext.read.parquet(self.rddDir + "/" + Games.table_name + ".parquet")
        games.registerTempTable("games")
        games.cache()
        print "games=", games
        print games.take(2)

        gamePlayers = sqlContext.read.parquet(self.rddDir + "/" + "game_players.parquet")
        gamePlayers.registerTempTable("game_players")
        gamePlayers.cache()

        stadium = sqlContext.load(source="com.databricks.spark.csv", header="true", path = SRCDIR + "/stadium.csv")
        stadium.registerTempTable("stadium")
        stadium.cache()

        weather = sqlContext.read.json(UpdateWeather.weatherFile, schema=Weather.schema)
        weather.registerTempTable("weather")
        weather.cache()

        # create Batter Features
        self.createBatterFeatures(sqlContext, games, gamePlayers, stadium, weather)

        # create Pitcher Features
        self.createPitcherFeatures(sqlContext, games, gamePlayers, stadium, weather)


if __name__ == '__main__':
    print "Starting.", datetime.now()
    sc = SparkContext()
    #sqlContext = SQLContext(sc)
    sqlContext = HiveContext(sc)

    print "Starting PitcherMovingAverage", datetime.now()
    x = PitcherMovingAverage(sqlContext)
    x.run()
    print "Ending PitcherMovingAverage", datetime.now()
    

    print "Starting HitterMovingAverage", datetime.now()
    x = HitterMovingAverage(sqlContext)
    x.run()
    print "Ending HitterMovingAverage", datetime.now()

    print "Starting CreateFeatures", datetime.now()
    x = CreateFeatures(sqlContext)
    x.run()
    print "Ending CreateFeatures", datetime.now()

    sc.stop()
