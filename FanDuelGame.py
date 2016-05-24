#!/usr/bin/env python2.7

import json
import sys
from pyspark import SparkContext, StorageLevel, SparkConf
from pyspark.sql import SQLContext, Row, HiveContext
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
import getopt
import csv
from itertools import combinations, groupby
from collections import defaultdict
from operator import itemgetter
from datetime import datetime,timedelta
import copy
import urllib2
import math
import os
import re
from pytz import timezone
from MLB import MLB
from AbstractDF import AbstractDF
from EncodeCategorical import EncodeCategorical
from CreateStatsRDD import CreateStatsRDD
import TrainModel 
from OptimizeRoster import OptimizeRoster



class OptimalSpark(OptimizeRoster):

    def __init__(self, dataDirectory, eligiblePlayers):
        super(OptimalSpark, self).__init__(dataDirectory, eligiblePlayers)
        self.statsColumn = 'fd_points'

    def getPlayerStats(self):
        print "getPlayerStats - NOOP"
        for p in self.eligiblePlayers:
            self.statsRoster[p['lookup_name']] = p

    def buildPositionPlayers(self):
    
        for pos in self.positions:
            self.positionPlayers[pos] = []

        for p in self.eligiblePlayers:
            self.positionPlayers[p['position']].append(p)


class FanDuelGame(object):

    BaseDataDirectory = os.path.expanduser('~') + '/fd.mlb/fd.mlb.data'
    StaticPlayersFilename='fd.players'

    def __init__(self, sqlContext, gameJSONFilename, dataDirectory, gameDescription):
        self.sqlContext = sqlContext
        self.gameJSONFilename = gameJSONFilename
        self.dataDirectory = dataDirectory
        self.gameDescription = gameDescription
        self.gameDate = None
        self.setupDirectories()
        self.eligiblePlayers = {}

    def setupDirectories(self):
        if self.dataDirectory is None:
            if not os.path.isfile(self.gameJSONFilename):
                print 'file does not exist:', self.gameJSONFilename
                sys.exit()
            # we better have a -p filename 
            # create the directory where everything will live
            self.dataDirectory = self.BaseDataDirectory + '/' +datetime.now(timezone('US/Central')).strftime('%Y%m%d.%H%M')
            self.gameDate = datetime.date(datetime.now(timezone('US/Central')))
            if gameDescription is not None:
                self.dataDirectory += gameDescription
            print "dataDirectory=", dataDirectory
            if os.path.exists(self.dataDirectory):
                print 'Directory already exists:', self.dataDirectory
                sys.exit(1)
            os.makedirs(self.dataDirectory)

            # move the input of fd players.json to this directory.
            if 'json' in self.gameJSONFilename:
                extension = '.json'
            else:
                extension = '.csv'
            newFilename = self.dataDirectory + '/' + self.StaticPlayersFilename + extension
            os.rename(self.gameJSONFilename, newFilename)
            self.gameJSONFilename = newFilename
        else:
            self.gameJSONFilename = self.dataDirectory + '/' + self.StaticPlayersFilename + '.json'
            if not os.path.isfile(self.gameJSONFilename):
                self.gameJSONFilename = self.dataDirectory + '/' + self.StaticPlayersFilename + '.csv'
                if not os.path.isfile(self.gameJSONFilename):
                    print 'file does not exist:', self.gameJSONFilename
                    sys.exit()
            m = re.search("\/(\d{8})\.\d{4}", self.dataDirectory)
            if m:
                print "got m:", m
                print m.group(1)
                self.gameDate = datetime.date(datetime.strptime(m.group(1), '%Y%m%d'))
                print "selfgd=", self.gameDate
        print "self.gameJSONFilename=", self.gameJSONFilename

    def getEligiblePlayers(self):
        if 'json' in self.gameJSONFilename:
            return self.getEligiblePlayersJSON()
        else:
            return self.getEligiblePlayersCSV()

    def getEligiblePlayersCSV(self):
        mlb = MLB(self.sqlContext, self.dataDirectory, self.gameDate, isReadOnly=False)
        lineups = mlb.getLineups()

        with open(self.gameJSONFilename) as csvfile:
            reader = csv.DictReader(csvfile)
            for fdPlayer in reader:
                print "looking at player=", fdPlayer
                # {'Salary': '2200', 'FPPG': '1.5', 'Played': '75', 'Last Name': 'Pagan', 'Injury Details': 'Knee', 'Injury Indicator': 'DTD', 'First Name': 'Angel', 'Game': 'SFG@WAS', 'Team': 'SFG', 'Position': 'OF', 'Probable pitcher': '', 'Opponent': 'WAS'}
            
                player = {}
                #player['key'] = key
                player['position'] = fdPlayer['Position']
                player['name'] = fdPlayer['First Name'] + ' ' + fdPlayer['Last Name']
                player['game_id'] = fdPlayer['Game']
                #player['team_id'] = fdPlayer['Team']
                #player['id3'] = fields[4]
                player['salary'] = float(fdPlayer['Salary'])
                player['points'] = fdPlayer['FPPG']
                player['num_games'] = fdPlayer['Played']
                #player['some_boolean'] = fields[8]
                player['player_status'] = fdPlayer['Probable Pitcher']
                player['injury'] = fdPlayer['Injury Indicator']

                # We already have a team name, no need to convert from id
                #player['team'] = mlb.getTeamFromFanduelTeamId(player['team_id'])
                if fdPlayer['Team'] == 'TAM':
                    fdPlayer['Team'] = 'TB'
                player['team'] = mlb._validteams(fdPlayer['Team'])
                print player
                if player['team'] is None:
                    print "NO TEAM: ", fdPlayer['Team']
                    sys.exit(1)

                playerStatusKey = "{0}_{1}".format(player['name'], player['team'])

                pStatus = None
                try:
                    #pStatus = playerStatus[playerStatusKey]
                    pStatus = lineups[playerStatusKey]
                except KeyError:
                    pass

                print "pStatus=", pStatus
                if pStatus is None or (pStatus is not None and pStatus['status'] == 'No Lineup'):
                    # Try to skip non-probable pitchers
                    if player['position'] == 'P' and player['player_status'] == '':
                        print "Skipping, not probable pitcher"
                        continue

                    if pStatus is not None and pStatus['active'] == 'Inactive':
                        print "Skipping, player is inactive."
                        continue
                
                    if player['injury'] != '':
                        print "Skipping, player is injured."
                        continue
                else:
                    # Try to skip non-probable pitchers
                    if player['position'] == 'P':
                        if player['player_status'] == '':
                            print "Skipping, not probable pitcher"
                            continue
                    else:
                        if pStatus['status'] != 'Starter':
                            print "NOT STARTER, skipping."
                            continue
                        elif player['injury'] != '':
                            print "Skipping, player is injured."
                            continue
                eligiblePlayerKey = "{0}_{1}".format(player['name'], player['team'])
                self.eligiblePlayers[eligiblePlayerKey] = player
        return self.eligiblePlayers

    def getEligiblePlayersJSON(self):
        f = open(self.gameJSONFilename)
        data = json.load(f)
        f.close()

        mlb = MLB(self.sqlContext, self.dataDirectory, self.gameDate, isReadOnly=False)
        lineups = mlb.getLineups()
        for key in data:
            #print "key=", key
            fields = data[key]
            #print "item=", fields

            # [u'2B', u'Ed Lucas', u'83325', u'606', u'1000', u'2500', 1.3, 26, False, 0, u'']
            player = {}
            player['key'] = key
            player['position'] = fields[0]
            player['name'] = fields[1]
            player['game_id'] = fields[2]
            player['team_id'] = fields[3]
            player['id3'] = fields[4]
            player['salary'] = float(fields[5])
            player['points'] = fields[6]
            player['num_games'] = fields[7]
            player['some_boolean'] = fields[8]
            player['player_status'] = fields[9]
            player['injury'] = fields[10]

            player['team'] = mlb.getTeamFromFanduelTeamId(player['team_id'])
            print player

            playerStatusKey = "{0}_{1}".format(player['name'], player['team'])
            pStatus = None
            try:
                #pStatus = playerStatus[playerStatusKey]
                pStatus = lineups[playerStatusKey]
            except KeyError:
                pass

            print "pStatus=", pStatus
            if pStatus is None or (pStatus is not None and pStatus['status'] == 'No Lineup'):
                # Try to skip non-probable pitchers
                if player['position'] == 'P' and player['player_status'] not in [1, 5]:
                    print "Skipping, not probable pitcher"
                    continue

                if pStatus is not None and pStatus['active'] == 'Inactive':
                    print "Skipping, player is inactive."
                    continue

                if player['position'] != 'P' and player['player_status'] not in [4]:
                    print "Skipping, player_status not 4"
                    continue
            else:
                # Try to skip non-probable pitchers
                if player['position'] == 'P' and player['player_status'] in [1, 5]:
                    print "Keeping, not probable pitcher"
                    pass
                elif pStatus['status'] != 'Starter':
                    print "NOT STARTER, skipping."
                    continue

            playerStatsKey = player['name']

            if player['injury'] != '' or player['player_status'] in [2, 6]:
                print "Skipping, injury."
                continue


            eligiblePlayerKey = "{0}_{1}".format(player['name'], player['team'])
            self.eligiblePlayers[eligiblePlayerKey] = player
        return self.eligiblePlayers

    def run(self):
        self.getEligblePlayers()

def usage():
    print sys.argv[0] + ' -p player.json [-d existing.dir] [-g description] [-a] {actual score model}'
    sys.exit(1)

def getCommandLine():
    filename = None
    directory = None
    gameDescription = None
    actualModel = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'p:d:g:a')
    except getopt.GetoptError:
        usage()
    for opt, arg in opts:
        if opt =='-h':
            usage()
        elif opt == '-p':
            filename = arg
        elif opt == '-d':
            directory = arg
        elif opt == '-g':
            gameDescription = arg
        elif opt == '-a':
            actualModel = True


    if filename is None and directory is None:
        usage()
    return (filename, directory, gameDescription, actualModel)


def alternateNames(name, pids):

    altnames = [
        ('MICHAEL', 'MIKE'), 
        ('MIKE', 'MICHAEL'),
        ('JACOB', 'J.T.'),
        ('NORICHIKA', 'NORI'),
        ('CHI-CHI', 'CHI CHI'),
        (' JR.', ''),
        ('CHARLES', 'CHARLIE'),
        ('CHARLIE', 'CHARLES'),
        ('SKIP', 'JARED'),
        ('CLINTON', 'CLINT'),
        ('SAMUEL', 'SAM'),
        ('NATHAN', 'NATE'),
        ('ISAAC', 'IKE'),
    ]
    
    for names in altnames:
        #print "names=", names
        newname = name.replace(names[0], names[1])
        #print "newname=", newname
        if newname in pids:
            #print "got it", newname
            return newname
    
    return None

if __name__ == '__main__':
    print "Starting.", datetime.now()
    sconf = SparkConf().set("spark.buffer.pageSize", 1024*1024).setAppName("FanDuelGame")
    sc = SparkContext(conf=sconf)
    sqlContext = HiveContext(sc)

    rddDir = CreateStatsRDD.rddDir
    (filename, dataDirectory, gameDescription, actualModel)=getCommandLine()
    print "start: ", datetime.now()
    game = FanDuelGame(sqlContext, filename, dataDirectory, gameDescription)
    eligiblePlayers = game.getEligiblePlayers()
    print "eligiblePlayers=", eligiblePlayers

    print "gameDate=", game.gameDate

    # get MLB.com players
    gamePlayers = sqlContext.parquetFile(rddDir + "/" + "game_players.parquet")
    gamePlayers.registerTempTable("game_players")
    gamePlayers.cache()

    ldf = sqlContext.sql("select distinct lookup_name, player_id from game_players where '" + str(game.gameDate) + "' >= effective_start_dt and '" + str(game.gameDate) + "' < effective_stop_dt").collect()
    print "ldf=", ldf
    pids = {}
    for row in ldf:
        x = row.asDict()
        pids[x['lookup_name'].upper()] = x['player_id']
    
    print "pids=", pids
    with open(rddDir + "batting_encoded.json", 'r') as f:
        encoded = json.load(f)
    encodedPlayerIds = encoded['player_id']
    decodedHitterPlayerIds = dict(zip(encodedPlayerIds.values(), encodedPlayerIds.keys()))
    print "encodedPlayerIds=", encodedPlayerIds
    with open(rddDir + "pitching_encoded.json", 'r') as f:
        encoded = json.load(f)
    encodedPitcherPlayerIds = encoded['player_id']
    decodedPitcherPlayerIds = dict(zip(encodedPitcherPlayerIds.values(), encodedPitcherPlayerIds.keys()))
    encodedPlayerIds.update(encoded['player_id'])
    print "NOW encodedPlayerIds=", encodedPlayerIds
    # swap them so we can decode later
    # see above
    #decodePlayerIds = dict(zip(encodedPlayerIds.values(), encodedPlayerIds.keys()))

    predictHitters = []
    predictPitchers = []
    for p in eligiblePlayers:
        print "Looking at p=", p
        pp = {}
        pp['salary'] = eligiblePlayers[p]['salary']
        if eligiblePlayers[p]['position'] == 'P':
            predictPlayers = predictPitchers
        else:
            predictPlayers = predictHitters

        pp['position'] = eligiblePlayers[p]['position']
        pp['game_date'] = game.gameDate
        pp['team'] = eligiblePlayers[p]['team']

        print "pp=", pp
        if p.upper() in pids:
            pp['lookup_name'] = p.upper()
            try:
                pp['player_id'] = encodedPlayerIds[str(pids[p.upper()])]
            except KeyError:
                print "couldn't find ", p
                pp['player_id'] = 0
            predictPlayers.append(pp)
        else:
            #print "p not found in mlb?", p
            newname = alternateNames(p.upper(), pids)
            if newname is not None:
                #print "got it: ", newname
                pp['lookup_name'] = newname
                pp['player_id'] = encodedPlayerIds[str(pids[newname])]
                predictPlayers.append(pp)
            else:
                print "REALLY NOT FOUND.", p.upper()
        

    print "predictHitters=", predictHitters
    phRDD = sc.parallelize(predictHitters)
    phDF = sqlContext.createDataFrame(phRDD, samplingRatio=0.5)
    phDF.registerTempTable("fd_hitters")
    print "phDF=", phDF.take(2)
    
    print "predictPitchers=", predictPitchers
    ppRDD = sc.parallelize(predictPitchers)
    ppDF = sqlContext.createDataFrame(ppRDD, samplingRatio=0.5)
    ppDF.registerTempTable("fd_pitchers")
    print "ppDF=", ppDF.take(22)

    encodedHitterFeatures = sqlContext.parquetFile(rddDir + "/batting_features.enc.parquet")
    encodedHitterFeatures.registerTempTable("bfe")

    hfDF = sqlContext.sql("""select bfe.* from fd_hitters, bfe where
                            fd_hitters.player_id = bfe.player_id
                            and fd_hitters.game_date = bfe.game_date""")
    print "count hfdf=", hfDF.count()

    encodedPitchingFeatures = sqlContext.parquetFile(rddDir + "/pitching_features.enc.parquet")
    encodedPitchingFeatures.registerTempTable("pfe")
    print "pfe=", encodedPitchingFeatures.take(22)

    pfDF = sqlContext.sql("""select distinct pfe.* from fd_pitchers, pfe where
                            fd_pitchers.player_id = pfe.player_id
                            and fd_pitchers.game_date = pfe.game_date""")
    #TODO - why are we getting duplicate records?
    print "count pfdf=", pfDF.count()
    print "pfDF=", pfDF.collect()
    print "pfDF vals=", pfDF.select('player_id', 'game_id').collect()


    #model = RandomForestModel.load(sc, rddDir + "batting_model.RandomForest")
    #model = GradientBoostedTreesModel.load(sc, rddDir + "batting_model.RandomForest")
    model = GradientBoostedTreesModel.load(sc, rddDir + "batting_fd_points_model.RandomForest")
    playerAndPredictions = TrainModel.predictHitters(hfDF, decodedHitterPlayerIds, rddDir, sc)
    hitterFeatures = hfDF.collect()
#    global predictField
#    predictField = 'fd_points'
#    data = hfDF.map(toLabeledPoint)
#    predictions = model.predict(data.map(lambda x: x.features))
#    print "predictions=", predictions
#    print "predictions take=", predictions.take(2)
#    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions).cache()
#    print "predictions=", labelsAndPredictions.take(2)
#    playerAndPredictions = hfDF.map(lambda x: x.asDict()['player_id']).map(lambda x: decodePlayerIds[x]).zip(predictions).cache()
#    print "playerAndPredictions=", playerAndPredictions.take(2)
    
    #model = RandomForestModel.load(sc, rddDir + "pitching_model.RandomForest")
    #model = GradientBoostedTreesModel.load(sc, rddDir + "pitching_model.RandomForest")
    pitcherPredictions = TrainModel.predictPitchers(pfDF, decodedPitcherPlayerIds, rddDir, sc)
    pitcherFeatures = pfDF.collect()
#    data = pfDF.map(toLabeledPoint)
#    predictions = model.predict(data.map(lambda x: x.features))
#    print "p predictions=", predictions
#    print "p predictions take=", predictions.take(2)
#    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions).cache()
#    print "predictions=", labelsAndPredictions.take(2)
#    pitcherPredictions = pfDF.map(lambda x: x.asDict()['player_id']).map(lambda x: decodePlayerIds[x]).zip(predictions).cache()
#    print "pitcherPredictions=", pitcherPredictions.take(2)
    
    #Merge pitchers and hitters
    playerAndPredictions = playerAndPredictions.unionAll(pitcherPredictions).coalesce(8)
    print "playerAndPredictions count=", playerAndPredictions.count()
    print "playerAndPredictions=", playerAndPredictions.take(2)

    # build hash table of predicted points, to join back to eligible players
    predictedPoints = {}
    for x in playerAndPredictions.collect():
        print "x=", x
        if getattr(x, 'asDict', None) is not None:
            print "IN"
            z = x.asDict()
            predictedPoints[z['player_id']] = z['fd_points']
        else:
            predictedPoints[x[0]] = x[1]
    print "predictedPoints=", predictedPoints

    secondarySort = {}
    for x in hitterFeatures:
        p = x.asDict()
        #print "h=", p
        secondarySort[decodedHitterPlayerIds[p['player_id']]] = p['mv_7_std_fd_points']
    for x in pitcherFeatures:
        p = x.asDict()
        #print "p=", p
        secondarySort[decodedPitcherPlayerIds[p['player_id']]] = p['mv_30_std_fd_points']

    predicted = []
    nopredictions = []
    for p in predictHitters:
        print "hitter p=", p
        try:
            # hack to make secondary sort ascening even though sort is descending
            p['secondary_sort'] = secondarySort[decodedHitterPlayerIds[p['player_id']]] * -1.0
        except KeyError:
            print "NO PREDICTIONS: ", p
            p['secondary_sort'] = -1.0
            nopredictions.append(p)
        
        try:
            p['fd_points'] = predictedPoints[decodedHitterPlayerIds[p['player_id']]]
            print "predicted=", p['fd_points']
        except KeyError:
            # couldnt find any stats to predict against?
            print "NO PREDICTIONS: ", p
            p['fd_points'] = -99.0
            p['secondary_sort'] = -1.0
            nopredictions.append(p)
        predicted.append(p)

    for p in predictPitchers:
        print "pitcher p=", p
        try:
            # hack to make secondary sort ascening even though sort is descending
            p['secondary_sort'] = secondarySort[decodedPitcherPlayerIds[p['player_id']]] * -1.0
        except KeyError:
            print "NO PREDICTIONS: ", p
            p['secondary_sort'] = -1.0
            nopredictions.append(p)
        
        try:
            p['fd_points'] = predictedPoints[decodedPitcherPlayerIds[p['player_id']]]
            print "predicted=", p['fd_points']
        except KeyError:
            # couldnt find any stats to predict against?
            print "NO PREDICTIONS: ", p
            p['fd_points'] = -99.0
            p['secondary_sort'] = -1.0
            nopredictions.append(p)
        predicted.append(p)

    now = datetime.now(timezone('US/Central')).strftime('%Y%m%d.%H%M')
    with open(game.dataDirectory + '/no_predictions.out.' + now, 'w') as f:
        f.write(str(nopredictions) + '\n')
    
    s_predicted = sorted(predicted, key=lambda k: k['fd_points'], reverse=True)
    with open(game.dataDirectory + '/predictions.out.' + now, 'w') as f:
        for p in s_predicted:
            #f.write(str(predicted) + '\n')
            f.write('\t'.join(map(str,[p['position'], p['lookup_name'], p['salary'], p['fd_points'], p['secondary_sort'] ])) +'\n')
    
    print "predicted=", predicted
    optimal = OptimalSpark(game.dataDirectory, predicted)
    optimal.run()
    sc.stop()
