from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
#from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
#from pyspark.mllib.tree import RandomForest, RandomForestModel
#from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
#from pyspark.mllib.classification import LogisticRegressionWithSGD
import numpy as np
import os
import re
import shutil
import math
from CreateStatsRDD import CreateStatsRDD


not_features = ['fd_points', 'type', 'team_abbrev', 'stadium_venue_w_chan_loc', 'stadium_location', 'stadium_name', 'last_name', 'first_name', 'box_name', 'home_abbrev', 'away_abbrev', 'home_code', 'away_code', 'away_name', 'home_name', 'current_position', 'position', 'gameday_sw', 'game_id', 'game_pk', 'id', 'game_date', 'lookup_name', 'team_abbrev', 'status', 'is_starter', 'player_id' ]

pitcherPredictors = ['fd_points', 'win', 'earned_runs', 'strikeouts', 'innings_pitched']
pitcherPredictors = ['fd_points']
not_features.extend(['win', 'earned_runs', 'strikeouts', 'innings_pitched'])

hitterPredictors = ['fd_points', '_1b', '_2b', '_3b', 'hr', 'rbi', 'r', 'bb', 'sb', 'hbp', 'out']
hitterPredictors = ['fd_points']
not_features.extend(['_1b', '_2b', '_3b', 'hr', 'rbi', 'r', 'bb', 'sb', 'hbp', 'out'])

justFDPoints = len(hitterPredictors) == 1

def nan_to_zero(x):
    if x is None:
        return 0.0
    if math.isnan(float(x)):
        x = 0.0
    return x

def dict2vec(d):
    return np.array([nan_to_zero(d[k]) for k in sorted(d.keys()) if k not in not_features])

def toLabeledPoint(df, predictField):
    print "toLabeledPoint predictField=", predictField
    def _toLabeledPoint(x):
        #print "x=", x

        x = x.asDict()
        #print "inner predictField=", predictField
        #print "x=", x

        try:
            _ = x[predictField]
        except Exception:
            raise Exception("predictField=" + str(predictField) + ", x=" + str(x))
        if x[predictField] is None:
            x[predictField] = 0.0
        label = float(x[predictField])
        #for k in not_features:
        #    del x[k]
        #return LabeledPoint(label, x.values())
        return LabeledPoint(label, dict2vec(x))

    return df.map(_toLabeledPoint)

def getCatFeatures(sqlrdd, numVals):
    features = {}
    print "numVals=", numVals
    d = sqlrdd.take(1)[0].asDict()
    labels = [k for k in sorted(d.keys()) if k not in not_features]
    featureLookup = {}
    i = 0
    for label in labels:
        print str(i) + " label=", label
        if label in numVals:
            features[i] = numVals[label]
        featureLookup[i] = label
        i += 1
    print "features=", features
    return (features, featureLookup)
    
def populateDebugString(model, featureLookup):
    string = model.toDebugString()
    def repl(m):
        return featureLookup[int(m.group(2))]

    n = re.sub(r'(feature (\d+))', repl, string)
    print "n=", n

def renameSumFD(r):
    x = r.asDict()
    fd_sum = 0.0
    print "x=", x

    fd_sum = x['fd_points']
    x['fd_sum'] = fd_sum
    x['fd_points_orig'] = x['fd_points']
    x['fd_points'] = fd_sum
    print "now x=", x
    return Row(**x)

def sumFD(r):
    x = r.asDict()
    fd_sum = 0.0
    print "x=", x
    fd_sum += x.get('_1b', 0.0)
    fd_sum += x.get('bb', 0.0)
    fd_sum += 4.0 * x.get('hr', 0.0)
    fd_sum += 3.0 * x.get('_3b', 0.0)
    fd_sum += 2.0 * x.get('_2b', 0.0)
    fd_sum += x.get('r', 0.0)
    fd_sum += x.get('rbi', 0.0)
    fd_sum += 2.0 * x.get('sb', 0.0)
    fd_sum -= 0.25 * x.get('out', 0.0)

    fd_sum += 4.0 * x.get('win', 0.0)
    fd_sum -= x.get('earned_runs', 0.0)
    fd_sum += x.get('strikeouts', 0.0)
    fd_sum += x.get('innings_pitched', 0.0)

    x['fd_sum'] = fd_sum
    x['fd_points_orig'] = x['fd_points']
    x['fd_points'] = fd_sum
    print "now x=", x
    return Row(**x)

def trainTestSaveModel(rddDir, encodedFeaturesParq, featuresNumValsFile):
    return trainTestSaveFDPointsModel(rddDir, encodedFeaturesParq, featuresNumValsFile)

def trainTestSaveFDPointsModel(rddDir, encodedFeaturesParq, featuresNumValsFile):
    modelType = ""
    if "batting" in encodedFeaturesParq:
        modelType = 'batting'
    else:
        modelType = 'pitching'
    predictor = 'fd_points'
    not_features.extend(predictor)
    # Load and parse the data file.
    features = sqlContext.read.parquet(encodedFeaturesParq).cache()
    print features.take(3)
    print "# features=", features.count()
    numVals = sqlContext.read.json(featuresNumValsFile).take(1)[0].asDict()
    (catFeatures, featureLookup) = getCatFeatures(features, numVals)
    all_fd_points_df = None
    fd_points_testData = None
    predictions = None

    print "catFeatures=", catFeatures

    # Split the data into training and test sets (30% held out for testing)
    (f_trainingData, f_testData) = features.randomSplit([0.7, 0.3], seed=1)
    #trainingData = f_trainingData.map(toLabeledPoint).coalesce(50)
    trainingData = toLabeledPoint(f_trainingData, predictor).coalesce(50)
    #testData = f_testData.map(toLabeledPoint).coalesce(50)
    testData = toLabeledPoint(f_testData, predictor).coalesce(50)
    testData.cache()
    print "testData count=", testData.count()
    playerIds = f_testData.map(lambda x: str(x.player_id) + '_' + x.game_id).coalesce(50)
    print "playerIds=", playerIds
    print "playerIds=", playerIds.take(2)
    print "len playerIds=", playerIds.count()

    # Train a GradientBoostedTrees model.
    #  Notes: (a) Empty categoricalFeaturesInfo indicates all features are continuous.
    #         (b) Use more iterations in practice.
    model = GradientBoostedTrees.trainRegressor(trainingData, categoricalFeaturesInfo=catFeatures, maxDepth=5, numIterations=100, maxBins=300)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features)).cache()
    print "# predictions=", predictions.count()
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    fd_points_testData = f_testData.map(lambda x: (str(x.player_id) + '_' + x.game_id, x.fd_points or 0.0)).toDF(['player_id', 'actual_fd_points']).coalesce(50)

    testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(testData.count())
    testMAE = labelsAndPredictions.map(lambda (v, p): abs(v - p)).sum() / float(testData.count())
    print predictor + ' Test Mean Squared  Error = ' + str(testMSE)
    print predictor + ' Test Mean Absolute Error = ' + str(testMAE)

#    print " # playerIds=", playerIds.count()
#    all_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor]).alias('all_fd_points_df').cache()
#    print "FIRST ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
#    print "# all_fd_points_df", all_fd_points_df.count()
#    print "first all_fd_points_df", all_fd_points_df.take(5)
#    print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
    print "converted:"
    print populateDebugString(model, featureLookup)

    # Save and load model
    modelFilename = rddDir + "pitching_" + predictor + "_model.RandomForest"
    if modelType == "batting":
        modelFilename = rddDir + "batting_" + predictor + "_model.RandomForest"
    try:
        shutil.rmtree(modelFilename)
    except OSError:
        pass
    model.save(sc, modelFilename)

    fd_points_testData_filename = rddDir + modelType + '_' + 'fd_points_testData.csv'
    try:
        shutil.rmtree(fd_points_testData_filename)
    except OSError:
        pass
    fd_points_testData.write.format('com.databricks.spark.csv').option('header', 'true').save(fd_points_testData_filename)

def trainTestSaveALLModel(rddDir, encodedFeaturesParq, featuresNumValsFile):
    predictors = []
    modelType = ""
    if "batting" in encodedFeaturesParq:
        modelType = 'batting'
        predictors = hitterPredictors
    else:
        modelType = 'pitching'
        predictors = pitcherPredictors
    not_features.extend(predictors)
    # Load and parse the data file.
    features = sqlContext.read.parquet(encodedFeaturesParq).cache()
    print features.take(3)
    print "# features=", features.count()
    numVals = sqlContext.read.json(featuresNumValsFile).take(1)[0].asDict()
    (catFeatures, featureLookup) = getCatFeatures(features, numVals)
    all_fd_points_df = None
    fd_points_testData = None
    predictions = None
    for predictor in predictors:
        #global predictField
        #predictField = predictor
        #data = features.map(toLabeledPoint).coalesce(50)
        #data = toLabeledPoint(features, predictor).coalesce(50)
        #print "len data=", data.count()

        print "catFeatures=", catFeatures

        # Split the data into training and test sets (30% held out for testing)
        (f_trainingData, f_testData) = features.randomSplit([0.7, 0.3], seed=1)
        #trainingData = f_trainingData.map(toLabeledPoint).coalesce(50)
        trainingData = toLabeledPoint(f_trainingData, predictor).coalesce(50)
        #testData = f_testData.map(toLabeledPoint).coalesce(50)
        testData = toLabeledPoint(f_testData, predictor).coalesce(50)
        testData.cache()
        print "testData count=", testData.count()
        playerIds = f_testData.map(lambda x: str(x.player_id) + '_' + x.game_id).coalesce(50)
        print "playerIds=", playerIds
        print "playerIds=", playerIds.take(2)
        print "len playerIds=", playerIds.count()

        # Train a GradientBoostedTrees model.
        #  Notes: (a) Empty categoricalFeaturesInfo indicates all features are continuous.
        #         (b) Use more iterations in practice.
        model = GradientBoostedTrees.trainRegressor(trainingData, categoricalFeaturesInfo=catFeatures, maxDepth=5, numIterations=1, maxBins=300)

        # Evaluate model on test instances and compute test error
        predictions = model.predict(testData.map(lambda x: x.features)).cache()
        print "# predictions=", predictions.count()
        labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
        if fd_points_testData is None:
            fd_points_testData = f_testData.map(lambda x: (str(x.player_id) + '_' + x.game_id, x.fd_points)).toDF(['player_id', 'actual_fd_points']).coalesce(50)

        testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(testData.count())
        testMAE = labelsAndPredictions.map(lambda (v, p): abs(v - p)).sum() / float(testData.count())
        print predictor + ' Test Mean Squared  Error = ' + str(testMSE)
        print predictor + ' Test Mean Absolute Error = ' + str(testMAE)

        if all_fd_points_df is None:
            #all_fd_points_df = testData.map(lambda x: x.player_id).zip(predictions).toDF(['player_id', predictor]).cache()
            print "FIRST: # predictions=", predictions.count()
            print " # playerIds=", playerIds.count()
            all_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor]).alias('all_fd_points_df').cache()
            print "FIRST ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
            print "# all_fd_points_df", all_fd_points_df.count()
            print "first all_fd_points_df", all_fd_points_df.take(5)
            print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
        else:
            print "ELSE: # predictions=", predictions.count()
            print " # playerIds=", playerIds.count()
            curr_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor]).alias('curr_fd_points_df')
            print "all_fd_points_df", all_fd_points_df.printSchema()
            print "PRE all_fd_points_df", all_fd_points_df.take(5)
            print "curr_fd_points_df", curr_fd_points_df.printSchema()
            print "few curr_fd_points_df", curr_fd_points_df.take(5)
            print "# curr_fd_points_df", curr_fd_points_df.count()
            print "distinct curr_fd_points_df", curr_fd_points_df.select('player_id').distinct().count()
            print "first curr", curr_fd_points_df.take(5)
            #all_fd_points_df = all_fd_points_df.join(curr_fd_points_df, all_fd_points_df.player_id == curr_fd_points_df.player_id, 'inner').drop(curr_fd_points_df.player_id)
            all_fd_points_df = all_fd_points_df.join(curr_fd_points_df, col("all_fd_points_df.player_id") == col("curr_fd_points_df.player_id")).drop(curr_fd_points_df.player_id).alias('all_fd_points_df').cache()
            print "second ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
            #print "all debugstring", all_fd_points_df.rdd.toDebugString()
            #print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
        print "first few all_fd_points_df=", all_fd_points_df.take(3)
        print "count few all_fd_points_df=", all_fd_points_df.count()
        print "converted:"
        print populateDebugString(model, featureLookup)

        # Save and load model
        modelFilename = rddDir + "pitching_" + predictor + "_model.RandomForest"
        if modelType == "batting":
            modelFilename = rddDir + "batting_" + predictor + "_model.RandomForest"
        try:
            shutil.rmtree(modelFilename)
        except OSError:
            pass
        model.save(sc, modelFilename)
        #sameModel = GradientBoostedTreesModel.load(sc, "myModelPath")
    print "DONE. all_fd_points_df", all_fd_points_df.printSchema()
    print "# of all_fd_points=", all_fd_points_df.count()
    print "first of all_fd_points=", all_fd_points_df.take(5)

    try:
        shutil.rmtree(rddDir + 'all_fd_points_df.csv')
    except OSError:
        pass
    all_fd_points_df.write.format('com.databricks.spark.csv').save(rddDir + 'all_fd_points_df.csv')
    allPredictions = None
    if len(predictors) > 1:
        allPredictions = all_fd_points_df.map(sumFD).toDF()
    else:
        allPredictions = all_fd_points_df.map(renameSumFD).toDF()
        print allPredictions.rdd.toDebugString()
        print "predf allPredictions=", allPredictions.take(5)
        #allPredictions = allPredictions.toDF()
    try:
        shutil.rmtree(rddDir + 'allPredictions.csv')
    except OSError:
        pass
    allPredictions.write.format('com.databricks.spark.csv').save(rddDir + 'allPredictions.csv')
    print "allPredictions=", allPredictions.take(5)
    print "# of allPredictions=", allPredictions.count()
    predict_and_actuals = allPredictions.join(fd_points_testData, allPredictions.player_id == fd_points_testData.player_id).drop(fd_points_testData.player_id)
    print "predict_and_actuals=", predict_and_actuals.take(3)
    #labelsAndPredictions = all_fd_points_df.map(lambda x: x.fd_points).zip(allPredictions).cache()
    labelsAndPredictions = predict_and_actuals
    print "labelsAndPredictions=", labelsAndPredictions.take(3)
    def mse(x):
        r = x.asDict()
        if r['actual_fd_points'] is None:
            r['actual_fd_points'] = 0.0
        return (r['actual_fd_points'] - r['fd_sum']) * (r['actual_fd_points'] - r['fd_sum'])

    def mae(x):
        r = x.asDict()
        if r['actual_fd_points'] is None:
            r['actual_fd_points'] = 0.0
        return abs(r['actual_fd_points'] - r['fd_sum'])
    testMSE = labelsAndPredictions.map(mse).sum() / float(allPredictions.count())
    testMAE = labelsAndPredictions.map(mae).sum() / float(allPredictions.count())
    print 'Merged ' + modelType + ' Test Mean Squared  Error = ' + str(testMSE)
    print 'Merged ' + modelType + ' Test Mean Absolute Error = ' + str(testMAE)

def getFDPointsPredictions(pfDF, decodePlayerIds, rddDir, sc, pitching_hitter, predictors):
    all_fd_points_df = None
    playerIds = pfDF.map(lambda x: x.player_id).map(lambda x: decodePlayerIds[x])
    print "playerIds=", playerIds.collect()
    predictor = 'fd_points'
    print "predictor=", predictor
    modelFilename = rddDir + pitching_hitter + "_" + predictor + "_model.RandomForest"
    model = GradientBoostedTreesModel.load(sc, modelFilename)
    data = toLabeledPoint(pfDF, predictor)
    predictions = model.predict(data.map(lambda x: x.features))
    print "p predictions=", predictions
    print "p predictions take=", predictions.take(16)
    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions).cache()
    print "labelsAndPredictions=", labelsAndPredictions.take(16)
    print " # playerIds=", playerIds.count()
    all_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor])
    print "FIRST ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
    print "# all_fd_points_df", all_fd_points_df.count()
    print "first all_fd_points_df", all_fd_points_df.take(5)
    print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
    print pitching_hitter + " predictions=", all_fd_points_df.take(50)
    return all_fd_points_df


def getPredictions(pfDF, decodePlayerIds, rddDir, sc, pitching_hitter, predictors):
    all_fd_points_df = None
    #playerIds = pfDF.map(lambda x: str(x.player_id) + '_' + x.game_id)
    playerIds = pfDF.map(lambda x: x.player_id).map(lambda x: decodePlayerIds[x])
    print "playerIds=", playerIds.collect()
    for predictor in predictors:
        print "predictor=", predictor
        #modelFilename=rddDir + "pitching_" + predictor + "_model.RandomForest"
        modelFilename = rddDir + pitching_hitter + "_" + predictor + "_model.RandomForest"
        model = GradientBoostedTreesModel.load(sc, modelFilename)
        data = toLabeledPoint(pfDF, predictor)
        #pitcherFeatures = pfDF.collect()
        predictions = model.predict(data.map(lambda x: x.features))
        print "p predictions=", predictions
        print "p predictions take=", predictions.take(16)
        labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions).cache()
        print "predictions=", labelsAndPredictions.take(16)
        #pitcherPredictions = pfDF.map(lambda x: x.asDict()['player_id']).map(lambda x: decodePlayerIds[x]).zip(predictions).cache()
        #print "pitcherPredictions=", pitcherPredictions.take(16)
        if all_fd_points_df is None:
            #all_fd_points_df = testData.map(lambda x: x.player_id).zip(predictions).toDF(['player_id', predictor]).cache()
            print "FIRST: # predictions=", predictions.count()
            print " # playerIds=", playerIds.count()
            all_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor])
            print "FIRST ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
            print "# all_fd_points_df", all_fd_points_df.count()
            print "first all_fd_points_df", all_fd_points_df.take(5)
            print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
        else:
            print "ELSE: # predictions=", predictions.count()
            print " # playerIds=", playerIds.count()
            curr_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor])
            print "all_fd_points_df", all_fd_points_df.printSchema()
            print "PRE all_fd_points_df", all_fd_points_df.take(16)
            print "curr_fd_points_df", curr_fd_points_df.printSchema()
            print "# curr_fd_points_df", curr_fd_points_df.count()
            #print "distinct curr_fd_points_df", curr_fd_points_df.select('player_id').distinct().count()
            print "first curr", curr_fd_points_df.take(16)
            all_fd_points_df = all_fd_points_df.join(curr_fd_points_df, all_fd_points_df.player_id == curr_fd_points_df.player_id, 'inner').drop(curr_fd_points_df.player_id)
            print "second ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
            #print "all debugstring", all_fd_points_df.rdd.toDebugString()
            print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
#    def sumFD(r):
#        x = r.asDict()
#        fd_sum = 0.0
#        for k,v in x.iteritems():
#            if k not in ['fd_points', 'player_id']:
#                fd_sum += v
#        x['fd_sum'] = fd_sum
#        x['fd_points_orig'] = x['fd_points']
#        x['fd_points'] = fd_sum
#        print "sumx=", x
#        return Row(**x)

    predictions = all_fd_points_df.map(sumFD)
    print pitching_hitter + " predictions=", predictions.take(50)
    return predictions

def predictHitters(hfDF, decodePlayerIds, rddDir, sc):

    return getFDPointsPredictions(hfDF, decodePlayerIds, rddDir, sc, 'batting', hitterPredictors)
#    predictField = 'fd_points'
#    data = toLabeledPoint(hfDF, predictField)
#    #hitterFeatures = hfDF.collect()
#    predictions = model.predict(data.map(lambda x: x.features))
#    print "predictions=", predictions
#    print "predictions take=", predictions.take(2)
#    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions).cache()
#    print "predictions=", labelsAndPredictions.take(2)
#    playerAndPredictions = hfDF.map(lambda x: x.asDict()['player_id']).map(lambda x: decodePlayerIds[x]).zip(predictions).cache()
#    print "playerAndPredictions=", playerAndPredictions.take(2)
#    return playerAndPredictions

def predictPitchers(pfDF, decodePlayerIds, rddDir, sc):
    return getFDPointsPredictions(pfDF, decodePlayerIds, rddDir, sc, 'pitching', pitcherPredictors)
#    all_fd_points_df = None
#    #playerIds = pfDF.map(lambda x: str(x.player_id) + '_' + x.game_id)
#    playerIds = pfDF.map(lambda x: x.player_id).map(lambda x: decodePlayerIds[x])
#    print "playerIds=", playerIds.collect()
#    for predictor in pitcherPredictors:
#        print "predictor=", predictor
#        modelFilename = rddDir + "pitching_" + predictor + "_model.RandomForest"
#        model = GradientBoostedTreesModel.load(sc, modelFilename)
#        data = toLabeledPoint(pfDF, predictor)
#        #pitcherFeatures = pfDF.collect()
#        predictions = model.predict(data.map(lambda x: x.features))
#        print "p predictions=", predictions
#        print "p predictions take=", predictions.take(16)
#        labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions).cache()
#        print "predictions=", labelsAndPredictions.take(16)
#        #pitcherPredictions = pfDF.map(lambda x: x.asDict()['player_id']).map(lambda x: decodePlayerIds[x]).zip(predictions).cache()
#        #print "pitcherPredictions=", pitcherPredictions.take(16)
#        if all_fd_points_df is None:
#            #all_fd_points_df = testData.map(lambda x: x.player_id).zip(predictions).toDF(['player_id', predictor]).cache()
#            print "FIRST: # predictions=", predictions.count()
#            print " # playerIds=", playerIds.count()
#            all_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor])
#            print "FIRST ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
#            print "# all_fd_points_df", all_fd_points_df.count()
#            print "first all_fd_points_df", all_fd_points_df.take(5)
#            print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
#        else:
#            print "ELSE: # predictions=", predictions.count()
#            print " # playerIds=", playerIds.count()
#            curr_fd_points_df = playerIds.zip(predictions).toDF(['player_id', predictor])
#            print "all_fd_points_df", all_fd_points_df.printSchema()
#            print "PRE all_fd_points_df", all_fd_points_df.take(16)
#            print "curr_fd_points_df", curr_fd_points_df.printSchema()
#            print "curr_fd_points_df", curr_fd_points_df.printSchema()
#            print "# curr_fd_points_df", curr_fd_points_df.count()
#            print "distinct curr_fd_points_df", curr_fd_points_df.select('player_id').distinct().count()
#            print "first curr", curr_fd_points_df.take(16)
#            all_fd_points_df = all_fd_points_df.join(curr_fd_points_df, all_fd_points_df.player_id == curr_fd_points_df.player_id, 'inner').drop(curr_fd_points_df.player_id)
#            print "second ALL_FD_POINTS_DF", all_fd_points_df.printSchema()
#            #print "all debugstring", all_fd_points_df.rdd.toDebugString()
#            print "distinct all_fd_points_df", all_fd_points_df.select('player_id').distinct().count()
##    def sumFD(r):
##        x = r.asDict()
##        fd_sum = 0.0
##        for k,v in x.iteritems():
##            if k not in ['fd_points', 'player_id']:
##                fd_sum += v
##        x['fd_sum'] = fd_sum
##        x['fd_points_orig'] = x['fd_points']
##        x['fd_points'] = fd_sum
##        print "sumx=", x
##        return Row(**x)
#        
#    predictions = all_fd_points_df.map(sumFD)
#    print "predictPitchers predictions=", predictions.take(50)
#    return predictions

if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    rddDir = CreateStatsRDD.rddDir
    batting_features = rddDir + "/" + "batting_features.enc.parquet"
    batting_numVals = rddDir + "/" + "batting_features.numvals.json"
    pitching_features = rddDir + "/" + "pitching_features.enc.parquet"
    pitching_numVals = rddDir + "/" + "pitching_features.numvals.json"

    print "training batting:"
    trainTestSaveModel(rddDir, batting_features, batting_numVals)

    print "training pitching:"
    not_features.append('fd_position')
    trainTestSaveModel(rddDir, pitching_features, pitching_numVals)
    sc.stop()
