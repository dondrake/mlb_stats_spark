#!/usr/bin/env python

import forecastio
import json
import os
import pytz
import shutil
from pytz import timezone
from datetime import datetime
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext, Row, HiveContext
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType, DoubleType, ByteType, ShortType, BooleanType
from CreateStatsRDD import CreateStatsRDD
from Game import Games, Game
from AbstractDF import AbstractDF

SRCDIR = os.path.dirname(os.path.realpath(__file__))

class Weather(AbstractDF):
    schema = StructType( sorted (
        [
        StructField("game_id", StringType(), False),
        StructField("game_time_et", TimestampType(), False),
        StructField("icon", StringType(), False),
        StructField("nearestStormDistance", StringType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("precipIntensity", DoubleType(), False),
        StructField("precipProbability", DoubleType(), False),
        StructField("precipType", StringType(), False),
        StructField("apparentTemperature", DoubleType(), False),
        StructField("dewPoint", DoubleType(), False),
        StructField("windSpeed", DoubleType(), False),
        StructField("windBearing", DoubleType(), False),
        StructField("cloudCover", DoubleType(), False),
        StructField("humidity", DoubleType(), False),
        StructField("pressure", DoubleType(), False),
        StructField("visibility", DoubleType(), False),
        StructField("ozone", DoubleType(), False),
        StructField("blowing_in", BooleanType(), False),
        StructField("blowing_out", BooleanType(), False),
        ],
    key = lambda x: x.name))

    skipSelectFields = ['game_id', 'modified', 'game_time_et']


class UpdateWeather(object):
    # get your API key by registering here: https://developer.forecast.io/
    _api_key = ''

    weatherFile = CreateStatsRDD.rddDir + "/" + "weather.json"

    domeVal = {
        'dewPoint': 0.1,
        'humidity': 0.3,
        'temperature': 70.0,
        'apparentTemperature': 70.0,
        'pressure': 1000,
        'windSpeed': 0.0,
        'cloudCover': 0.0,
        'precipIntensity': 0.0,
        'precipProbability': 0.0,
        'precipType': '',
        'nearestStormDistance': '1000',
        'ozone': 310.0,
        'icon': 'dome_clear',
        'visibility': 100.0,
        'windBearing': 0.0,
        'blowing_in': False,
        'blowing_out': False,
    }

    def __init__(self, sparkContext, sqlContext, games, stadium):
        self.games = games
        self.stadium = stadium
        self.sqlContext = sqlContext
        self.sparkContext = sparkContext

        if self._api_key == '':
            raise Exception("_api_key is not set, cannot run.")

        self.weatherExists = os.path.exists(self.weatherFile)
        
        if self.weatherExists:
            self.weather = sqlContext.jsonFile(self.weatherFile, schema=Weather.schema)
            #temp filter
            #self.weather = self.weather.filter(self.weather["game_time_et"] < '2015-06-20 00:00:00')
            #self.weather = self.weather.filter(self.weather["game_id"] != 'gid_2015_06_15_chamlb_pitmlb_1')
            self.weather.registerTempTable("weather")
            self.weather.cache()
        else:
            self.weather = sqlContext.createDataFrame(sc.emptyRDD(), Weather.schema)
            self.weather.registerTempTable("weather")
            self.weather.cache()

    def getGamesToUpdate(self):
        if self.weatherExists:
            #order by g.game_time_et desc  \
            games = self.sqlContext.sql("select distinct * from (select g.game_id, g.game_time_et, s.latitude, s.longitude, s.has_dome, s.center_azimuth  \
                from games g join stadium s ON (g.stadium_id = s.stadium_id) \
                left outer join weather w on (g.game_id = w.game_id) \
                where  w.game_id is null \
                UNION ALL\
                select g.game_id, g.game_time_et, s.latitude, s.longitude, s.has_dome, s.center_azimuth  \
                from games g join stadium s ON (g.stadium_id = s.stadium_id) \
                WHERE g.game_time_et > from_unixtime(unix_timestamp()) \
                and g.game_date = to_date(from_unixtime(unix_timestamp())) \
                ) as w \
                ").collect()
        else:
            games = self.sqlContext.sql("select g.game_id, g.game_time_et, s.latitude, s.longitude, s.has_dome, s.center_azimuth \
                from games g, stadium s  \
                where g.stadium_id = s.stadium_id \
                order by g.game_time_et desc \
                limit 10").collect()
        return games
       
    def saveWeather(self, updates):
        rows = []
        if self.weatherExists:
            # go through rdd
            gameIds = []
            for w in updates:
                gameIds.append(w.game_id)

            def removeOld(x):
                x = x.asDict()
                print "checking x=", x
                print "is in =", (x['game_id'] not in gameIds)
                return x['game_id'] not in gameIds

            rows = self.weather.rdd.filter(removeOld).collect()
            #print "rows=", rows
            for w in updates:
                rows.append(w.createRow())
        else:
            # create rdd of rows
            rows = []
            for w in updates:
                rows.append(w.createRow())
        #print "now rows =", rows
        rdd = self.sparkContext.parallelize(rows)
        print "rddROW = ", rdd.take(2)
        df = self.sqlContext.createDataFrame(rdd, Weather.schema)
        try:
            shutil.rmtree(self.weatherFile)
        except OSError:
            pass
        df.save(self.weatherFile, 'json')

    def fixDir(self, az):
        if az < 0:
            return az + 360
        if az > 360:
            return az - 360
        return az

    def isBlowingOut(self, direction, azimuth):
        buff = 10
        lower = self.fixDir(azimuth - 45 - buff)
        upper = self.fixDir(azimuth + 45 + buff)
        if direction >= lower and direction <= upper:
            return True
        else:
            return False

    def isBlowingIn(self, direction, azimuth):
        return self.isBlowingOut(self.fixDir(direction  - 180), azimuth)

    def update(self):
        
        games = self.getGamesToUpdate()
        print "games=", games
        eastern = timezone('US/Eastern')
        fmt = '%Y-%m-%d %H:%M:%S %Z%z'
        
        updates = []
        for game in games:
            w = Weather()
            w.game_id = game.game_id
            w.game_time_et = game.game_time_et
            print "game=", game
            time = eastern.localize(game.game_time_et)
            print "time=", time
            print "fmt =", time.strftime(fmt)
            print "time=", time
            if int(game.has_dome) > 0:
                w.icon = self.domeVal['icon']
                w.temperature = self.domeVal['temperature']
                w.nearestStormDistance = self.domeVal['nearestStormDistance']
                w.precipIntensity = self.domeVal['precipIntensity']
                w.precipProbability = self.domeVal['precipProbability']
                w.precipType = self.domeVal['precipType']
                w.apparentTemperature = self.domeVal['apparentTemperature']
                w.dewPoint = self.domeVal['dewPoint']
                w.windSpeed = self.domeVal['windSpeed']
                w.windBearing = self.domeVal['windBearing']
                w.cloudCover = self.domeVal['cloudCover']
                w.humidity = self.domeVal['humidity']
                w.pressure = self.domeVal['pressure']
                w.visibility = self.domeVal['visibility']
                w.blowing_in = self.domeVal['blowing_in']
                w.blowing_out = self.domeVal['blowing_out']
                try:
                    w.ozone = val.ozone
                except Exception:
                    w.ozone = self.domeVal['ozone']
            else:
                val = forecastio.load_forecast(self._api_key, game.latitude, game.longitude, time, units='us').currently()
                print "val=", val
                w.icon = val.icon
                w.temperature = val.temperature
                try:
                    w.nearestStormDistance = val.nearestStormDistance
                except Exception:
                    w.nearestStormDistance = self.domeVal['nearestStormDistance']
                try:
                    w.precipIntensity = val.precipIntensity
                except Exception:
                    w.precipIntensity = self.domeVal['precipIntensity']
                try:
                    w.precipProbability = val.precipProbability
                except Exception:
                    w.precipProbability = self.domeVal['precipProbability']
                try:
                    w.precipType = val.precipType
                except Exception:
                    w.precipType = self.domeVal['precipType']
                try:
                    w.apparentTemperature = val.apparentTemperature
                except Exception:
                    w.apparentTemperature = self.domeVal['apparentTemperature']
                try:
                    w.dewPoint = val.dewPoint
                except Exception:
                    w.dewPoint = self.domeVal['dewPoint']
                try:
                    w.windSpeed = val.windSpeed
                except Exception:
                    w.windSpeed = self.domeVal['windSpeed']
                try:
                    w.windBearing = float(val.windBearing)
                except Exception:
                    w.windBearing = self.domeVal['windBearing']
                try:
                    w.cloudCover = val.cloudCover
                except Exception:
                    w.cloudCover = self.domeVal['cloudCover']
                try:
                    w.humidity = val.humidity
                except Exception:
                    w.humidity = self.domeVal['humidity']
                try:
                    w.pressure = val.pressure
                except Exception:
                    w.pressure = self.domeVal['pressure']
                try:
                    w.visibility = val.visibility
                except Exception:
                    w.visibility = self.domeVal['visibility']
                w.blowing_in = self.isBlowingIn(w.windBearing, float(game.center_azimuth))
                w.blowing_out = self.isBlowingOut(w.windBearing, float(game.center_azimuth))
                try:
                    w.ozone = val.ozone
                except Exception:
                    w.ozone = self.domeVal['ozone']
                print "w=", w
            updates.append(w)

        self.saveWeather(updates)
        print "updates=", updates

if __name__ == '__main__':
    print "Starting.", datetime.now()
    sc = SparkContext()
    #sqlContext = SQLContext(sc)
    sqlContext = HiveContext(sc)

    games = sqlContext.parquetFile(CreateStatsRDD.rddDir + "/" + Games.table_name + ".parquet")
    games.registerTempTable("games")
    games.cache()
    print "games=", games
    print games.take(2)

    stadium = sqlContext.load(source="com.databricks.spark.csv", header="true", path = SRCDIR + "/stadium.csv")
    stadium.registerTempTable("stadium")
    stadium.cache()
    print "stadium=", stadium.take(2)
    
    weather = UpdateWeather(sc, sqlContext, games, stadium)
    weather.update()

    badIds = sqlContext.sql("select game_id, count(*) from weather group by game_id having count(*) >1").collect()
    if len(badIds) > 0:
        print "BAD weather game_ids=", badIds
    else:
        print "no bad weather ids."
    sc.stop()
