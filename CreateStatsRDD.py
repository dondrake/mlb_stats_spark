#!/usr/bin/env python2.7

from datetime import datetime, timedelta, date
from pytz import timezone
from urlparse import urljoin
import argparse
import scrape
import os
import logging
import re
import sys
import shutil
from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext, Row
from GamePlayers import GamePlayers
from GameEvents import GameEvents
from Game import Games, Game
from BattingStats import BattingStats
from PitchingStats import PitchingStats, PitcherStats

def dump(x):
    print x

class CreateStatsRDD(object):

    cache = os.path.expanduser('~') + '/spark_mlb/spark.mlb.com/gd2.mlb.com/components/game/mlb'
    rddDir = os.path.expanduser('~') + '/spark_mlb/spark.mlb.com/rdd/' 

    def __init__(self, begin=None, end=None):
        self.begin=begin
        self.end=end
        self.log = scrape.setup_logging(ns="gd", filename="scraper.log")
        print "cache=", self.cache
        if not os.path.exists(self.cache):
            print "directory doesn't exist? ", self.cache
            sys.exit(1)

        if not os.path.exists(self.rddDir):
            os.mkdir(self.rddDir)

        yesterday = datetime.now(timezone('US/Central')) - timedelta(days=1)
        yesterday = yesterday.replace(tzinfo=None).date()
        today = datetime.now(timezone('US/Central')).replace(tzinfo=None).date()
        print "yesterday=", yesterday
        print "today=", today
        if self.begin is None and self.end is None:
            # default to yesterday
            start = urljoin(self.cache, scrape.datetime_to_url(yesterday))
            self.startDate = yesterday
            stop = urljoin(self.cache, scrape.datetime_to_url(yesterday))
            self.stopDate = today
        elif self.begin is None and self.end is not None:
            print "-b is required."
            sys.exit()
        else:
            self.startDate = datetime.strptime(self.begin, '%Y%m%d').date()
            self.start = urljoin(self.cache,
                            scrape.datetime_to_url(self.startDate))

            if self.end is None:
                stop = urljoin(self.cache, scrape.datetime_to_url(today))
                self.stopDate = today
            else:
                self.stopDate = datetime.strptime(self.end, '%Y%m%d').date()
                stop = urljoin(self.cache, scrape.datetime_to_url(self.stopDate))


    def getYearsMonths(self):
        print "starting."
        for x in scrape.filesystem_scraper([self.cache], "year"):
            print "x=",x, self.startDate
            m= re.search('/year_(\d{4})/?', x)
            if not m:
                print "no year present", x
                continue
            year = int(m.group(1))
            if not (year >= self.startDate.year and year <= self.stopDate.year):
                print "skipping"
                continue
            for y in scrape.filesystem_scraper([x], "month"):
                print "y=", y
                m= re.search('/month_(\d{2})/?', y)
                if not m:
                    continue
                month = int(m.group(1))
                for z in scrape.filesystem_scraper([y], "day"):
                    print "z=", z
                    m= re.search('/day_(\d{2})/?', z)
                    if not m:
                        continue
                    day = int(m.group(1))
                    if day == 0:
                        print "DAY == 0", z
                        continue
                    urlDate = date(year, month, day)
                    print urlDate, self.startDate, self.stopDate
                    if (urlDate >= self.startDate and urlDate <= self.stopDate):
                        print "got one:", z
                        yield z

    def rmtree(self, dir):
        try:
            shutil.rmtree(dir)
        except OSError: 
            pass


    def saveRDD(self, sqlContext, rdd, tableName, schema):
        parquetFile = self.rddDir + "/" + tableName + ".parquet"
        jsonFile = self.rddDir + "/" + tableName + ".json"
        self.rmtree(parquetFile)
        self.rmtree(jsonFile)
        print "saving tableName"
        print "schema=", schema
        sqlrdd = sqlContext.createDataFrame(rdd, schema)
        sqlrdd.registerTempTable(tableName)
        #sqlrdd.save(parquetFile)
        print "saving parquet " + parquetFile
        sqlrdd.write.save(parquetFile, format="parquet")
        #sqlrdd.save(jsonFile, "json")
        print "saving json " + jsonFile
        sqlrdd.write.save(jsonFile, format="json")

    def createRawParquet(self, sc, sqlContext, gamesRDD):
        tables = [Games, GameEvents, GamePlayers, PitchingStats]
        #tables = [GamePlayers]
        counts = []
        for table in tables:

            rdd = gamesRDD.flatMap(table.getRows)
            rdd.cache()
            if table == GamePlayers:
                rdd = GamePlayers.createSCD(rdd)
            accum = rdd.count()

            print "table=", table
            print "table=", rdd.take(1)[0]
            self.saveRDD(sqlContext, rdd, table.table_name, table.schema_obj.schema)
            counts.append((table.table_name, accum))
            print rdd.toDebugString()
        print "counts=", counts

    def createHitterStats(self, sqlContext):
        # create Hitter Stats
        gameEvents = sqlContext.read.parquet(self.rddDir + "/" + GameEvents.table_name + ".parquet")
        gameEvents.registerTempTable("game_events")
        gameEvents.cache()
        print "gameEvents=", gameEvents

        games = sqlContext.read.parquet(self.rddDir + "/" + Games.table_name + ".parquet")
        games.registerTempTable("games")
        games.cache()
        print "games=", gameEvents

        e = gameEvents.map(BattingStats.mapEventstoBattingStats)
        print "map count=", e.count()
        e = e.reduceByKey(BattingStats.reduceByKey).values()
        print "e=", e
        print "e=", e.take(3)
        print "reduce count=", e.count()

        schema_e = sqlContext.createDataFrame(e)
        schema_e.registerTempTable("batter_stats")
        schema_e.cache()

        batter_games = schema_e.join(games, schema_e.game_id == games.game_id)
        unique_cols = []
        unique_cols.extend(Game().getSelectFields(games))
        unique_cols.extend([schema_e[name] for name in schema_e.columns if (name != 'game_id') ])
        batter_games = batter_games.select(*unique_cols)
        print "batter_games=", batter_games
        print "first batter_games=", batter_games.take(3)
        # save 
        self.rmtree(self.rddDir + "/" + "batter_games.parquet")
        batter_games.save(self.rddDir + "/" + "batter_games.parquet")
        self.rmtree(self.rddDir + "/" + "batter_games.json")
        batter_games.save(self.rddDir + "/" + "batter_games.json", "json")

        return batter_games

    def createPitcherStats(self, sqlContext):
        games = sqlContext.read.parquet(self.rddDir + "/" + Games.table_name + ".parquet")
        games.registerTempTable("games")
        games.cache()
        print "games=", games

        pitching_stats = sqlContext.read.parquet(self.rddDir + "/" + PitchingStats.table_name + ".parquet") 
        pitching_stats.registerTempTable("pitching_stats")
        pitching_stats.cache()

        pitching_games = pitching_stats.join(games, pitching_stats.game_id == games.game_id)
        unique_cols = []
        unique_cols.extend(PitcherStats().getSelectFields(pitching_stats))
        print "uniqueCols=", unique_cols
        unique_cols.extend(Game().getSelectFields(games))
        print "now uniqueCols=", unique_cols
    
        pitching_games = pitching_games.select(*unique_cols)
        self.rmtree(self.rddDir + "/" + "pitcher_games.parquet")
        pitching_games.save(self.rddDir + "/" + "pitcher_games.parquet")
        self.rmtree(self.rddDir + "/" + "pitcher_games.json")
        pitching_games.save(self.rddDir + "/" + "pitcher_games.json", "json")

    def run(self):

        sc = SparkContext()
        sqlContext = SQLContext(sc)
        #sqlContext = HiveContext(sc)

        start_scrape = datetime.now()
        begin, begin_parts = scrape.get_boundary(self.begin)
        end, end_parts = scrape.get_boundary(self.end)

        print "here"
        all_years_months_days = self.getYearsMonths()
        print "all_years=", all_years_months_days

        game_ids = scrape.get_games(all_years_months_days, source=scrape.filesystem_scraper)
        print "games=", game_ids

        gamesRDD = sc.parallelize(game_ids)
        gamesRDD.cache()
        print "fileRDD=", gamesRDD

        print "# parttions:", gamesRDD.getNumPartitions()
        print "count=", gamesRDD.count()

        # create RDDs
        self.createRawParquet(sc, sqlContext, gamesRDD)
    
        # Hitter Stats
        batter_games = self.createHitterStats(sqlContext)

        # create Pitcher Stats
        self.createPitcherStats(sqlContext)
        
        print "STOPPING"
        sc.stop()
    

def get_args():
    """Return command line arguments as parsed by argparse."""
    parser = argparse.ArgumentParser(description="blah blah blah")
    parser.add_argument("-b", "--begin", dest="begin", type=str,
                        help="Beginning date in Y-m-d format")
    parser.add_argument("-e", "--end", dest="end", type=str,
                        help="Ending date in Y-m-d format")
    parser.add_argument("-c", "--cache", dest="cache", type=str,
                        help="Local cache directory", default=None)
    parser.add_argument("-d", "--daemon", dest="daemon", action="store_true",
                        default=False, help="Run %(prog)s as a daemon.")

    return parser.parse_args()


if __name__ == '__main__':

    args = get_args()
    print "args=", args
    x = CreateStatsRDD(args.begin, args.end)
    x.run()
