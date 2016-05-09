import json
from datetime import datetime
import xml.etree.ElementTree as ET
import sys
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType, DoubleType, ByteType, ShortType, BooleanType
from AbstractDF import AbstractDF, RDDBuilder

class PitcherStats(AbstractDF):
    schema = StructType( sorted (
        [
        StructField("game_id", StringType(), False),
        StructField("game_date", DateType(), False),
        StructField("player_id", IntegerType(), False),
        StructField("team_id", StringType(), False),
        StructField("team_abbrev", StringType(), False),
        StructField("name_display_first_last", StringType(), False),
        StructField("name", StringType(), False),
        StructField("win", ByteType(), False),
        StructField("earned_runs", ByteType(), False),
        StructField("strikeouts", ByteType(), False),
        StructField("innings_pitched", DoubleType(), False),
        StructField("hr", ByteType(), False),
        StructField("num_pitches", ShortType(), False),
        StructField("num_strikes", ShortType(), False),
        StructField("num_walks", ByteType(), False),
        StructField("num_hits", ByteType(), False),
        StructField("num_runs", ByteType(), False),
        StructField("fd_points", DoubleType(), False),
        StructField("is_starter", BooleanType(), False),
        StructField("batters_faced", ShortType(), False),
        StructField("non_win_fd_points", DoubleType(), False),
        StructField("modified", TimestampType()),
        ],
    key = lambda x: x.name))
    skipSelectFields = ['game_id', 'modified', 'game_date']


class PitchingStats(RDDBuilder):
    table_name = "pitching_stats"
    schema_obj = PitcherStats()

    def __init__(self, gameDir):
        RDDBuilder.__init__(self, gameDir, 'boxscore.json')

    def transformPosition(self, pos):
        # transform positions
        if pos in ['SP', 'RP']:
            pos = 'P'
        elif pos in ['LF', 'CF', 'RF']:
            pos = 'OF'
        elif pos in ['DH']:
            pos = '1B'
        return pos

    def getPlayers(self):
        f = open(self.full_file)
        self.data = json.load(f)
        f.close()

        homeAway = self.data['data']['boxscore']['pitching']
   
        for ha in homeAway:
            #print "ha=", ha
            pitcherList = ha['pitcher']
            team_flag = ha['team_flag']
            if isinstance( pitcherList, dict):
                pitcherList = [ pitcherList ]
            pitcherCount = 0
            for pitcher in pitcherList:
                #print "pitcher=", pitcher

                ps = PitcherStats()
                ps.game_id = self.gameId
                ps.game_date = self.gameDate
                ps.player_id = pitcher['id']
                if team_flag == 'home':
                    ps.team_id = self.data['data']['boxscore']['home_id']
                    ps.team_abbrev = self.data['data']['boxscore']['home_team_code']
                else:
                    ps.team_id = self.data['data']['boxscore']['away_id']
                    ps.team_abbrev = self.data['data']['boxscore']['away_team_code']
                ps.name_display_first_last = pitcher['name_display_first_last']
                ps.name = pitcher['name']
                ps.win = 0
                if 'win' in pitcher and pitcher['win'] == 'true':
                    ps.win = 1
                ps.earned_runs = int(pitcher['er'])
                ps.strikeouts = int(pitcher['so'])
                ps.innings_pitched = float(pitcher['out']) / 3.0
                ps.hr = pitcher['hr']
                if 'np' in pitcher:
                    ps.num_pitches = pitcher['np']
                else:
                    ps.num_pitches = 0
                if 's' in pitcher:
                    ps.num_strikes = pitcher['s']
                else:
                    ps.num_strikes = 0
                ps.num_walks = pitcher['bb']
                ps.save = pitcher['sv']
                ps.num_hits = pitcher['h']
                ps.num_runs = pitcher['r']
                ps.fd_points = 0.0
                if ps.win == 1:
                    ps.fd_points += 12.0
                ps.fd_points += 3.0 * ps.innings_pitched
                ps.fd_points -= 3.0 * int(ps.earned_runs)
                ps.fd_points += 3.0 * int(ps.strikeouts)
                if pitcherCount == 0:
                    ps.is_starter = True
                else:
                    ps.is_starter = False
                pitcherCount += 1
                ps.batters_faced = pitcher['bf']
                ps.non_win_fd_points = -1 * ps.earned_runs + ps.strikeouts + ps.innings_pitched
                ps.modified = datetime.now()

                if int(ps.num_pitches) > 0:
                    yield ps

    @staticmethod
    def getRows(gameDir):
        rows = []
        pitchers = PitchingStats(gameDir)
        if pitchers.is_postponed:
            return rows
        
        for player in pitchers.getPlayers():
            rows.append(player.createRow())

        return rows
