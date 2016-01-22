from datetime import datetime
import xml.etree.ElementTree as ET
import json
import re
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType
from AbstractDF import AbstractDF, RDDBuilder

class Game(AbstractDF):
    schema = StructType( sorted(
        [
        StructField("game_id", StringType()),
        StructField("game_date", DateType()),
        StructField("game_year", IntegerType()),
        StructField("game_type", StringType()),
        StructField("home_time", StringType()),
        StructField("game_pk", IntegerType()),
        StructField("gameday_sw", StringType()),
        StructField("game_time_et", TimestampType()),
        StructField("home_code", StringType()),
        StructField("home_abbrev", StringType()),
        StructField("home_name", StringType()),
        StructField("home_won", IntegerType()),
        StructField("home_loss", IntegerType()),
        StructField("home_division", StringType()),
        StructField("home_league", StringType()),
        StructField("home_team_id", IntegerType()),
        StructField("away_code", StringType()),
        StructField("away_abbrev", StringType()),
        StructField("away_name", StringType()),
        StructField("away_won", IntegerType()),
        StructField("away_loss", IntegerType()),
        StructField("away_division", StringType()),
        StructField("away_league", StringType()),
        StructField("away_team_id", IntegerType()),
        StructField("stadium_id", IntegerType()),
        StructField("stadium_name", StringType()),
        StructField("stadium_venue_w_chan_loc", StringType()),
        StructField("stadium_location", StringType()),
        StructField("modified", TimestampType()),
        ],
    key = lambda x: x.name))
    skipSelectFields = ['game_type', 'modified']

class Games(RDDBuilder):
    table_name = "games"
    schema_obj = Game()
    def __init__(self, filename):
        RDDBuilder.__init__(self, filename, 'linescore.json')

    def getGame(self):

        if self.is_postponed:
            print "postponed: ", self.gameId
            return None
        with open(self.full_file, 'r') as f:
            data = json.load(f)

        data = data['data']['game']
        #print "data=", data
        # remove playoffs and pre-season
        if data['game_type'] != 'R':
            return None
        if data['home_team_name'] == 'TBD' or data['time'] == 'TBD':
            return None
        g = Game()
        g.game_id = self.gameId
        g.game_date = self.gameDate
        g.game_year = self.gameDate.year
        g.game_type = data['game_type']
        g.home_time = data['home_time']
        g.game_pk = data['game_pk']
        g.gameday_sw = data['gameday_sw']
        if ':' not in data['time']:
            # double headers don't have a time, just make one up
            g.game_time_et = datetime(self.gameDate.year, self.gameDate.month, self.gameDate.day, 19, 0, 0, 0)
        else:
            (hour, minute) = data['time'].split(':')
            hour = int(hour)
            min = int(minute)
            if data['ampm'] == 'PM':
                hour += 12
                hour = hour % 24
            g.game_time_et = datetime(self.gameDate.year, self.gameDate.month, self.gameDate.day, hour, min, 0, 0)
        g.home_code = data['home_code']
        g.home_abbrev = data['home_code'].upper()
        g.home_name = data['home_team_city']
        g.home_won = data['home_win']
        g.home_loss = data['home_loss']
        g.home_division = data['home_division']
        g.home_league = data['home_league_id']
        g.home_team_id = data['home_team_id']

        g.away_code = data['away_code']
        g.away_abbrev = data['away_code'].upper()
        g.away_name = data['away_team_city']
        g.away_won = data['away_win']
        g.away_loss = data['away_loss']
        g.away_division = data['away_division']
        g.away_league = data['away_league_id']
        g.away_team_id = data['away_team_id']

        g.stadium_id = data['venue_id']
        g.stadium_name = data['venue']
        try:
            g.stadium_venue_w_chan_loc = data['venue_w_chan_loc']
        except Exception:
            # Sydney Australia, no weather codes
            pass
        g.stadium_location = data.get('location', '')
        g.modified = datetime.now()

        return g

    def getGame_OLD(self):

        if self.is_postponed:
            print "postponed: ", self.gameId
            return None
        tree = ET.parse(self.full_file)
        root = tree.getroot()
        print "root=", root
        g = Game()
        g.game_id = self.gameId
        g.game_date = self.gameDate
        g.id = root.attrib['game_pk']
        g.type = root.attrib['type']
        g.local_game_time = root.attrib['local_game_time']
        g.game_pk = root.attrib['game_pk']
        g.gameday_sw = root.attrib['gameday_sw']
        m = re.match('(\d\d):(\d\d) (..)', root.attrib['game_time_et'])
        (hour, min, ampm) = m.group(1,2,3)
        hour = int(hour)
        min = int(min)
        if ampm == 'PM':
            hour += 12
            hour = hour % 24
        #game.game_time_et = datetime(gameDate.year, gameDate.month, gameDate.day, hour, min, 0, 0, timezone('US/Eastern'))
        g.game_time_et = datetime(self.gameDate.year, self.gameDate.month, self.gameDate.day, hour, min, 0, 0)
        homeTeam = root.findall("./team[@type='home']")[0]
        print "home=", homeTeam
        g.home_code = homeTeam.attrib['code']
        g.home_abbrev = homeTeam.attrib['abbrev']
        g.home_name = homeTeam.attrib['name']
        g.home_name_full = homeTeam.attrib['name_full']
        g.home_won = homeTeam.attrib['w']
        g.home_loss = homeTeam.attrib['l']
        g.home_division_id = homeTeam.attrib['division_id']
        g.home_league = homeTeam.attrib['league']
        g.home_league = homeTeam.attrib['league']

        awayTeam = root.findall("./team[@type='away']")[0]
        print "away=", awayTeam
        g.away_code = awayTeam.attrib['code']
        g.away_abbrev = awayTeam.attrib['abbrev']
        g.away_name = awayTeam.attrib['name']
        g.away_name_full = awayTeam.attrib['name_full']
        g.away_won = awayTeam.attrib['w']
        g.away_loss = awayTeam.attrib['l']
        g.away_division_id = awayTeam.attrib['division_id']
        g.away_league = awayTeam.attrib['league']
        g.away_league = awayTeam.attrib['league']

        stadium = root.findall("./stadium")[0]
        g.stadium_id = stadium.attrib['id']
        g.stadium_name = stadium.attrib['name']
        g.stadium_venue_w_chan_loc = stadium.attrib['venue_w_chan_loc']
        g.stadium_location = stadium.attrib['location']
        g.modified = datetime.now()
        print "stadium=", stadium

        return g

    @staticmethod
    def getRows(gameDir):
        g = Games(gameDir).getGame()
        print "game=", g
        if g is not None:
            return [g.createRow()]
        else:
            return []

