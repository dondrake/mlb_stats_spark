import json
from datetime import datetime, date, timedelta
import xml.etree.ElementTree as ET
import sys
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType, DoubleType
from AbstractDF import AbstractDF, RDDBuilder

class GamePlayer(AbstractDF):
    schema = StructType( sorted (
        [
        StructField("game_date", DateType()),
        StructField("team_abbrev", StringType()),
        StructField("player_id", IntegerType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("box_name", StringType()),
        StructField("lookup_name", StringType()),
        StructField("rl", StringType()),
        StructField("bats", StringType()),
        StructField("position", StringType()),
        StructField("fd_position", StringType()),
        StructField("status", StringType()),
        StructField("team_id", IntegerType()),
        StructField("effective_start_dt", DateType()),
        StructField("effective_stop_dt", DateType()),
        StructField("modified", TimestampType()),
        ],
    key = lambda x: x.name))
    skipSelectFields = [ 'game_date', 'player_id', 'modified', 'lookup_name', 'effective_start_dt', 'effective_stop_dt', ]


class GamePlayers(RDDBuilder):
    table_name = "game_players"
    schema_obj = GamePlayer()

    def __init__(self, gameDir):
        RDDBuilder.__init__(self, gameDir, 'players.xml')

    def transformPosition(self, pos):
        # transform positions
        if pos in ['SP', 'RP']:
            pos = 'P'
        elif pos in ['LF', 'CF', 'RF']:
            pos = 'OF'
        elif pos in ['DH']:
            pos = '1B'
        return pos

    def fieldAsNone(self, attrib, field):
        val = None
        try:
            val = attrib[field]
        except KeyError:
            pass
        return val

    def createGamePlayer(self, player, teamAbbrev):

        p = GamePlayer()
        p.game_date = self.gameDate
        p.player_id = player.attrib['id']
        try:
            p.team_abbrev = player.attrib['team_abbrev']
        except Exception:
            p.team_abbrev = teamAbbrev
            #raise Exception("player="+ str(player.attrib) + ' '+ self.full_file)
        p.first_name = player.attrib['first']
        p.last_name = player.attrib['last']
        p.box_name = player.attrib['boxname']
        p.lookup_name = player.attrib['first'] + ' ' + player.attrib['last'] + '_' + p.team_abbrev
        p.rl = player.attrib['rl']
        # sometimes missing
        p.bats = player.attrib.get('bats', 'R')
        p.position = player.attrib['position']
        if p.position == '':
            p.position = 'OF'
        p.fd_position = self.transformPosition(p.position)
        #p.current_position = self.fieldAsNone(player.attrib,'current_position')
        p.status = player.attrib['status']
        try:
            p.team_id = player.attrib['team_id']
        except Exception:
            p.team_id = None
        p.effective_start_dt = p.game_date
        p.effective_stop_dt = p.game_date + timedelta(days=1)
        p.modified = datetime.now()

        return p

    def getPlayers(self):
        tree = ET.parse(self.full_file)
        root = tree.getroot()
        homeTeam = root.findall("./team[@type='home']")[0]
        teamAbbrev = homeTeam.get("id")
        players = homeTeam.findall("./player")
        for player in players:
            #print "player=", player
            p = self.createGamePlayer(player, teamAbbrev)
            yield p
        awayTeam = root.findall("./team[@type='away']")[0]
        players = awayTeam.findall("./player")
        teamAbbrev = awayTeam.get("id")
        for player in players:
            #print "player=", player
            p = self.createGamePlayer(player, teamAbbrev)
            yield p

    @staticmethod
    def getRows(gameDir):
        rows = []
        gps = GamePlayers(gameDir)
        if gps.is_postponed:
            return rows
        
        for player in gps.getPlayers():
            rows.append(player.createRow())

        return rows

    # create a slow changing dimension rdd from GamePlayers
    @staticmethod
    def createSCD(rdd):
        def getKeys(x):
            return (x.player_id, x)
        
        def createRows(someTuple):
            (key, iterable) = someTuple
            gameList = sorted(iterable, key = lambda x: x.game_date)
            previousGame = None
            startDate = None

            for r in gameList:
                g = r.asDict()
                #print "g=", g
                if previousGame is None:
                    previousGame = g.copy()
                    previousGame['effective_start_dt'] = g['game_date']
                    previousGame['effective_stop_dt'] = g['game_date'] + timedelta(days=1)
                    startDate = g['game_date']
                if g['bats'] != previousGame['bats'] or g['rl'] != previousGame['rl'] or g['team_id'] != previousGame['team_id'] or g['status'] != previousGame['status']:
                    previousGame['effective_start_dt'] = startDate
                    previousGame['effective_stop_dt'] = g['game_date']
                    g['effective_start_dt'] = g['game_date']
                    #print "bats=", g['bats'] != previousGame['bats']
                    #print "rl=", g['rl'] != previousGame['rl']
                    #print "teamid=", g['team_id'] != previousGame['team_id']
                    #print "status=", g['status'] != previousGame['status']
                    #print "Yielding! g=", g
                    #print "Yielding! p=", previousGame
                    yield Row(**previousGame)
                    startDate = g['game_date']
                previousGame = g.copy()

            if g['effective_stop_dt'] != date(9999,1,1):
                g['effective_start_dt'] = startDate
                g['effective_stop_dt'] = date(9999,1,1)
                print "Yielding LAST ", g
                yield Row(**g)
            

        return rdd.map(getKeys).groupByKey(16).flatMap(createRows)
