import json
from pytz import timezone
from datetime import datetime, timedelta
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType
from AbstractDF import AbstractDF, RDDBuilder


class GameEvent(AbstractDF):
    schema = StructType( sorted (
        [
        StructField("game_id", StringType(), False),
        StructField("game_date", DateType(), False),
        StructField("inning", IntegerType(), False),
        StructField("top_bottom", StringType(), False),
        StructField("fd_batter_event", StringType(), False),
        StructField("des", StringType(), False),
        StructField("balls", IntegerType(), False),
        StructField("strikes", IntegerType(), False),
        StructField("outs", IntegerType(), False),
        StructField("pitcher", IntegerType(), False),
        StructField("batter", IntegerType(), False),
        StructField("num", IntegerType(), False),
        StructField("start_tfs_zulu", TimestampType(), False),
        StructField("modified", TimestampType(), False),
        ], 
    key = lambda x: x.name))

class GameEvents(RDDBuilder):
    table_name = "game_events"
    schema_obj = GameEvent()

    def __init__(self, gameDir):
        RDDBuilder.__init__(self, gameDir, 'game_events.json')
        if self.gameId is None:
            raise Exception("gameId cannot be None")
        self.events=[]

    def FanDuelEvent(self, event):
        e = event['event']
        if e in set([ 'Groundout', 'Lineout', 'Flyout', 'Force Out', 'Forceout', 'Grounded Into DP', 'Pop Out', 'Double Play', 'Batter Interference', 'Bunt Groundout', 'Bunt Pop Out', 'Triple Play', 'Bunt Lineout']):
            return 'O'
        elif e == 'Single':
            return 'H'
        elif e == 'Strikeout' or e == 'Strikeout - DP':
            return 'SO'
        elif e == 'Walk' or e == 'Intent Walk':
            return 'W'
        elif e == 'Home Run':
            return 'HR'
        elif e == 'Double':
            return '2B'
        elif e == 'Triple':
            return '3B'
        elif e == 'Field Error' or e == 'Error':
            return 'E'
        elif e == 'Sac Fly' or e == 'Sac Bunt' or e == 'Sac Fly DP' or e == 'Sacrifice Bunt DP':
            return 'SAC'
        elif e == 'Hit By Pitch':
            return 'HBP'
        elif e == 'Wild Pitch':
            return 'WP'
        elif e == 'Fielders Choice':
            return 'FC'
        elif e == 'Fielders Choice Out':
            return 'FC'
        elif e == 'Balk':
            return 'BALK'
        elif e.startswith('Stolen Base'):
            return 'SB'
        elif e == 'Fan interference' or e == 'Fan Interference':
            if 'double' in event['des']:
                return '2B'
            elif 'triples' in event['des']:
                return '3B'
            elif 'out' in event['des']:
                return 'O'
            elif 'throwing error' in event['des']:
                return 'E'
            elif 'fielding error' in event['des']:
                return 'E'
            elif 'singles' in event['des']:
                return 'H'
            else:
                print "Unhandles event", e, event['des']
        print "Unhandles event:", e
        return None

    def createGameEvent(self, inning, topBottom, event):
        #print  event['event']
        ge = GameEvent()
        #ge.id = event['num']
        ge.game_id = self.gameId
        #print "game_id=", self.gameId
        ge.game_date = self.gameDate
        ge.inning = inning
        ge.top_bottom = topBottom
        ge.event = event['event']
        ge.fd_batter_event = self.FanDuelEvent(event)
        ge.des = event['des']
        try:
            ge.balls = event['b']
        except Exception:
            ge.balls = 0
        try:
            ge.strikes = event['s']
        except Exception:
            ge.strikes = 0
        ge.outs = event['o']
        ge.pitcher = event['pitcher']
        ge.batter = event['batter']
        ge.num = event['num']
        # 2015-04-14T00:08:57Z
        #sometimes blank?
        # http://gd2.mlb.com/components/game/mlb/year_2015/month_03/day_21/gid_2015_03_21_lanmlb_texmlb_1/game_events.json
        if event['start_tfs_zulu'] == '':
            ge.start_tfs_zulu = None
        else:
            ge.start_tfs_zulu = datetime.strptime(event['start_tfs_zulu'],'%Y-%m-%dT%H:%M:%SZ')
        ge.modified = datetime.now()

        #print "insert", ge
        return ge

    def getEvents(self):
        if self.is_postponed:
            return 
        f = open(self.full_file)
        self.data = json.load(f)
        f.close()
        if isinstance( self.data['data']['game']['inning'], dict):
            # game never started, postponed due to rain
            return
        for inning in self.data['data']['game']['inning']:
            inningNum = inning['num']
            #print "Inning:", inningNum
            for tb in ['top', 'bottom']:
                # bottom of the ninth not always there....
                if tb not in inning or 'atbat' not in inning[tb]:
                    continue
                #print "tb=", tb
                #print "NOW=", inning[tb]

                # bottom of ninth, walkoff homerun at top of order makes this a dict.
                atbat = inning[tb]['atbat']
                #print "atbat=", atbat
                if isinstance(atbat, dict):
                    atbat = [ atbat ]
                    #print "NOW atbat=", atbat
                for event in atbat:
                    #print "event=", event
                    if event['event'] == 'Runner Out':
                        # this is a pickoff, doesn't count as an at bat
                        continue
                    if event['event'] == 'Catcher Interference':
                        # this doesn't count as an at bat
                        # http://www.baseball-reference.com/bullpen/Catcher's_interference
                        continue
                    self.events.append(event)
                    yield self.createGameEvent(inningNum, tb, event)
                if 'action' in inning[tb]:
                    action = inning[tb]['action']
                    # action can be a dictionary or a list of events
                    if isinstance(action, dict):
                        action = [ action ]
                    for event in action:
                        if event['event'] == 'Game Advisory':
                            continue
                        elif event['event'].startswith('Pickoff') or event['event'].startswith('Picked off'):
                            continue
                        elif event['event'] == 'Pitching Substitution':
                            continue
                        elif event['event'] == 'Defensive Sub':
                            continue
                        elif event['event'] == 'Defensive Switch':
                            continue
                        elif event['event'] == 'Defensive Indiff':
                            # it' not a stolen base, so skip it
                            continue
                        elif event['event'] == 'Passed Ball':
                            continue
                        elif event['event'] == 'Offensive sub' or event['event'] == 'Offensive Sub':
                            continue
                        elif event['event'] == 'Ejection':
                            continue
                        elif event['event'] == 'Player Injured':
                            continue
                        elif event['event'].startswith('Caught Stealing'):
                            continue
                        elif event['event'] == 'Runner Out':
                            continue
                        elif event['event'] == 'Runner Advance':
                            continue
                        elif event['event'] == 'Umpire Substitution':
                            continue
                        elif event['event'] == 'Batter Turn':
                            continue
                        elif event['event'] == 'Pitcher Switch':
                            continue
                        elif event['event'] == 'Manager Review':
                            print "? Manager Review", event['des']
                            continue
                        elif event['event'] == 'Umpire Review':
                            print "? Umpire Review", event['des']
                            continue
                        elif event['event'] == 'Base Running Double Play':
                            continue
                        event['num'] = -1
                        if event['event'] == 'Wild Pitch': 
                            event['pitcher'] = event['player']
                            event['batter'] = -1
                        else:
                            event['pitcher'] = -1
                            event['batter'] = event['player']
                        event['start_tfs_zulu'] = event['tfs_zulu']
                        self.events.append(event)
                        yield self.createGameEvent(inningNum, tb, event)

    @staticmethod
    def getRows(gameDir):
        rows = []
        ges = GameEvents(gameDir)
        if ges.is_postponed:
            return rows
        events = ges.getEvents()

        for event in events:
            #print "event=", events
            rows.append(event.createRow())

        return rows
