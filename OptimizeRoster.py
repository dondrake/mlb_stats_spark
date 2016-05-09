from getLineups import MLB
from itertools import combinations, groupby
from collections import defaultdict
from operator import itemgetter
from datetime import datetime,timedelta
import json
import copy
from RosterRank import RosterRank, Roster

class OptimizeRoster(object):

    positions = ['P','C','1B','2B','3B','SS','OF']

    def __init__(self, dataDirectory, eligiblePlayers):
        
        self.dataDirectory = dataDirectory
        self.eligiblePlayers = eligiblePlayers
        self.statsColumn = 'points'
        self.statsRoster = {}
        self.positionPlayers = {}
        self.preCalcPos = {}
        self.rRank = None

    def makePlayerKey(self, name, team):
        return name + '_' + team

    def getPlayerStats(self):
        
        filename = self.dataDirectory + '/fd.players.json'
        f = open(filename)
        data = json.load(f)
        f.close()

        mlb = MLB(self.dataDirectory)
        self.statsRoster = {}

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
            player[self.statsColumn] = fields[6]
            player['num_games'] = fields[7]
            player['some_boolean'] = fields[8]
            player['player_status'] = fields[9]
            player['injury'] = fields[10]

            player['team'] = mlb.getTeamFromFanduelTeamId(player['team_id'])
            print player

            self.statsRoster[self.makePlayerKey(player['name'], player['team'])] = player

    def buildPositionPlayers(self):

        for pos in self.positions:
            self.positionPlayers[pos] = []

        # go through the players that are eligible for this set of games
        # see if we have stats for them, if so, we add them to positionPlayers[]
        for p in self.eligiblePlayers:
            if self.makePlayerKey(p['name'], p['team']) in self.statsRoster:
                player = self.statsRoster[self.makePlayerKey(p['name'], p['team'])]
                self.positionPlayers[p['position']].append(player)
            else:
                # default value
                p[self.statsColumn] = 0.0
                self.positionPlayers[p['position']].append(p)
            

    def optimizePositionPlayers(self):
        # remove duplicate players with same salary and varying points
        # sort descending and then take the first one in the groupby
        # decreases search space by a few orders of magnitude
        for pos in self.positions:
            self.positionPlayers[pos] = sorted(self.positionPlayers[pos], key=itemgetter('salary',self.statsColumn,'secondary_sort'), reverse=True )
            print "pos:", pos, len(self.positionPlayers[pos])
            newList = []
            for k,g in groupby(self.positionPlayers[pos], itemgetter('salary')):
                print "k=",k, "g=",g
                if pos == 'OF':
                    count = 1
                    # Add this salary, we could have 3 OF's with a decent score. Instead of just one 
                    for val in g:
                        #print "adding val=", val
                        newList.append(val)
                        if count > 3:
                            break
                        count += 1
                else:
                    val = g.next()
                    #print "val=", val
                    newList.append(val)
                    #for i in g:
                    #    print "\trest=", i
            self.positionPlayers[pos] = newList


    def preCalcPositions(self):
        tempRoster = {}
        for ss in self.positionPlayers['SS']:
            for players in combinations(self.positionPlayers['OF'], 3):
                # Add to tuple
                players += (ss,)
                #print "players=", players
                #(totalCost, totalPoints) = self.getRosterTotals(players)
                roster = Roster(self.statsColumn, list(players))
                if roster.totalCost in tempRoster and  roster.totalPoints < tempRoster[roster.totalCost].totalPoints:
                        tempRoster[roster.totalCost].numSkipped += 1
                        #print "SKIPPING totalCost ", totalCost, " < ", totalPoints, " ? ",tempRoster[totalCost]
                        continue
                if roster.totalCost not in tempRoster:
                    tempRoster[roster.totalCost] = roster
                    #print "NEW ADDED roster=", roster
                elif roster.totalPoints > tempRoster[roster.totalCost].totalPoints:
                    roster.numSkipped = tempRoster[roster.totalCost].numSkipped + 1
                    tempRoster[roster.totalCost] = roster
                    #print "MORE POINTS ADDED roster=", roster
                else:
                    tempRoster[roster.totalCost].numSkipped += 1
                    #print "SKIPPED NOT ENOUGH POINTS"
        print "len tempRoster=", len(tempRoster)
        #print "tempRoster=", tempRoster
        self.preCalcPos = tempRoster

    def bruteForce(self):
        print "# of players:"
        grandTotalCombinations = 1
        for pos in self.positions:
            self.positionPlayers[pos] = sorted(self.positionPlayers[pos], key=lambda k: k[self.statsColumn], reverse=True)
            print "pos:", pos, len(self.positionPlayers[pos])
            if pos != 'OF':
                grandTotalCombinations *= len(self.positionPlayers[pos])

        print "OF combinations:", len(list(combinations(self.positionPlayers['OF'], 3)))
        grandTotalCombinations *= len(list(combinations(self.positionPlayers['OF'], 3)))
        print "Total Combinations: {:,}".format(grandTotalCombinations)


        self.rRank = RosterRank(self.__class__.__name__, grandTotalCombinations)
        startTime=datetime.now()
        print "start=", startTime
        #preCalcPos = preCalcPositions(positionPlayers)
        numPitcher=0
        numCatcher=0
        numFirstBase=0
        numSecondBase=0
        numThirdBase=0
        numShortstop=0
        totalCombinations=0
        numSkipped=0
        illegalLineup=0
        rosters = []
        zrosters = []
        salaryCap = 35000
        rosterScoreHist = defaultdict(int)
        zrosterScoreHist = defaultdict(int)
        #positions = ['P','C','1B','2B','3B','SS','OF'];
        for pitcher in self.positionPlayers['P']:
            numPitcher += 1
            roster = [ pitcher ]
            for catcher in self.positionPlayers['C']:
                numCatcher += 1
                roster.append(catcher)
                for firstbase in self.positionPlayers['1B']:
                    numFirstBase += 1
                    roster.append(firstbase)
                    for secondbase in self.positionPlayers['2B']:
                        numSecondBase += 1
                        roster.append(secondbase)
                        for thirdbase in self.positionPlayers['3B']:
                            numThirdBase += 1
                            roster.append(thirdbase)
                            #for players in combinations(positionPlayers['OF'], 3):
                            numPreCalc = 0
                            for key, rosterDict in self.preCalcPos.items():
                                rosterObj = rosterDict.copy()
                                rosterObj.extend(roster)
                                self.rRank.addRoster(rosterObj)
                                totalCombinations += 1

                                if (totalCombinations % 2000000) == 0:
                                    #zrosters= sorted(zrosters, key=lambda k: k['zscore'], reverse=True)[:1000]
                                    print "numPitcher=", numPitcher, "numCatcher=", numCatcher
                            roster.pop()
                        roster.pop()
                    roster.pop()
                roster.pop()
            roster.pop()

        self.rRank.printStats()
        self.rRank.saveStats(self.dataDirectory)

    def run(self):
        self.getPlayerStats()
        self.buildPositionPlayers()
        self.optimizePositionPlayers()
        self.preCalcPositions()
        self.bruteForce()
