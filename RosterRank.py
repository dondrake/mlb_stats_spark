from getLineups import MLB
from itertools import combinations, groupby
from collections import defaultdict
from operator import itemgetter
from datetime import datetime,timedelta
from pytz import timezone
import json
import copy
import sys

class Roster(object):

    positions = ['P','C','1B','2B','3B','SS','OF']
    salaryCap = 35000

    def __init__(self, statsColumn, roster):
        self.statsColumn = statsColumn
        self.roster = copy.copy(roster)
        self.totalCost = 0
        self.totalPoints = 0.0
        self.numSkipped = 0
        self.hasIllegalRoster = False
        self.isOverSalaryCap = False
    
        self.computeStats()

    def computeStats(self):
        self.totalCost = 0
        self.totalPoints = 0.0
        self.hasIllegalRoster = False
        self.isOverSalaryCap = False

        #calc stats
        teamCount = defaultdict(int)
        for player in self.roster:
            self.totalCost += player['salary']
            self.totalPoints += player[self.statsColumn]

            # See if we have an illegal lineup
            # Can't have more than 4 players from same team
            teamCount[player['team']] += 1
            if teamCount[player['team']] > 4:
                #self.numSkipped += 1
                #self.hasIllegalLineup += 1
                self.hasIllegalRoster = True

        if self.totalCost > self.salaryCap:
            self.isOverSalaryCap = True

    def extend(self, players): 
        self.roster.extend(players)
        self.computeStats()

    def append(self, player): 
        self.roster.append(player)
        self.computeStats()

    def copy(self):
        roster = Roster(self.statsColumn, [])
        roster.roster = copy.copy(self.roster)
        roster.totalCost = self.totalCost
        roster.totalPoints = self.totalPoints
        roster.numSkipped = self.numSkipped
        roster.hasIllegalRoster = self.hasIllegalRoster
        roster.isOverSalaryCap = self.isOverSalaryCap
        return roster

    def __repr__(self):
        print "Salary: {0:4.2f}, Points: {1:4.2f} # Skipped: {2:,d}".format(self.totalCost, self.totalPoints, self.numSkipped)
        fields = ['position', 'team', 'lookup_name', 'salary', self.statsColumn, 'secondary_sort']
        print "\t".join(fields)
        for pos in self.positions:
            for player in self.roster:
                if player['position'] == pos:
                    print "\t".join(map(str, [player[f] for f in fields]))
        return ''

class RosterRank(object):

    maxRosters = 20
    minRosterCheck = 1000000

    def __init__(self, name, grandTotalCombinations):
        
        self.name = name
        self.grandTotalCombinations = grandTotalCombinations
        self.histogram = defaultdict(int)
        self.rosters = []
    
        self.numSkipped = 0
        self.hasIllegalLineup = 0

        self.numAdded = 0
        self.totalCombinations = 0
        self.startTime = datetime.now()

    def addRoster(self, roster):
        self.numAdded += 1
        self.totalCombinations += 1
        self.totalCombinations += roster.numSkipped
        self.numSkipped += roster.numSkipped

        if (self.numAdded % self.minRosterCheck) == 0:
            self.printStats()

        if roster.isOverSalaryCap:
            return
        if roster.hasIllegalRoster:
            self.hasIllegalLineup += 1
            #print "ILLEGAL:", roster)
            return

        self.histogram['{0:4.2f}'.format(roster.totalPoints)] +=1
        self.rosters.append(roster)

    def sortRosters(self):
        self.rosters = sorted(self.rosters, key=lambda k: k.totalPoints, reverse=True)[:30]
        self.histogram = defaultdict(int, sorted(self.histogram.items(),key=lambda k: float(k[0]), reverse=True)[:30])

    def printStats(self):
        self.sortRosters()
        delta = datetime.now() - self.startTime
        opsPerSec = self.totalCombinations / delta.total_seconds()
        secondsLeft = (self.grandTotalCombinations - self.totalCombinations) / opsPerSec
        formattedTimeLeft = str(timedelta(seconds=secondsLeft))
        print "started at", self.startTime, "duration=", delta
        print "totalCombinations= {:,}".format(self.totalCombinations), "numSkipped={:,}".format(self.numSkipped), "GrandTotal: {0:,}, {1:4.4f}".format(self.grandTotalCombinations, self.totalCombinations/float(self.grandTotalCombinations)*100.0), "ops/sec= {:4,.2f}".format(opsPerSec), "time left=", formattedTimeLeft, "illegalLineup=", self.hasIllegalLineup
        print '# found:', len(self.rosters)
        print "numSkipped={:,}".format(self.numSkipped)
        print "numIllegal={:,}".format(self.hasIllegalLineup)
        print "numAdded={:,}".format(self.numAdded)
        if len(self.rosters) > 0:
            print self.name, "ROSTER_RANK roster:"
            for i in range(min(10, len(self.rosters))):
                print "RANK:", i
                print self.rosters[i]
                print ""
            print "hist=", sorted(self.histogram.items(),key=lambda k: float(k[0]), reverse=True)

    def saveStats(self, dataDirectory):
        origSTDOUT = sys.stdout

        now = datetime.now(timezone('US/Central')).strftime('%Y%m%d.%H%M')
        filename = dataDirectory + '/results.' + self.name +'.txt.' + now
        f = open(filename, 'w')
        sys.stdout = f

        self.printStats()
        f.close()
        sys.stdout = origSTDOUT

        #filename = dataDirectory + '/results.' + self.name + '.json.' + now
        #f = open(filename, 'w')
        #json.dump(self.rosters[0].__dict__, f)
        #f.close()

        if self.name == 'FanDuelActualScoreModel':
            myrosters = []
            myrosters = [x.__dict__ for x in self.rosters]
            filename = dataDirectory + '/results.' + self.name +'.allrosters.txt' 
            f = open(filename, 'w')
            json.dump(myrosters, f)
            f.close()
