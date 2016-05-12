#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from BeautifulSoup import BeautifulSoup
import re
import collections
import datetime
import random
import json
from itertools import groupby, count
import os.path
from base64 import b64decode, b64encode
#import jellyfish  # similar players.
from operator import itemgetter  # similar players.
import logging
import requests
from urlparse import urlparse
import sys
import time
from getopt import getopt
import re
import codecs

class MLB(object):
    
    # modified abbreviations to match ESPN
    FanDualTeamIdToName = {
        "594":"TB","614":"PIT","613":"MIL","609":"WSH","617":"COL","615":"STL","620":"SF","619":"SD","591":"BAL","596":"CHW","610":"CHC","611":"CIN","608":"PHI","606":"MIA","595":"TOR","593":"NYY","607":"NYM","602":"OAK","604":"TEX","598":"DET","599":"KC","618":"LAD","612":"HOU","605":"ATL","616":"ARI","597":"CLE","601":"LAA","600":"MIN","603":"SEA","592":"BOS"
    }

    # fancy swap
    FanDuelTeamNametoID = dict(zip(FanDualTeamIdToName.values(), FanDualTeamIdToName.keys()))

    def __init__(self, sqlContext, cacheDir, gameDate, isReadOnly = True):
        self.sqlContext = sqlContext
        self._mlbdb = os.path.abspath(os.path.dirname(__file__)) + '/csv/'
        self.log = logging.getLogger(__name__)
        self.cacheDir = cacheDir
        self.gameDate = gameDate
        self.isReadOnly = isReadOnly
        mlb = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(self._mlbdb + "mlb.csv").cache()
        print "mlb=", mlb
        print "mlbshow=", mlb.show()
        mlb.registerTempTable("mlb")
        mlb_alias = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(self._mlbdb + "mlbteamaliases.csv").cache()
        mlb_alias.registerTempTable("mlbteamaliases")
        print "mlba=", mlb_alias
        print "mlbashow=", mlb_alias.show()
        self._initTeamHash()
    
    def _initTeamHash(self):
        self.teamAlias = {}
        aliasRow = self.sqlContext.sql("SELECT team, teamalias FROM mlbteamaliases union all select team, team from mlb").collect()
        for row in aliasRow:
            self.teamAlias[row[1]] = row[0]
        print "teamALias=", self.teamAlias
        

    def _splicegen(self, maxchars, stringlist):
        """Return a group of splices from a list based on the maxchars
        string-length boundary.
        """
    
        runningcount = 0
        tmpslice = []
        for i, item in enumerate(stringlist):
            runningcount += len(item)
            if runningcount <= int(maxchars):
                tmpslice.append(i)
            else:
                yield tmpslice
                tmpslice = [i]
                runningcount = len(item)
        yield(tmpslice)
    
    def _batch(self, iterable, size):
        """http://code.activestate.com/recipes/303279/#c7"""
    
        c = count()
        for k, g in groupby(iterable, lambda x:c.next()//size):
            yield g
    
    def _validate(self, date, format):
        """Return true or false for valid date based on format."""
    
        try:
            datetime.datetime.strptime(str(date), format) # format = "%m/%d/%Y"
            return True
        except ValueError:
            return False
    
    def _httpget(self, url, h=None, d=None, l=True):
        timeDiff = 60 * 15 # in seconds
        #check cache
        parsed_uri = urlparse(url)
        #print "url=", url
        #print "parsed_uri=", parsed_uri

        cache_dir = self.cacheDir + '/www.cache/' + parsed_uri.netloc
        #print "cache_dir=", cache_dir

        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        cache_file = cache_dir + '/' + b64encode(url)
        print "_httpget cache_file=", cache_file
        if not os.path.exists(cache_file):
            return self._httpget_cache(url, cache_file, h, d, l)
        else: 
            modified = os.path.getmtime(cache_file)
            #print "modified=", modified
            diff = time.mktime(time.localtime()) - time.mktime(time.localtime(modified))
            #print "diff=", diff
            if not self.isReadOnly and datetime.datetime.now().date == self.gameDate and diff > timeDiff:
                return self._httpget_cache(url, cache_file, h, d, l)
            else:
                with open(cache_file, 'r') as f:
                    return f.read()
        
    def _httpget_cache(self, url, cache_file, h=None, d=None, l=True):
        """General HTTP resource fetcher. Pass headers via h, data via d, and to log via l."""
    
        try:
            if h and d:
                #page = utils.web.getUrl(url, headers=h, data=d)
                r = requests.get(url, params=d, headers=h)
            else:
                h = {"User-Agent":"Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:17.0) Gecko/20100101 Firefox/17.0"}
                #page = utils.web.getUrl(url, headers=h)
                r = requests.get(url, headers=h)
            #print "opening ", cache_file
            print "encoding=", r.encoding
            r.encoding = 'utf-8'
            print "encoding=", r.encoding
            f = codecs.open(cache_file, 'w', 'utf-8')
            f.write(r.text)
            f.close()
            return r.text
        except Exception as e:
            print "ERROR opening {0} message: {1}".format(url, e)
            self.log.error("ERROR opening {0} message: {1}".format(url, e))
            return None
    
    def _b64decode(self, string):
        """Returns base64 decoded string."""
    
        return b64decode(string)
    
    def _dtFormat(self, outfmt, instring, infmt):
        """Convert from one dateformat to another."""
    
        try:
            d = datetime.datetime.strptime(instring, infmt)
            output = d.strftime(outfmt)
        except:
            output = instring
        return output
    
    def _millify(self, num):
        """Turns a number like 1,000,000 into 1M."""
    
        for unit in ['','k','M','B','T']:
            if num < 1000.0:
                return "%3.3f%s" % (num, unit)
            num /= 1000.0
    
    ######################
    # DATABASE FUNCTIONS #
    ######################
    
    def _allteams(self):
        """Return a list of all valid teams (abbr)."""
    
        teamRows = self.sqlContext.sql("select team from mlb order by team").collect()
        teamlist = [item[0] for item in teamRows]
        print "teamlist=", teamlist
#        with sqlite3.connect(self._mlbdb) as conn:
#            cursor = conn.cursor()
#            query = "SELECT team FROM mlb"
#            cursor.execute(query)
#            teamlist = [item[0] for item in cursor.fetchall()]
    
        return teamlist
    
    def getTeamFromFanduelTeamId(self, team_id):
        abbrev = self.FanDualTeamIdToName[team_id]
        return self._validteams(abbrev)

    def _validteams(self, optteam):
        """Takes optteam as input function and sees if it is a valid team.
        Aliases are supported via mlbteamaliases table.
        Returns a 1 upon error (no team name nor alias found.)
        Returns the team's 3-letter (ex: NYY or ARI) if successful."""
    
        try:
            return self.teamAlias[optteam.lower()]
        except KeyError:
            try:
                return self.teamAlias[optteam.upper()]
            except KeyError:
                return None

        print "validTeams optteam=*" +  optteam + "*"
        aliasRow = self.sqlContext.sql("SELECT team FROM mlbteamaliases WHERE teamalias='" + optteam.lower() +"'").collect()
        if len(aliasRow) == 0:
            aliasRow = self.sqlContext.sql("SELECT team FROM mlb WHERE team='" + optteam.upper() + "'").collect()
            if len(aliasRow) == 0:
                print "RETURN NONE", optteam
                return None
            else:
                returnval = str(aliasRow[0][0])
        else:
            returnval = str(aliasRow[0][0])
        print "returnval=", returnval
        return returnval

#        with sqlite3.connect(self._mlbdb) as conn:
#            cursor = conn.cursor()
#            query = "SELECT team FROM mlbteamaliases WHERE teamalias=?"  # check aliases first.
#            cursor.execute(query, (optteam.lower(),))
#            aliasrow = cursor.fetchone()  # this will be None or the team (NYY).
#            if not aliasrow:  # person looking for team.
#                query = "SELECT team FROM mlb WHERE team=?"
#                cursor.execute(query, (optteam.upper(),))  # standard lookup. go upper. nyy->NYY.
#                teamrow = cursor.fetchone()
#                if not teamrow:  # team is not found. Error.
#                    returnval = None  # checked in each command.
#                else:  # ex: NYY
#                    returnval = str(teamrow[0])
#            else:  # alias turns into team like NYY.
#                returnval = str(aliasrow[0])
#        # return time.
#        return returnval
    
    def _translateTeam(self, db, column, optteam):
        """Translates optteam (validated via _validteams) into proper string using database column."""
    
        teamRow = self.sqlContext.sql("select " + db + " from mlb where " + column + "='" + optteam + "'").collect()
        print "teamRow=", teamRow
        print "return teamRow=", teamRow[0][0]
        return str(teamRow[0][0])
#        with sqlite3.connect(self._mlbdb) as conn:
#            cursor = conn.cursor()
#            # query = "SELECT %s FROM mlb WHERE %s='%s'" % (db, column, optteam)
#            query = "SELECT %s FROM mlb WHERE %s=?" % (db, column)
#            #print "query=", query
#            # cursor.execute(query)
#            cursor.execute(query, (optteam,))
#            row = cursor.fetchone()
#    
#        return (str(row[0]))
    
    def mlblineup(self, optteam):
        """<team>
        Gets lineup for MLB team.
        Ex: NYY
        """
    
        # test for valid teams.
        optteam = self._validteams(optteam)
        if not optteam:  # team is not found in aliases or validteams.
            #print("ERROR: Team not found. Valid teams are: {0}".format(self._allteams()))
            return
        # create url and fetch lineup page.
        url = self._b64decode('aHR0cDovL2Jhc2ViYWxscHJlc3MuY29tL2xpbmV1cF90ZWFtLnBocD90ZWFtPQ==') + optteam
        print "url=", url
        html = self._httpget(url)
        if not html:
            self.log.error("ERROR opening {0}".format(url))
            return
        # sanity check.
        if 'No game today' in html:
            print("ERROR: No game today for {0}".format(optteam))
            return
        # process html. this is kinda icky.
        soup = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES, fromEncoding='utf-8')
        div = soup.find('div', attrs={'class':'team-lineup highlight'})
        divs = div.findAll('div')   
        # 20140330 - had to fix this again.
        gmdate = divs[1].getText()  # date of game.
        seconddiv = None
        otherpitcher = ''
        if len(divs) > 3:
            seconddiv = divs[3]   # opp pitcher.
            otherpitcher = seconddiv.getText()  # opp pitcher and team.
        lineup = div.find('div', attrs={'class':'game-lineup'})
        # sanity check.
        if "No lineup yet" in lineup.getText():
            gmdate = 'No Lineup'
        else:  # div is a collection of divs, each div = person in lineup.
            lineup = lineup.findAll('div')
            lineup = [i.getText(separator=' ').encode('utf-8') for i in lineup]
        # output.
        record = {}
        record['team'] = optteam
        record['gmdate'] = gmdate
        record['otherPitcher'] = otherpitcher
        record['lineup'] = []
        #print record
        #print lineup
        if record['gmdate'] in ['No Game', 'No Lineup']:
            return record
        for item in lineup:
            player = {}
            item=item.rstrip()
            print "item=", item, "x"
            if item == '':
                continue
            # 8. Ryan Flaherty (L) 2B
            p = re.compile(" ?(\d)\. ((\w+ )+) \(.\) (\w+)")
            p = re.compile(" ?(\d)\. ([\w ]+) \((.)\) (\w+)")
            m = p.match(item)
            if m:
                player['batting_order'] = m.group(1)
                player['name'] = m.group(2)
                player['bats_lr'] = m.group(3)
                player['position'] = m.group(4)
                record['lineup'].append(player)
        return record
    
    def mlbpitcher(self, irc, msg, args, optteam):
        """<team>
        Displays current pitcher(s) and stats in active or previous game for team.
        Ex: NYY
        """
    
        # test for valid teams.
        optteam = self._validteams(optteam)
        if not optteam:  # team is not found in aliases or validteams.
            irc.reply("ERROR: Team not found. Valid teams are: {0}".format(self._allteams()))
            return
        # build url and fetch scoreboard.
        url = self._b64decode('aHR0cDovL3Njb3Jlcy5lc3BuLmdvLmNvbS9tbGIvc2NvcmVib2FyZA==')
        html = self._httpget(url)
        if not html:
            irc.reply("ERROR: Failed to fetch {0}.".format(url))
            self.log.error("ERROR opening {0}".format(url))
            return
        # process scoreboard.
        soup = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES, fromEncoding='utf-8')
        games = soup.findAll('div', attrs={'id': re.compile('.*?-gamebox')})
        # container to put all of the teams in.
        teamdict = collections.defaultdict()
        # process each "game" (two teams in each)
        for game in games:
            teams = game.findAll('p', attrs={'class':'team-name'})
            for team in teams:  # each game has two teams.
                tt = team.find('a')
                if tt:
                    # self.log.info("team: {0}".format(team))
                    ahref = team.find('a')['href']
                    teamname = ahref.split('/')[7].lower()  # will be lowercase.
                    teamname = self._translateTeam('team', 'eshort', teamname)  # fix the bspn discrepancy.
                    teamid = team['id'].replace('-aNameOffset', '').replace('-hNameOffset', '')  # just need the gameID.
                    teamdict.setdefault(str(teamname), []).append(teamid)
        # grab the gameid. fetch.
        teamgameids = teamdict.get(optteam)
        # sanity check before we grab the game.
        # self.log.info("TEAMGAMEIDS: {0}".format(teamgameids))
        if not teamgameids:
            # self.log.info("ERROR: I got {0} as a team. I only have: {1}".format(optteam, str(teamdict)))
            irc.reply("ERROR: No upcoming/active games with: {0}".format(optteam))
            return
        # we have gameid. refetch boxscore for page.
        # now we fetch the game box score to find the pitchers.
        # everything here from now on is on the actual boxscore page.
        for teamgameid in teamgameids:  # we had to do foreach due to doubleheaders.
            url = self._b64decode('aHR0cDovL3Njb3Jlcy5lc3BuLmdvLmNvbS9tbGIvYm94c2NvcmU=') + '?gameId=%s' % (teamgameid)
            html = self._httpget(url)
            if not html:
                irc.reply("ERROR: Failed to fetch {0}.".format(url))
                self.log.error("ERROR opening {0}".format(url))
                return
            # now process the boxscore.
            soup = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES, fromEncoding='utf-8')
            pitcherpres = soup.findAll('th', text='Pitchers')
            # defaultdict to put key: team value: pitchers.
            teampitchers = collections.defaultdict()
            # now, find the pitchers. should be two, one per team+ (relievers).
            if len(pitcherpres) != 2:  # game is too far from starting.
                if "Box Score not available." in html:  # sometimes the boxscore is not up.
                    pstring = "Box Score not available."
                else:
                    pitchers = soup.find('div', attrs={'class': 'line-score clear'})
                    if not pitchers:  # horrible and sloppy but should work.
                        pstring = "Error."
                    else:
                        startingpitchers = pitchers.findAll('p')
                        if len(startingpitchers) != 3:  # 3 rows, bottom 2 are the pitchers.
                            pstring = "Error."
                        else:  # minimal but it should stop most errors.
                            sp1, sp2 = startingpitchers[1], startingpitchers[2]
                            gameTime = soup.find('p', attrs={'id':'gameStatusBarText'})  # find time.
                            pstring = "{0} vs. {1}".format(sp1.getText(), sp2.getText())
                            if gameTime:  # add gametime if we have it.
                                pstring += " {0}".format(gameTime.getText())
                # now that we've processed above, append to the teampitchers dict.
                teampitchers.setdefault(str(optteam), []).append(pstring)
            else:  # we have the starting pitchers.
                for pitcherpre in pitcherpres:
                    pitchertable = pitcherpre.findParent('table')
                    pitcherrows = pitchertable.findAll('tr', attrs={'class': re.compile('odd player-.*?|even player-.*?')})
                    for pitcherrow in pitcherrows:  # one pitcher per row.
                        tds = pitcherrow.findAll('td')  # list of all tds.
                        pitchername = self._bold(tds[0].getText().replace('  ',' '))  # fix doublespace.
                        pitcherip = self._bold(tds[1].getText()) + "ip"
                        pitcherhits = self._bold(tds[2].getText()) + "h"
                        pitcherruns = self._bold(tds[3].getText()) + "r"
                        pitcherer = self._bold(tds[4].getText()) + "er"
                        pitcherbb = self._bold(tds[5].getText()) + "bb"
                        pitcherso = self._bold(tds[6].getText()) + "k"
                        pitcherhr = self._bold(tds[7].getText()) + "hr"
                        pitcherpcst = self._bold(tds[8].getText()) + "pc"
                        pitcherera = self._bold(tds[9].getText()) + "era"
                        team = pitcherrow.findPrevious('tr', attrs={'class': 'team-color-strip'}).getText()
                        # must translate team using fulltrans.
                        team = self._translateTeam('team', 'fulltrans', team)
                        # output string for the dict below.
                        pstring = "{0} - {1} {2} {3} {4} {5} {6} {7} {8}".format(pitchername, pitcherip, pitcherhits,\
                                                                                 pitcherruns, pitcherer, pitcherbb, \
                                                                                 pitcherso, pitcherhr, pitcherpcst, \
                                                                                 pitcherera)
                        teampitchers.setdefault(str(team), []).append(pstring)  # append into dict.
        # now, lets attempt to output.
            output = teampitchers.get(optteam, None)
            if not output:  # something went horribly wrong if we're here.
                irc.reply("ERROR: No pitchers found for {0}. Check when the game is active or finished, not before.".format(optteam))
                return
            else:  # ok, things did work.
                irc.reply("{0} :: {1}".format(self._red(optteam), " | ".join(output)))

    def transformPosition(self, pos):
        # transform positions
        if pos in ['SP', 'RP']:
            pos = 'P'
        elif pos in ['LF', 'CF', 'RF']:
            pos = 'OF'
        elif pos in ['DH']:
            pos = '1B'
        return pos

    def mlbroster(self, optlist, optteam):
        """[--40man|--active] <team>
        Display active roster for team.
        Defaults to active roster but use --40man switch to show the entire roster.
        Ex: --40man NYY
        """

        # test for valid teams.
        x = self._validteams(optteam)
        if not x:  # team is not found in aliases or validteams.
            print("ERROR: Team not found. Valid teams are: {0}".format(self._allteams()))
            return
        # handle optlist (getopts) here.
        active, fortyman = True, False
        for (option, arg) in optlist:
            if option == '--active':
                active, fortyman = True, False
            if option == '--40man':
                active, fortyman = False, True
        #print active, fortyman
        # conditional url depending on switch above.
        if active and not fortyman:
            url = self._b64decode('aHR0cDovL2VzcG4uZ28uY29tL21sYi90ZWFtL3Jvc3Rlci9fL25hbWU=') + '/%s/type/active/' % optteam.lower()
        else:  # 40man
            url = self._b64decode('aHR0cDovL2VzcG4uZ28uY29tL21sYi90ZWFtL3Jvc3Rlci9fL25hbWU=') + '/%s/' % optteam.lower()
        # fetch url.
        print "roster url=", url
        html = self._httpget(url)
        if not html:
            print("ERROR: Failed to fetch {0}.".format(url))
            self.log.error("ERROR opening {0}".format(url))
            return
        # process html.
        soup = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES, fromEncoding='utf-8')
        table = soup.find('div', attrs={'class':'mod-content'}).find('table', attrs={'class':'tablehead'})
        rows = table.findAll('tr', attrs={'class':re.compile('^oddrow player.*|^evenrow player.*')})
        # k/v container for output.
        team_data = collections.defaultdict(list)
        # each row is a player, in a table of position.

        # TOR Infielders :: Edwin Encarnacion (1B) | Juan Francisco (3B)
        roster = {}
        for row in rows:
            player = {}
            playerType = row.findPrevious('tr', attrs={'class':'stathead'}).getText()
            player['status'] = 'Non-Starter'
            player['active'] = 'Active'
            if playerType in ['Pitchers', 'Catchers', 'Infielders', 'Outfielders', 'Designated Hitters']:
                player['active'] = 'Active'
            elif playerType in ['Disabled List', 'Minors', 'Suspended', 'Bereavement']:
                player['active'] = 'Inactive'
            else:
                player['active'] = 'Unknown'
                print "Unknown type:", playerType
            playerNum = row.find('td')
            player['number'] = playerNum.getText()
            playerName = playerNum.findNext('td').find('a')
            player['name'] = playerName.getText()
            playerPos = playerName.findNext('td')
            player['pos'] = playerPos.getText()
            keyPos=self.transformPosition(player['pos'])
            #player_key = player['name'] + '_' + self.FanDuelTeamNametoID[optteam.upper()]
            player_key = player['name'] + '_' + self._validteams(optteam)
            roster[player_key] = player
            #team_data[playerType.getText()].append("{0} ({1})".format(playerName.getText(), playerPos.getText()))
        # output time.
        return roster
#        for i, j in team_data.iteritems():  # output one line per position.
#            print("{0} {1} :: {2}".format(optteam.upper(), i, " | ".join([item for item in j])))


    def getLineups(self):
        print "getting lineups."
        lineups = {}

        for team in self._allteams():
            print "team=", team
            lineup = self.mlblineup(team)
            print "lineup=", lineup
            if lineup['gmdate'] == 'No lineup':
                print "No lineup."
            optlist, args = getopt(['--40man', ''], "f", [ "40man", "active"])
            print "optlist=", optlist
            teamname = self._translateTeam('eshort', 'team', team)
            print "teamname=", teamname, "team=", team
            roster = self.mlbroster(optlist, teamname)
            print "roster=", roster
            if len(lineup['lineup']) == 0:
                print "NO LINEUP."
                for key in roster:
                    print "key=", key
                    roster[key]['status'] = 'No Lineup'
            else:
                for player in lineup['lineup']:
                    rosterKey = player['name'] + '_' + team
                    try:
                        roster[rosterKey]['status'] = 'Starter'
                    except KeyError:
                        roster[rosterKey]={}
                        roster[rosterKey]['status'] = 'Starter'
                    roster[rosterKey]['pos'] = self.transformPosition(player['position'])
            # merge dictionaries
            lineups = dict(lineups.items() + roster.items())
        print "lineups=", lineups
        return lineups

    
if __name__ == '__main__':
    mlb = MLB()

    mlb.getLineups()    
