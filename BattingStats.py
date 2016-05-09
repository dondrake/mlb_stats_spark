import re


class BattingStats(object):
    def __init__(self):
        self.game_id = ""
        self.player_id = 0
        self.ab = 0
        self._1b = 0
        self._2b = 0
        self.fd_points_2b = 0.0
        self._3b = 0
        self.fd_points_3b = 0.0
        self.hr = 0
        self.fd_points_hr = 0.0
        self.rbi = 0
        self.r = 0
        self.bb = 0
        self.sb = 0
        self.fd_points_sb = 0.0
        self.hbp = 0
        self.out = 0
        self.fd_points_out = 0.0
        self.total_hits = 0
        self.fd_points_tb = 0.0
        self.fd_points = 0.0

    def __repr__(self):
        return str(self.__dict__)

    def __add__(self, other):
        b = BattingStats()
        # don't agg dims
        b.game_id = self.game_id
        b.player_id = self.player_id

        b.ab = self.ab + other.ab
        b._1b = self._1b + other._1b
        b._2b = self._2b + other._2b
        b.fd_points_2b = self.fd_points_2b + other.fd_points_2b
        b._3b = self._3b + other._3b
        b.fd_points_3b = self.fd_points_3b + other.fd_points_3b
        b.hr = self.hr + other.hr
        b.fd_points_hr = self.fd_points_hr + other.fd_points_hr
        b.rbi = self.rbi + other.rbi
        b.r = self.r + other.r
        b.bb = self.bb + other.bb
        b.sb = self.sb + other.sb
        b.fd_points_sb = self.fd_points_sb + other.fd_points_sb
        b.hbp = self.hbp + other.hbp
        b.out = self.out + other.out
        b.fd_points_out = self.fd_points_out + other.fd_points_out
        b.total_hits = self.total_hits + other.total_hits
        b.fd_points_tb = self.fd_points_tb + other.fd_points_tb
        b.fd_points = self.fd_points + other.fd_points
        return b

    def handleFDEvent(self, event):
        # Runs are under counted, we need to look up all player names:
        # Kole Calhoun homers (2) on a fly ball to right center field. Chris Iannetta scores. Johnny Giavotella scores.
        # This should be handled upstream...

        # This is for WPs (pitcher stats)
        if event.batter <= 0:
            return

        self.game_id = event.game_id
        self.player_id = event.batter

        self.ab += 1
        if event.fd_batter_event in ['SO', 'O']:
            self.out += 1
        elif event.fd_batter_event == 'H':
            self._1b += 1
            self.total_hits +=1
        elif event.fd_batter_event == 'W':
            self.bb += 1
            self.ab -= 1
        elif event.fd_batter_event == 'HR':
            self.hr += 1
            self.total_hits +=1
            self.rbi +=1
            self.r +=1
        elif event.fd_batter_event == 'HBP':
            self.hbp += 1
            self.ab -= 1
        elif event.fd_batter_event == 'SB':
            self.sb += 1
            self.ab -= 1
        elif event.fd_batter_event == '2B':
            self._2b += 1
            self.total_hits +=1
        elif event.fd_batter_event == '3B':
            self._3b += 1
            self.total_hits +=1
        elif event.fd_batter_event == 'R':
            # Fake event from below
            self.r += 1
            self.ab -= 1
        elif event.fd_batter_event == 'SAC':
            # not a hit, but doesnt count as an at bat
            self.ab -= 1
        elif event.fd_batter_event == 'FC':
            # not a hit, but does count as an at bat
            pass
        elif event.fd_batter_event == 'E':
            # counts as atbat
            pass
        elif event.fd_batter_event == 'BALK':
            self.ab -= 1
        elif event.fd_batter_event is None:
            if "is now pitching" in event.des:
                # switch-pitcher!?!
                # 'unhandled event:', None, u'Pat Venditte is now pitching right-handed.  ', u'gid_2015_03_03_sfnmlb_oakmlb_1', 519381, -1)
                pass
        else:
            raise Exception("unhandled event:", event.fd_batter_event, event.des, event.game_id, event.batter, event.pitcher)
            
        # find: all occurences of
        # 'Russell Martin doubles (8) on a sharp fly ball to right fielder Marlon Byrd. Andrew McCutchen scores. Neil Walker scores. '
        res = re.findall('[\w ]+ scores\.', event.des)
        if res is not None:
            self.rbi += len(res)
        self.out = self.ab - self.total_hits

        fantasyPoints = 0.0
        fantasyPoints += (3.0 * self._1b)
        fantasyPoints += (6.0 * self._2b)
        fantasyPoints += (9.0 * self._3b)
        fantasyPoints += (12.0 * self.hr)
        fantasyPoints += (3.5 * self.rbi)
        fantasyPoints += (3.2 * self.r)
        fantasyPoints += (3.0 * self.bb)
        fantasyPoints += (6.0 * self.sb)
        fantasyPoints += (3.0 * self.hbp)
        #fantasyPoints -= (0.25 * self.out)
        #fantasyPoints /= player['G']
        self.fd_points = fantasyPoints

        self.fd_points_2b += (6.0 * self._2b)
        self.fd_points_3b += (9.0 * self._3b)
        self.fd_points_hr += (12.0 * self.hr)
        self.fd_points_sb += (6.0 * self.sb)
        #self.fd_points_out -= (0.25 * self.out)
        self.fd_points_tb += self.bb + self._1b + self.fd_points_2b + self.fd_points_3b + self.fd_points_hr 

    @staticmethod
    def mapEventstoBattingStats(event):
        battingStats = BattingStats()
        battingStats.handleFDEvent(event)
        key = "|".join(map(str, [event.game_id, event.batter]))
        return (key, battingStats)

    @staticmethod
    def reduceByKey(a, b):
        if a is None:
            return b
        #print "a=", a
        #print "b=", b
        c = a + b
        #print "c=", c
        return c


