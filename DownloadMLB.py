#!/usr/bin/env python2.7

from datetime import datetime, timedelta, date
from pytz import timezone
#from urllib.parse import urljoin
from urlparse import urljoin
import html5lib
import argparse
import scrape
import os
import requests
import logging
import re
import sys
from pyspark import SparkContext


def dump(x):
    print x

def getFiles(game):
    session = requests.Session()

    print "GAME=", game
    files = scrape.get_files([game], session=session)
    count, fails = scrape.download(files, DownloadMLB.cache)
    return (count, fails)

def summarize(a, x):
    #print "a=", a, "x=", x
    total = a[0] + x[0]
    alist = a[1]
    alist.extend(x[1])
    return (total, alist)

class DownloadMLB(object):

    WEB_ROOT = 'http://gd2.mlb.com/components/game/mlb/'

    cache=os.path.expanduser('~') + '/spark_mlb/spark.mlb.com/'

    def __init__(self, begin=None, end=None):
        self.begin=begin
        self.end=end
        self.log = scrape.setup_logging(ns="gd", filename="scraper.log")
        print "cache=", self.cache
        if not os.path.exists(self.cache):
            os.makedirs(self.cache)

        yesterday = datetime.now(timezone('US/Central')) - timedelta(days=1)
        yesterday = yesterday.replace(tzinfo=None).date()
        print "yesterday=", yesterday
        today = datetime.now(timezone('US/Central')).replace(tzinfo=None).date()
        print "today=", today
        if self.begin is None and self.end is None:
            # default to yesterday
            start = urljoin(self.WEB_ROOT, scrape.datetime_to_url(yesterday))
            self.startDate = yesterday
            stop = urljoin(self.WEB_ROOT, scrape.datetime_to_url(yesterday))
            self.stopDate = today
        elif self.begin is None and self.end is not None:
            print "-b is required."
            sys.exit()
        else:
            self.startDate = datetime.strptime(self.begin, '%Y%m%d').date()
            self.start = urljoin(self.WEB_ROOT,
                            scrape.datetime_to_url(self.startDate))

            if self.end is None:
                stop = urljoin(self.WEB_ROOT, scrape.datetime_to_url(today))
                self.stopDate = today
            else:
                self.stopDate = datetime.strptime(self.end, '%Y%m%d').date()
                stop = urljoin(self.WEB_ROOT, scrape.datetime_to_url(self.stopDate))
        #self.cache = None

    def web_scraper(self, roots, match=None, session=None):
        """Yield URLs in a directory which start with `match`.
        If `match` is None, all links are yielded."""
        for root in roots:
            if session is not None:
                response = session.get(root)
            else:
                response = requests.get(root)
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                log.error("web_scraper error: %s raised %s", root, str(exc))
                continue

            #source = ElementTree.fromstring(response.content)
            source = html5lib.parse(response.content, namespaceHTMLElements=False)
            a_tags = source.findall(".//a")
            for a in a_tags:
                url = a.attrib["href"]
                if match is None or url[slice(0, len(match))] == match:
                    yield urljoin(root, url)

    def getYearsMonths(self, root, session):
        print "starting."
        for x in self.web_scraper([root], "year", session):
            print "x=",x, self.startDate
            m= re.search('/year_(\d{4})/', x)
            if not m:
                continue
            year = int(m.group(1))
            if not (year >= self.startDate.year and year <= self.stopDate.year):
                print "skipping"
                continue
            for y in self.web_scraper([x], "month", session):
                print "y=", y
                m= re.search('/month_(\d{2})/', y)
                if not m:
                    continue
                month = int(m.group(1))
                for z in self.web_scraper([y], "day", session):
                    print "z=", z
                    m= re.search('/day_(\d{2})/', z)
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

    def run(self):


        sc = SparkContext()
        start_scrape = datetime.now()
        begin, begin_parts = scrape.get_boundary(self.begin)
        end, end_parts = scrape.get_boundary(self.end)


        session = requests.Session()

        print "here"
        all_years_months_days = self.getYearsMonths(self.WEB_ROOT, session)

        games = scrape.get_games(all_years_months_days, session=session)

        gamesRDD = sc.parallelize(games)
        print "fileRDD=", gamesRDD

        gamesRDD.foreach(dump)
        print "# parttions:", gamesRDD.getNumPartitions()
        print "count=", gamesRDD.count()
        res = gamesRDD.map(getFiles).reduce(summarize)
        print "res=", res

        count = res[0]
        fails = res[1]
        #files = scrape.get_files(games, session=session)
        #count, fails = scrape.download(files, self.cache)
        end_scrape = datetime.now()
        self.log.info("%d files downloaded in %s", count,
                 str(end_scrape - start_scrape))
        if fails:
            for url in fails:
                self.log.error("failed to download %s", url)

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
    downloader = DownloadMLB(args.begin, args.end)
    downloader.run()
