#from urllib.parse import urljoin, urlsplit
from urlparse import urljoin, urlsplit
from xml.etree import ElementTree
from datetime import datetime, timedelta
import logging
import os
import requests
import html5lib

log = logging.getLogger(__name__)

 
def setup_logging(ns="none", filename=None):
    """Setup and return a logger"""
    level = logging.DEBUG if os.environ.get("DEBUG", False) else logging.INFO

    log = logging.getLogger(ns)
    log.setLevel(level)

    formatter = logging.Formatter("%(asctime)s | %(name)s | "
                                  "%(levelname)s | %(message)s")

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)
    log.addHandler(console)

    if filename is not None:
        file_handler = logging.FileHandler(filename)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)

    return log

def get_boundary(date):
    """Format a boundary date string and return the datetime and the number
    of parts the datetime object was constructed from.

    This could be a partial date consisting of a year;
    year and month; or year, month, and day."""
    parts = 0
    for fmt in ("%Y", "%Y-%m", "%Y-%m-%d"):
        try:
            parts += 1
            return datetime.strptime(date, fmt), parts
        except (TypeError, ValueError):
            continue
    else:
        return None, 0

def get_inclusive_urls(urls, start, stop):
    """Yield URLs which are of the range [start, stop]"""
    in_range = False
    out_range = False
    for url in urls:
        # Check both that a URL contains or is contained within one of
        # the boundaries. This is necessary as deeper links come.
        print "inc url=", url, start, stop
        if url in start or start in url:
            print "in Range"
            in_range = True
        if url in stop or stop in url:
            print "out Range"
            out_range = True
        if in_range:
            yield url
        if out_range:
            break

def datetime_to_url(dt, parts=3):
    """Convert a Python datetime into the date portion of a Gameday URL
    """
    fragments = ["year_{0.year:04}", "month_{0.month:02}", "day_{0.day:02}"]
    return "/".join(fragments[:parts]).format(dt) + "/"


def download(urls, root):
    """Download `urls` into `root`. Return the count of files downloaded.
    Each URL is stored as its full URL (minus the scheme)."""
    session = requests.Session()
    downloads = 0
    fails = []
    for url in urls:
        parts = urlsplit(url)
        directory, filename = os.path.split(parts.path)
        # Skip directory pages.
        if not filename:
            continue

        target = os.path.join(root, parts.netloc + directory)
        # Ignore if the target directory already existed.
        if not os.path.exists(target):
            os.makedirs(target)

        print "url=", url
        response = session.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            log.error("download error: %s raised %s", url, str(exc))
            fails.append(url)
            continue

        with open(os.path.join(target, filename), "w") as fh:
            #fh.write(response.content.decode("utf8"))
            fh.write(response.content)
            log.debug("downloaded %s", url)
            downloads += 1

    return downloads, fails


def web_scraper(roots, match=None, session=None):
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


def filesystem_scraper(roots, match=None, session=None, **kwargs):
    """Yield paths in a directory which start with `match`.
    If `match` is None, all files are yielded."""
    try:
        for root in roots:
            for name in os.listdir(root):
                if match is None or name.startswith(match):
                    f = os.path.join(root, name)
                    print "f=", f
                    if os.path.exists(f):
                        print "yielding ", f
                        yield f 
    except OSError:
        pass


def get_years(root, source=web_scraper, session=None):
    """From the root URL, yield URLs to the available years."""
    for x in source([root], "year", session):
        yield x


def get_months(years, source=web_scraper, session=None):
    """Yield URLs to the available months for every year."""
    for x in source(years, "month", session):
        yield x

def get_days(months, source=web_scraper, session=None):
    """Yield URLs to the available days for every month."""
    for x in source(months, "day", session):
        yield x

def get_games(days, source=web_scraper, session=None):
    """Yield URLs to the available games for every day."""
    for x in source(days, "gid", session):
        yield x

def get_files(games, source=web_scraper, session=None):
    """Yield URLs to the relevant files for every game."""
    for game in games:
        for x in source([game], "game.xml", session):
            yield x
        for x in source([game], "game_events.json", session):
            yield x
        for x in source([game], "boxscore.json", session):
            yield x
        for x in source([game], "players.xml", session):
            yield x
        for x in source([game], "linescore.json", session):
            yield x
        for x in source([urljoin(game+'/', "inning/")], "inning_all.xml", session):
            yield x
