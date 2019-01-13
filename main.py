import re
import signal
import sys
import threading
from threading import Thread
from urllib.request import urlopen
from urllib.parse import urlparse, urljoin
from urllib import robotparser
from functools import lru_cache
import _thread

import nltk
from bs4 import BeautifulSoup


from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

import indexer
import db
import time
import warnings

warnings.filterwarnings("ignore")

from siteClass import Site

canContinue = True

class Spider:
    def crawl(self, url, visit_amount, database):
        numberVisited = 0

        while numberVisited < visit_amount and canContinue:
            try:
                siteInfo = urlparse(url)
                domain = siteInfo.scheme + '://' + siteInfo.hostname

                robot = self.get_robot_rules(domain)

                if robot.can_fetch("*", url):
                    try:
                        html = self.get_website_html(url)
                        text = self.get_website_text(html)
                        hash = indexer.get_hash_values_from_text(text)
                        links = self.get_links(html, domain)
                        site = Site(domain, url, hash, self.remove_stopwords(text), links)

                        database.run(site)
                        numberVisited = numberVisited + 1

                    except Exception as e:
                        #print(e)
                        #print("Error: " + url)
                        continue

                #else:
                    #print("Can't fetch: " + str(url))
            except Exception as e:
                pass
                #print("Error: " + str(e))
            finally:
                url = database.get_site_to_visit()

        database.conn.close()

    def get_links(self, html, domain):
        try:
            bSoup = BeautifulSoup(html, "html.parser").findAll("a", href=True)
            hrefs = [urljoin(domain, x['href']) for x in bSoup]

            links = []
            for link in hrefs:
                if link.startswith("http://") or link.startswith("https://"):
                    links.append(link)

            return links
        except Exception as e:
            print(e)

    def get_website_html(self, url):
        html = urlopen(url).read()

        if "<!DOCTYPE HTML" in str(html[:50].upper()):
            return html.decode('ASCII', 'ignore')

        raise Exception("Not HTML")

    def get_website_text(self, html):
        htmlClean = BeautifulSoup(html, "html.parser").findAll(["p", "span", "h1", "h2", "h3", "strong", "em", "td", "th"], text=True)
        return ' '.join([' '.join(x.string.lower().split()) for x in htmlClean])

    @lru_cache(maxsize=100)
    def get_robot_rules(self, domain):
        rp = robotparser.RobotFileParser()
        rp.set_url(domain + '/robots.txt')
        rp.read()
        return rp

    def remove_stopwords(self, text):
        stop_words = set(stopwords.words('english'))
        stop_words.update(['.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}', '...'])

        word_tokens = word_tokenize(text)
        return [w for w in word_tokens if not w in stop_words]


def start(urls, cnt, database):
    spider = Spider()
    spider.crawl(urls, cnt, database)


def start_measure():
    datab = db.Database()
    datab.print_site_count(5)


def signal_handler(sig, frame):
    print("doo doo doo")
    global canContinue
    canContinue = False
    for thre in threads:
        thre.join()
    db.Database().insert_domains()
    db.Database().set_inverse_document_frec()
    sys.exit(0)


#### GO NUTS

nltk.download('punkt')
nltk.download('stopwords')

_thread.start_new_thread(start_measure, ())

threads = []

signal.signal(signal.SIGINT,  signal_handler)
start_url = "http://deeplearning.net/"
db.Database().truncate()
start(start_url, 5, db.Database())

for x in range(1, 50):
    time.sleep(0.1)
    y = 30
    t = Thread(target=start, args=("", 100, db.Database(),))
    t.start()
    threads.append(t)

while True:
    continue

