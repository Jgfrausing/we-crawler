import threading
import time
from queue import Queue
from urllib.parse import urlparse

import pymysql

from orderedSetQueue import OrderedSetQueue
from threading import Lock

import warnings
warnings.filterwarnings("ignore")


def get_database_connect():
    return pymysql.connect(host="localhost", user="root", passwd="12345678", port=3306, autocommit=True)


class AtomicCounter:
    def __init__(self, initial=0):
        self.value = initial
        self._lock = threading.Lock()

    def increment(self, num=1):
        with self._lock:
            self.value += num
            return self.value


word_mutex = Lock()
site_mutex = Lock()
domain_mutex = Lock()

site_queue = Queue()
sites_to_visit_queue = OrderedSetQueue()
sites_visited = set()
words_in_db = dict()

domain_id_counter = AtomicCounter(0)
word_id_counter = AtomicCounter(0)

site_ref = set()
domains = dict()



class Database:
    pop_size = 5

    def __init__(self):
        self.conn = get_database_connect()
        self.db = self.conn.cursor()

    def run(self):
        sites_visited.update(self.get_sites_visited())
        while True:

            site = site_queue.get()
            if site.url not in sites_visited:
                self.save_site(site)
                self.insert_sites_to_visit(site.links)
                self.insert_word_to_db(site.words)
                self.save_words(site)

    def run(self, site):
        sites_visited.update(self.get_sites_visited())
        if site.url not in sites_visited:
            try:
                self.save_domain(site.domain2)
                if self.save_site(site):
                    self.save_references(site)
                    self.insert_sites_to_visit(site.links)
                    self.insert_word_to_db(site.words)
                    self.save_words(site)

            except Exception as e:
                print(e)

    def save_domain(self, domain):
        with domain_mutex:
            if domain not in domains:
                domains[domain] = domain_id_counter.increment(1)

    def get_domain(self, url):
        try:
            siteInfo = urlparse(url)
            return siteInfo.scheme + '://' + siteInfo.hostname
        except Exception as e:
            print(e)


    def save_references(self, site):
        try:
            for link in site.links:
                link_domain = self.get_domain(link)
                self.save_domain(link_domain)
                sid = domains[site.domain2]
                did = domains[link_domain]
                if sid != did:
                    var = (sid, did)
                    site_ref.add(var)
        except Exception as e:
            print(e)


    def separate(self, lst):
        return ', '.join(self.quote(h) for h in lst)

    def quote(self, s):
        return '"{0}"'.format(s)
    
    def get_save_site_sql(self, site):
        return "INSERT IGNORE INTO wi_project.sites (link,md5,sha1,sha224,sha256,sha384,sha512) VALUES(" + self.quote(site.url) + ", " + self.separate(site.hash) + "); "

    def insert_sites_to_visit(self, sites):
        for s in sites:
            if s not in sites_visited:
                sites_to_visit_queue.put(s)
        #map(self.sites_to_visit_queue.put_nowait, sites)

    def save_site(self, site):

        if site.url not in sites_visited:
            sql = self.get_save_site_sql(site)
            sites_visited.add(site.url)

            rows = self.db.execute(sql)
            return rows == 1
        return False


    def get_sites_visited(self):
        self.db.execute("SELECT link FROM wi_project.sites")
        res = [x[0] for x in self.db.fetchall()]
        self.conn.commit()
        return res

    def get_site_to_visit(self):
        return sites_to_visit_queue.get()
        '''
        count = 0
        urls = []
        while count < self.pop_size:
            self.db.execute("SELECT id, url FROM wi_project.urls_to_crawl order by id LIMIT 1;")
            site = self.db.fetchall()
            self.conn.commit()
            if len(site) == 0:
                break

            id = site[0][0]
            url = site[0][1]
            sql = "DELETE FROM wi_project.urls_to_crawl WHERE id = " + str(id) + ";"
            success = self.db.execute(sql)
            self.conn.commit()
            if success == 1:
                count += 1
                urls.append(url)

        return urls
        '''

    def get_site_id(self, url):
        sql = "SELECT id FROM wi_project.sites s WHERE s.link = " + self.quote(url) + " limit 1;"

        self.db.execute(sql)
        # First element of list first element of tuple
        res = self.db.fetchall()[0][0]
        return res

    def insert_word_to_db(self, words):

        for word in words:
            try:
                canSave = False
                with word_mutex:
                    if word not in words_in_db:
                        id = word_id_counter.increment(1)
                        words_in_db[word] = id
                        canSave = True
                if canSave:
                    self.db.execute("INSERT IGNORE INTO wi_project.words (id, word) SELECT " + self.quote(id) + ", " + self.quote(word) + ";")
            except Exception as e:
                print(e)

    def get_word_ids(self, words):
        #self.db.execute("SELECT word, id FROM wi_project.words w WHERE w.word in (" + self.separate(words) + ")")
        #res = self.db.fetchall()

        return words_in_db

    def save_words(self, site):
        url = site.url
        words = site.words
        site_id = self.get_site_id(url)

        # Get word ids+word
        word_dict = words_in_db

        # Add word-url-bridge
        for word in words:
            try:
                sql = "INSERT IGNORE INTO wi_project.word_in_site (word_id, site_id, occurrence) VALUES (" + str(word_dict[word]) + ", " + str(site_id) + ", " + str(words[word]) + ");"
                self.db.execute(sql)
            except:
                continue


    def print_site_count(self, sec):
        previous = 0
        start = time.time()
        while True:
            time.sleep(sec)
            self.db.execute("SELECT count(*) FROM wi_project.sites;")
            res = self.db.fetchall()[0][0]
            print("Pages per second: " + str((res - previous)/sec) + " ("+str(res)+"). Active threads = ", str(threading.active_count()) + ". Total avg: " + str(round(res/(time.time()-start), 1)))
            previous = res
            #self.conn.commit()

    def truncate(self):
        #sql = "alter table word_in_site drop index word_index;"
        #self.db.execute(sql)

        tables = ["sites", "words", "urls_to_crawl", "word_in_site", "domains", "domain_references"]
        for t in tables:
            self.db.execute("TRUNCATE wi_project." + t)

    def insert_domains(self):
        for domain in domains:
            sql = "INSERT INTO wi_project.domains (id, domain) VALUES ("+self.quote(domains[domain])+", " + self.quote(domain) + ");"
            self.db.execute(sql)

        for pair in site_ref:

            sql = "INSERT IGNORE INTO wi_project.domain_references (from_id, to_id) VALUES ("+self.quote(pair[0])+", " + self.quote(pair[1]) + ");"
            self.db.execute(sql)

    def set_inverse_document_frec(self):
        sql = "alter table word_in_site add index word_index (word_id);"
        self.db.execute(sql)
        sql = "CREATE TEMPORARY TABLE aggregated_cnt SELECT count(*) as cnt, word_id FROM word_in_site GROUP BY word_id;"
        self.db.execute(sql)
        sql = "UPDATE words w SET occurrence = (select cnt from aggregated_cnt where word_id = id limit 1);"
        self.db.execute(sql)
        sql = "drop table aggregated_cnt;"
        self.db.execute(sql)

    def search(self, words):

        sql ="SELECT site_id FROM wi_project.word_in_site WHERE word_id in (SELECT id FROM wi_project.words WHERE word in ("+self.separate(words)+")) group by site_id ORDER BY COUNT(*) DESC, EXP(SUM(LOG(tf_idf))) DESC limit 10;"
        self.db.execute(sql)
        site_ids = self.db.fetchall()

        sql = "SELECT link FROM wi_project.sites WHERE id in (" + self.separate([x[0] for x in site_ids]) + ");"
        self.db.execute(sql)
        urls = self.db.fetchall()

        return [x[0] for x in urls]

    def get_domain_ref(self):
        sql = "SELECT * FROM wi_project.domain_references;"
        self.db.execute(sql)

        return self.db.fetchall()

