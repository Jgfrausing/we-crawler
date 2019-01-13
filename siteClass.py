from collections import Counter


class Site:
    def __init__(self, domain, url, hash, words, links):
        self.domain2 = domain
        self.url = url
        self.hash = hash
        self.words = Counter([w[:99].lower() for w in words])
        self.links = links
