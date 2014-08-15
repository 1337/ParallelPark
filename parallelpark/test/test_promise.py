from unittest.case import TestCase
from parallelpark.parallel import pmap


class PromiseTest(TestCase):

    def test_pmap(self):
        # Test map
        def scrape(url):
            import urllib2
            try:
                print "scraping {0}".format(url)
                h = urllib2.urlopen(url)
                print "scraped {0}!".format(url)
                return h.url
            except Exception as err:
                print err
                return None

        urls = [
            'http://willetinc.com',
            'http://secondfunnel.com',
            'http://github.com',
            'http://google.com',
            'http://google.ca',
            'http://ohai.ca',
            'http://reddit.com',
            'http://pinterest.com',
        ]

        print pmap(scrape, urls)

if __name__ == '__main__':
    PromiseTest().test_pmap()
