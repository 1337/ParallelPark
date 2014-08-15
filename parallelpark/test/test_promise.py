from unittest.case import TestCase
from parallelpark.parallel import ParallelPark, parallel


class PromiseTest(TestCase):

    def test_promise(self):
        # Test map
        def scrape(url):
            import urllib2
            try:
                print "scraping {0}".format(url)
                return urllib2.urlopen(url)
            except Exception as err:
                return None

        @parallel
        def async_scrape(url):
            scrape(url)
            print "scraped {0}!".format(url)


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

        # use in iterator
        for response in ParallelPark(urls).map(scrape):
            print "%s %s" % (response.getcode(), response.url)

        for url in urls:
            a = async_scrape(url)
            print a  # accessing a.values will block


if __name__ == '__main__':
    PromiseTest().test_promise()
