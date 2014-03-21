#!/usr/bin/env python

"""
More elaborate version of "Parallelism in one line"
https://medium.com/building-things-on-the-internet/40e9b2b36148
"""
from urllib2 import HTTPError


__copyright__ = 'Willet Inc.'
__author__    = 'Brian Lai'


from multiprocessing.dummy import Pool as ThreadPool
from functools import partial


class ParallelPark(object):

    data = None
    args = tuple()
    kwargs = {}
    workers = None

    async_result = None

    def __init__(self, data, args=None, kwargs=None, worker_count=4):
        """
        :param {list<{function}>} data    an iterable for multiprocessing
        :param {list|tuple}       args    a list of positional arguments to
                                          pass to the functions
        :param {dict}             kwargs  KVs of named arguments to pass
                                          to the functions
        :param {int}              workers number of threads to use
        """
        if not args:
            args = tuple()
        if not kwargs:
            kwargs = {}

        self.data, self.workers, self.args, self.kwargs = (
            data, ThreadPool(worker_count), args, kwargs)

    def __iter__(self):
        for result in self.values:
            if result:
                yield result

    def map(self, fn, *args, **kwargs):
        """
        :returns self
        """
        if not fn:
            return self  # done

        if self.async_result:
            self.async_result.wait()
            self.data = self.async_result.get()

        args += self.args
        kwargs.update(self.kwargs)

        fish_curry = partial(fn, *args, **kwargs)
        intermediates = self.workers.map_async(fish_curry, self.data)

        self.async_result = intermediates

        return self

    def clean(self):
        self.data = filter(None, self.data)
        return self

    @property
    def values(self):
        """Get the final values from all the maps

        :returns generator (there's no reason to return a list)
        """
        if self.workers:
            self.workers.close()
            self.workers.join()
            self.data = self.async_result.get()

        return self.data


if __name__ == '__main__':
    # Test map
    def scrape(url):
        import urllib2
        try:
            return urllib2.urlopen(url)
        except Exception as err:
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

    for response in ParallelPark(urls).map(scrape):
        print "%s %s" % (response.getcode(), response.url)
