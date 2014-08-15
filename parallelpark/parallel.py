#!/usr/bin/env python

"""
More elaborate version of "Parallelism in one line"
https://medium.com/building-things-on-the-internet/40e9b2b36148
"""
from urllib2 import HTTPError


__copyright__ = 'Willet Inc.'
__author__ = 'Brian Lai'
__license__ = 'MIT'

# same API, doesn't actually multiprocess
# from multiprocessing.dummy import Pool
from multiprocessing import Pool
from functools import partial, wraps


class Promise(object):
    fn = None
    def __init__(self, fn, args, kwds):
        self.fn = fn
        self.args = args
        self.kwds = kwds

    def then(self, fn):
        pool = Pool(processes=1)              # Start a worker processes.
        result = pool.apply_async(self.fn, args=self.result, kwds=self.kwds, fn) # Evaluate "f(10)" asynchronously calling callback when finished.
        self.result = result
        return self


class ParallelPark(object):

    data = None
    args = tuple()
    kwargs = {}
    workers = None

    async_result = None

    def __init__(self, data=None, args=None, kwargs=None, worker_count=4):
        """
        :param {list<{function}>} data    an iterable for multiprocessing
        :param {list|tuple}       args    a list of positional arguments to
                                          pass to the functions
        :param {dict}             kwargs  KVs of named arguments to pass
                                          to the functions
        :param {int}              workers number of threads to use
        """
        if not data:
            data = []
        if not args:
            args = tuple()
        if not kwargs:
            kwargs = {}

        self.data, self.workers, self.args, self.kwargs = (
            data, Pool(worker_count), args, kwargs)

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
        if self.data:
            intermediates = self.workers.map_async(fish_curry, self.data)
        else:  # no data for async decorator
            intermediates = self.workers.map_async(fish_curry, [])

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


def parallel(fn):
    class Promise(object):
        def __init__(self, fn):
            self.fn = fn

        def __call__(self, *args, **kwargs):
            self.fn(*args, **kwargs)
            return self

        def then(self, fn):
            pass

    @wraps(fn)
    def wrapped_fn(*args, **kwargs):
        async = ParallelPark(args=args, kwargs=kwargs).map(fn)
        return async
    return wrapped_fn
