#!/usr/bin/env python

"""
More elaborate version of "Parallelism in one line"
https://medium.com/building-things-on-the-internet/40e9b2b36148
"""
import pickle


__copyright__ = 'Willet Inc.'
__author__ = 'Brian Lai'
__license__ = 'MIT'


def pmap(fn, lst):
    """Same as map(fn, lst)."""
    # pick "real multiprocessing" if possible, which works with global fns
    # otherwise, resort to threads
    try:
        pickle.dumps(fn)
        from multiprocessing import Pool
    except pickle.PicklingError:
        from multiprocessing.dummy import Pool

    pool = Pool()
    return pool.map(fn, lst)


