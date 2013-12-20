
__copyright__ = 'Willet Inc.'
__author__    = 'Brian Lai'

try:
    from concurrent import futures
except ImportError as err:
    import futures
    
from functools import partial


class ParallelPark(object):

    fns = []
    args = tuple()
    kwargs = {}
    workers = 4

    def __init__(self, *fns, **kwargs):
        """
        :param fns tuple
        kwargs allowed: args, kwargs, workers
        """
        if type(fns[0]) == list:  # passed in as [fn, fn, fn]; unpack
            self.fns = fns[0]
        else:
            self.fns = fns

        self._update(kwargs)

    def _update(self, dct):
        for key in dct:
            setattr(self, key, dct[key])

    def map(self, data=None, as_generator=True):
        """
        fn2(fn1(data, *args, **kwargs), *args, **kwargs)

        :param as_generator (no effect in Python 3+)

        :returns generator
        """

        if not data:
            data = []

        intermediates = data
        for fn in self.fns:
            with futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                fish_curry = partial(fn, *self.args, **self.kwargs)
                intermediates = executor.map(fish_curry, intermediates)

        if as_generator and hasattr(intermediates, '__iter__'):
            return intermediates
        return list(intermediates)

    def run(self, data=None, as_generator=True):
        """class(def 1, def 2).execute()

        :param as_generator (no effect in Python 3+)

        :returns list
        """
        len_fns = len(self.fns)
        idx_map = dict(zip(range(len_fns), self.fns))

        with futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            future_to_idx = dict([
                (executor.submit(idx_map[idx], data,
                                 *self.args, **self.kwargs), idx)
                for idx in idx_map
            ])

        for future in futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            idx_map[idx] = future.result()

        if as_generator and hasattr(idx_map, 'itervalues'):
            return idx_map.itervalues()

        return idx_map.values()

    def then(self, fn):
        pass

    def fail(self, handler):
        pass


if __name__ == '__main__':
    # Test map
    a = ParallelPark(lambda x: x + 5).map([1, 2, 3])
    for x in a:
        print(x)

    # Test run
    a = ParallelPark(lambda x: x + 5, lambda x: x - 5).run(1)
    for x in a:
        print(x)
