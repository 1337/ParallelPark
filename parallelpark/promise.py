"""
def poop():
    p = Promise()

    def done(data):
        p.resolve(data)

    another_promise.done(done)
    return p
"""

from multiprocessing import Pool, cpu_count


class Promise(object):
    _pool = _func = _args = _kwargs = None
    _done_callbacks = _error_callbacks = _always_callbacks = []
    _always_result = None

    def __init__(self, func, args=None, kwargs=None):
        if not args:
            args = tuple()
        if not kwargs:
            kwargs = {}

        self._func, self._args, self._kwargs = func, args, kwargs

    def __call__(self):
        self._pool = Pool(processes=cpu_count())

        try:
            self._pool.apply_async(
                self._func, args=self._args, kwds=self._kwargs,
                callback=self.resolve)
        except BaseException as err:
            self.reject(err)
            self._run_always_callbacks()

        self._pool.close()
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._pool:
            self._pool.close()

    def _run_done_callbacks(self, result):
        self._always_result = result
        for func in self._done_callbacks:
            func(result)

        self._done_callbacks = []
        return self

    def _run_error_callbacks(self, reason):
        self._always_result = reason
        for func in self._error_callbacks:
            func(reason)

        self._error_callbacks = []
        return self

    def _run_always_callbacks(self):
        for func in self._always_callbacks:
            func(self._always_result)

        self._always_callbacks = []
        return self

    def done(self, callback):
        self._done_callbacks.append(callback)
        return self

    def fail(self, callback):
        self._error_callbacks.append(callback)
        return self

    def always(self, callback):
        self._always_callbacks.append(callback)
        return self

    def resolve(self, result):
        self._run_done_callbacks(result)
        self._run_always_callbacks()
        return self

    def reject(self, result):
        self._run_error_callbacks(result)
        return self


def callback_function(result):
    print "the result is {0}".format(result)


def fail_function(reason):
    print "{0}".format(reason)


def sync_func(a):
    return a + 2


def async_func():
    return Promise(sync_func, args=(4, ))


if __name__ == "__main__":
    promise = async_func()
    (promise.done(callback_function)
            .fail(fail_function)())