__author__ = 'zhaolin.su'

import threading
import os
import socket
from time import sleep
from datetime import datetime
from su.util import simple_traceback
from su.env import LOGGER

local_locks = threading.local()
hostname = socket.gethostname()
pid = os.getpid()


class CacheLock(object):
    def __init__(self, cache, stats, group, key,
                 expire=30, timeout=20, verbose=True):
        # get a thread-local set of locks that we own
        self.locks = local_locks.locks = getattr(local_locks, 'locks', set())

        self.stats = stats
        self.group = group
        self.key = key
        self.cache = cache
        self.time = expire
        self.timeout = timeout
        self.have_lock = False
        self.verbose = verbose

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, tb):
        self.release()

    def acquire(self):
        start = datetime.now()

        my_info = (hostname, pid, simple_traceback(limit=7))

        #if this thread already has this lock, move on
        if self.key in self.locks:
            return

        timer = self.stats.get_timer("lock_wait")
        timer.start()

        #try and fetch the lock, looping until it's available
        while not self.cache.add(self.key, my_info, time=self.time):
            if (datetime.now() - start).seconds > self.timeout:
                if self.verbose:
                    info = self.cache.get(self.key)
                    if info:
                        info = "%s %s\n%s" % info
                    else:
                        info = "(nonexistent)"
                    msg = ("\nSome jerk is hogging %s:\n%s" % (self.key, info))
                    msg += "^^^ that was the stack trace of the lock hog, not me."
                else:
                    msg = "Timed out waiting for %s" % self.key
                raise TimeoutError(msg)
            #print('lock #%s# found, waiting... %s/%s' % (self.key, (datetime.now() - start).seconds, self.timeout))
            LOGGER.debug('lock #%s# found, waiting...' % self.key)
            sleep(.01)

        timer.stop(subname=self.group)

        #tell this thread we have this lock so we can avoid deadlocks
        #of requests for the same lock in the same thread
        self.locks.add(self.key)
        self.have_lock = True

    def release(self):
        #only release the lock if we gained it in the first place
        if self.have_lock:
            self.cache.delete(self.key)
            self.locks.remove(self.key)


def make_lock_factory(cache, stats):
    def factory(group, key, **kw):
        return CacheLock(cache, stats, group, key, **kw)
    return factory


if __name__ == '__main__':
    import time
    from su.g import make_lock, redis_lock
    from su.cache import RedisCache
    from threading import Thread

    # test lock acquire&release
    lock = make_lock('test_lock', 'test_acquire')
    assert redis_lock.get('test_acquire') is None
    lock.acquire()
    l = redis_lock.get('test_acquire')
    assert len(l) > 0
    assert isinstance(l, tuple)
    assert 'test_acquire' in lock.locks
    with make_lock('test_lock', 'test_acquire'):
        pass
    lock.release()
    assert redis_lock.get('test_acquire') is None

    def worker(fn, *args, **kwargs):
        t = Thread(target=lambda: fn(*args, **kwargs))
        t.setDaemon(True)
        t.start()
        return t

    # test wait
    current_process = 'undefined'
    long_process_is_running = False
    def long_process(length=1, **kwargs):
        with make_lock('test_lock', 'test_wait', **kwargs):
            print("locking...")
            global current_process, long_process_is_running
            long_process_is_running = True
            current_process = 'long_process'
            time.sleep(length)
            print("releasing...")
            long_process_is_running = False

    def check_process():
        with make_lock('test_lock', 'test_wait'):
            global current_process
            current_process = 'check_process'
            print("got it")
    worker(long_process)
    time.sleep(.1)
    assert current_process == 'long_process'
    assert long_process_is_running
    worker(check_process)
    assert current_process == 'long_process'
    assert long_process_is_running
    time.sleep(.5)
    assert current_process == 'long_process'
    assert long_process_is_running
    time.sleep(1)
    assert current_process == 'check_process'
    assert not long_process_is_running

    # test lock expire
    worker(long_process, length=2, expire=1)
    time.sleep(.1)
    assert current_process == 'long_process'
    assert long_process_is_running
    time.sleep(1)
    worker(check_process)
    time.sleep(.1)
    # although long_process is still running, the lock is released due to time=1
    assert current_process == 'check_process'
    assert long_process_is_running
    time.sleep(1)
    assert not long_process_is_running
