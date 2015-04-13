__author__ = 'zhaolin.su'

from su.tests.test_models import User,Post,Comment,Friendship,Vote
from su.cache import test_cache

from su.g import (
    backend,
    cache
)

from threading import Thread
from copy import copy


def multi_thread_test(test_fn, test_args, test_kws, num_threads=1000, num_per_thread=100, tidx_name=None):
    threads = []
    for x in range(num_threads):
        def _fn(args, kws, _idx):
            wrapped_kws = copy(kws)
            if tidx_name:
                wrapped_kws[tidx_name] = _idx
            def __fn():
                for y in range(num_per_thread):
                    test_fn(*args, **wrapped_kws)
            return __fn
        t = Thread(target=_fn(test_args, test_kws, str(x)))
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

num_threads = 10
num_per_thread = 100


def test_db_multi_process():
    users = []
    def _test_insert(idx):
        attrs = {'ups': idx, 'downs': idx, 'role': idx, 'name': 'user%s' % idx}
        user = User(**attrs)
        user._commit()
        users.append(user)
    multi_thread_test(_test_insert, [], {}, tidx_name='idx', num_threads=num_threads, num_per_thread=num_per_thread)

    db_users = User._by_id([user._id for user in users], ignore_cache=True, return_dict=False)
    assert list(users).sort(key=lambda item: item._id) == list(db_users).sort(key=lambda item: item._id)
    assert len(users) == num_threads * num_per_thread
    for user in users:
        assert user._ups == user._downs
        assert 0 <= int(user._ups) < num_threads

    u = users[0]
    def _test_update(idx):
        u._ups = int(idx)
        u.name = 'user%s' % idx
        u.asdf = 'new prop #' + idx
        u._commit()
        #remote_u = User._by_id(u._id, ignore_cache=True)  # maybe modified by other thread
        #assert remote_u._ups == u._ups
        #assert remote_u.name == u.name
    multi_thread_test(_test_update, [], {}, tidx_name='idx', num_threads=num_threads, num_per_thread=num_per_thread)


def test_multi_cache():
    import datetime
    start = datetime.datetime.now()
    multi_thread_test(test_cache, [cache], {}, tidx_name='prefix', num_threads=num_threads, num_per_thread=num_per_thread)
    end = datetime.datetime.now()
    print("time: %s" % (end-start))


def main():
    test_multi_cache()
    test_db_multi_process()

if __name__ == '__main__':
    backend.create_tables(reset_tables=True)
    main()