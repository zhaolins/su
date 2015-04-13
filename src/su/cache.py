import memcache
from threading import local
from hashlib import md5
from contextlib import contextmanager
from queue import Queue
from su.redix import InterpretedRedis, NoneHolder
from su.util import call_with_prefix_keys
from su.env import LOGGER

#import pylibmc
#from _pylibmc import MemcachedError

# This is for use in the health controller
_CACHE_SERVERS = set()


class CacheUtils(object):
    # Caches that never expire entries should set this to true, so that
    # CacheChain can properly count hits and misses.
    permanent = False

    def incr(self, key, delta=1, time=0):
        raise NotImplementedError

    def incr_multi(self, keys, delta=1, prefix=''):
        for k in keys:
            try:
                self.incr(prefix + k, delta)
            except ValueError:
                pass

    def add(self, key, val, time=0):
        raise NotImplementedError

    def add_multi(self, keys, prefix='', time=0):
        results = []
        for k, v in keys.items():
            r = self.add(prefix+str(k), v, time=time)
            results.append(r)
        return {key: results[i] for i, key in enumerate(keys)}

    def simple_get_multi(self, keys, **kw):
        raise NotImplementedError

    def get_multi(self, keys, prefix='', **kw):
        return call_with_prefix_keys(keys, lambda k: self.simple_get_multi(k, **kw), prefix)


class ClientPool(Queue):
    def __init__(self, mc=None, n_slots=None):
        Queue.__init__(self, n_slots)
        if mc is not None:
            self.fill(mc, n_slots)

    @contextmanager
    def reserve(self, block=True):
        """Context manager for reserving a client from the pool.

        If *block* is given and the pool is exhausted, the pool waits for
        another thread to fill it before returning.
        """
        mc = self.get(block)
        try:
            yield mc
        finally:
            self.put(mc)

    def fill(self, mc, n_slots):
        """Fill *n_slots* of the pool with clones of *mc*."""
        for i in range(n_slots):
            self.put(mc.clone())


class CMemcache(CacheUtils):
    def __init__(self,
                 servers,
                 debug=False,
                 noreply=False,
                 no_block=False,
                 min_compress_len=512 * 1024,
                 num_clients=10,
                 **kwargs):
        self.servers = servers
        self.clients = ClientPool(n_slots=num_clients)
        for x in range(num_clients):
            client = memcache.Client(servers, **kwargs)#, binary=True)
            behaviors = {
                'no_block': no_block, # use async I/O
                'tcp_nodelay': True, # no nagle
                '_noreply': int(noreply),
                'ketama': True, # consistent hashing
                }

            #client.behaviors.update(behaviors)
            self.clients.put(client)

        self.min_compress_len = min_compress_len

        _CACHE_SERVERS.update(servers)

    def get(self, key, default = None):
        with self.clients.reserve() as mc:
            ret = mc.get(key)
            if ret is None:
                return default
            return ret

    def get_multi(self, keys, prefix='', **kw):
        with self.clients.reserve() as mc:
            return mc.get_multi(keys, key_prefix=prefix)

    # simple_get_multi exists so that a cache chain can
    # single-instance the handling of prefixes for performance, but
    # pylibmc does this in C which is faster anyway, so CMemcache
    # implements get_multi itself. But the CacheChain still wants
    # simple_get_multi to be available for when it's already prefixed
    # them, so here it is
    simple_get_multi = get_multi

    def set(self, key, val, time=0):
        with self.clients.reserve() as mc:
            return mc.set(key, val, time=time, min_compress_len=self.min_compress_len)

    def set_multi(self, keys, prefix='', time=0):
        new_keys = {}
        for k, v in keys.items():
            new_keys[str(k)] = v
        with self.clients.reserve() as mc:
            return mc.set_multi(new_keys, key_prefix=prefix,
                                time=time, min_compress_len=self.min_compress_len)

    #def add_multi(self, keys, prefix='', time=0):
    #    new_keys = {}
    #    for k,v in keys.items():
    #        new_keys[str(k)] = v
    #    with self.clients.reserve() as mc:
    #        return mc.add_multi(new_keys, key_prefix = prefix,
    #                            time = time)

    #def incr_multi(self, keys, prefix='', delta=1):
    #    with self.clients.reserve() as mc:
    #        return mc.incr_multi(map(str, keys),
    #                             key_prefix = prefix,
    #                             delta=delta)

    def append(self, key, val, time=0):
        with self.clients.reserve() as mc:
            return mc.append(key, val, time=time)

    def incr(self, key, delta=1, time=0):
        # ignore the time on these
        with self.clients.reserve() as mc:
            return mc.incr(key, delta)

    def add(self, key, val, time=0):
        #try:
        with self.clients.reserve() as mc:
            return mc.add(key, val, time=time)
        #except memcache.DataExists:
        #    return None

    def delete(self, key, time=0):
        with self.clients.reserve() as mc:
            return mc.delete(key)

    def delete_multi(self, keys, prefix=''):
        with self.clients.reserve() as mc:
            return mc.delete_multi(keys, key_prefix=prefix)

    def flush(self):
        with self.clients.reserve() as mc:
            return mc.flush_all()

    def __repr__(self):
        return '<%s(%r)>' % (self.__class__.__name__,
                             self.servers)


class RedisCache(CacheUtils):
    permanent = True

    def __init__(self, pool):
        self.client = InterpretedRedis(connection_pool=pool)
        self.server = "%s:%s" % (pool.connection_kwargs['host'], pool.connection_kwargs['port'])
        _CACHE_SERVERS.update(self.server)

    def get(self, key, default=None):
        try:
            if self.client[key] is NoneHolder:
                return None
            return self.client[key]
        except KeyError:
            return default

    def simple_get_multi(self, keys, **kw):
        results = self.client.mget(keys)
        return {k: results[i] for i, k in enumerate(keys) if results[i] is not None}

    def set(self, key, val, time=0):
        if key is None:
            return False
        if time == 0:
            return self.client.set(key, val)
        if time > 0:
            return self.client.setex(key, time, val)
        else:
            return False

    def set_multi(self, keys, prefix='', time=0):
        new_keys = {}
        for k, v in keys.items():
            new_keys[str(k)] = v
        if not new_keys:
            return False
        if time == 0:
            func = lambda x: {prefix+key: True for key in new_keys} if self.client.mset(x) \
                else {prefix+key: False for key in new_keys}
            return call_with_prefix_keys(new_keys, func, prefix)
        elif time > 0:
            func = lambda x: {prefix+key: True for key in new_keys} if self.client.msetex(x, time) \
                else {prefix+key: False for key in new_keys}
            return call_with_prefix_keys(new_keys, func, prefix)
        else:
            return False

    def add_multi(self, data, prefix='', time=0):
        _data = {}
        for k, v in data.items():
            _data[str(k)] = v
        if not _data:
            return False
        if time == 0:
            def callback(_d):
                rets = self.client.madd(_d)
                return {key: rets[i] for i, key in enumerate(_d)}
            result = call_with_prefix_keys(_data, callback, prefix)
        elif time > 0:
            def callback(_d):
                rets = self.client.maddex(_d, time)
                return {key: rets[i] for i, key in enumerate(_d)}
            result = call_with_prefix_keys(_data, callback, prefix)
        else:
            result = {key: False for key in _data}
        return result

    def incr_multi(self, keys, delta=1, prefix=''):
        if not keys:
            return False
        exists = self.get_multi(keys, prefix=prefix)
        def callback(_d):
            rets = self.client.mincr(_d, delta)
            return {key: rets[i] for i, key in enumerate(_d)}
        return call_with_prefix_keys(exists.keys(), callback, prefix)

    def append(self, key, val, time=0):
        if key is None:
            return False
        # todo: removed b(val)
        return self.client.append(key, val)

    def incr(self, key, delta=1, time=0):
        if key is None:
            return False
        if self.client.exists(key):
            return self.client.incr(key, delta)
        else:
            return False

    def add(self, key, val, time=0):
        if time == 0:
            return self.client.set(key, val, nx=True)
        elif time > 0:
            return self.client.set(key, val, nx=True, ex=time)
        else:
            return False

    def delete(self, key, time=0):
        if key is None:
            return False
        return self.client.delete(key)

    def delete_multi(self, keys, prefix=''):
        if keys is None:
            return False

        def callback(k):
            deleted = self.client.delete(*k)
            return {key: True if deleted else False for key in k}
        return call_with_prefix_keys(keys, callback, prefix)

    def flush(self):
        return self.client.flushdb()

    def flushall(self):
        return self.client.flushall()

    def __repr__(self):
        return '<%s(%r)>' % (self.__class__.__name__, self.server)


class LocalCache(dict, CacheUtils):
    def __init__(self, *a, **kw):
        dict.__init__(self, *a, **kw)

    @staticmethod
    def _check_key(key):
        if not isinstance(key, str):
            raise TypeError('Key is not a string: %r' % (key,))

    def get(self, key, default=None):
        r = dict.get(self, key)
        return r if r is not None else default

    def simple_get_multi(self, keys, **kw):
        out = {}
        for k in keys:
            if k in self:
                out[k] = self[k]
        return out

    def set(self, key, val, time=0):
        # time is ignored on localcache
        self._check_key(key)
        self[key] = val

    def set_multi(self, keys, prefix='', time=0):
        for k, v in keys.items():
            self.set(prefix+str(k), v, time=time)

    def add(self, key, val, time=0):
        self._check_key(key)
        was = key in self
        self.setdefault(key, val)
        return not was

    def delete(self, key):
        if key in self:
            del self[key]

    def delete_multi(self, keys, prefix=''):
        for key in keys:
            k = prefix+str(key)
            if k in self:
                del self[k]

    def incr(self, key, delta=1, time=0):
        if key in self:
            self[key] = int(self[key]) + delta

    def decr(self, key, amt=1):
        if key in self:
            self[key] = int(self[key]) - amt

    def append(self, key, val, time = 0):
        if key in self:
            self[key] = str(self[key]) + val

    def prepend(self, key, val, time = 0):
        if key in self:
            self[key] = val + str(self[key])

    def replace(self, key, val, time = 0):
        if key in self:
            self[key] = val

    def flush_all(self):
        self.clear()

    def __repr__(self):
        return "<LocalCache(%d)>" % (len(self),)


def make_set_fn(fn_name):
    def fn(self, *a, **kw):
        ret = None
        for c in self.caches:
            ret = getattr(c, fn_name)(*a, **kw)
        LOGGER.debug("[cache] %s '%s'" % (fn_name, a[0] if len(a) else None))
        return ret
    return fn


class CacheChain(CacheUtils, local):
    def __init__(self, caches, cache_negative_results=False):
        super().__init__()
        self.caches = caches
        self.cache_negative_results = cache_negative_results
        self.stats = None
    # note that because of the naive nature of `add' when used on a
    # cache chain, its return value isn't reliable. if you need to
    # verify its return value you'll either need to make it smarter or
    # use the underlying cache directly
    add = make_set_fn('add')

    set = make_set_fn('set')
    append = make_set_fn('append')
    prepend = make_set_fn('prepend')
    replace = make_set_fn('replace')
    set_multi = make_set_fn('set_multi')
    add_multi = make_set_fn('add_multi')
    incr = make_set_fn('incr')
    incr_multi = make_set_fn('incr_multi')
    decr = make_set_fn('decr')
    delete = make_set_fn('delete')
    delete_multi = make_set_fn('delete_multi')
    flush_all = make_set_fn('flush_all')
    cache_negative_results = False

    def get(self, key, default=None, allow_local=True):
        stat_outcome = False  # assume a miss until a result is found
        found_in = None
        try:
            for c in self.caches:
                if not allow_local and isinstance(c, LocalCache):
                    continue

                val = c.get(key)
                if val is not None:
                    found_in = str(c)
                    if not c.permanent:
                        stat_outcome = True

                    #update other caches
                    for d in self.caches:
                        if c is d:
                            break  # so we don't set caches later in the chain
                        d.set(key, val)

                    if val is NoneHolder:
                        return default
                    else:
                        return val

            if self.cache_negative_results:
                for c in self.caches[:-1]:
                    c.set(key, NoneHolder)

            return default
        finally:
            LOGGER.debug("[cache] get '%s', %s" % (key, 'found in ' + found_in if found_in else 'not found in cache'))
            if self.stats:
                if stat_outcome:
                    self.stats.cache_hit()
                else:
                    self.stats.cache_miss()

    def get_multi(self, keys, prefix='', allow_local=True, **kw):
        l = lambda ks: self.simple_get_multi(ks, allow_local=allow_local, **kw)
        return call_with_prefix_keys(keys, l, prefix)

    def simple_get_multi(self, keys, allow_local=True, **kw):
        out = {}
        need = set(keys)
        hits = 0
        misses = 0
        local_hit = 0
        for c in self.caches:
            if not allow_local and isinstance(c, LocalCache):
                continue
            if c.permanent and not misses:
                # Once we reach a "permanent" cache, we count any outstanding
                # items as misses.
                misses = len(need)

            if len(out) == len(keys):
                # we've found them all
                break
            r = c.simple_get_multi(need)
            #update other caches
            if r:
                if not c.permanent:
                    hits += len(r)
                    if isinstance(c, LocalCache):
                        local_hit += len(r)
                for d in self.caches:
                    if c is d:
                        break # so we don't set caches later in the chain
                    d.set_multi(r)
                r.update(out)
                out = r
                need = need - set(r.keys())

        if need and self.cache_negative_results:
            d = dict((key, NoneHolder) for key in need)
            for c in self.caches[:-1]:
                c.set_multi(d)

        out = dict((k, v)
                   for (k, v) in out.items()
                   if v != NoneHolder)

        LOGGER.debug("[cache] simple_get_multi '%s', found %s, local: %s" % (list(keys), hits, local_hit))

        if self.stats:
            if not misses:
                # If this chain contains no permanent caches, then we need to
                # count the misses here.
                misses = len(need)
            self.stats.cache_hit(hits)
            self.stats.cache_miss(misses)

        return out

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__,
                            self.caches)

    def debug(self, key):
        print("Looking up [%r]" % key)
        for i, c in enumerate(self.caches):
            print("[%d] %10s has value [%r]" % (i, c.__class__.__name__,
                                                c.get(key)))

    def reset_local(self):
        # the first item in a cache chain is a LocalCache
        self.caches = (self.caches[0].__class__(),) + self.caches[1:]


class MemcacheChain(CacheChain):
    pass


class RedisChain(CacheChain):
    def add(self, key, val, time=0):
        authority = self.caches[-1]
        success = authority.add(key, val, time=time)
        v = val if success else authority.get(key)
        for cache in self.caches[:-1]:
            cache.set(key, v, time=time)

        return success

    def accrue(self, key, time=0, delta=1):
        auth_value = self.caches[-1].get(key)

        if auth_value is None:
            auth_value = 0

        try:
            auth_value = int(auth_value) + delta
        except ValueError:
            raise ValueError("Can't accrue %s; it's a %s (%r)" %
                             (key, auth_value.__class__.__name__, auth_value))

        for c in self.caches:
            c.set(key, auth_value, time=time)

        return auth_value


def test_cache(cache, prefix=''):
    #basic set/get
    cache.set('%s1' % prefix, 1)
    assert cache.get('%s1' % prefix) == 1

    #python data
    cache.set('%s2' % prefix, [1,2,3])
    assert cache.get('%s2' % prefix) == [1,2,3]

    #set multi, no prefix
    cache.set_multi({'%s3' % prefix:3, '%s4' % prefix: 4})
    assert cache.get_multi(('%s3' % prefix, '%s4' % prefix)) == {'%s3' % prefix: 3,
                                                                 '%s4' % prefix: 4}

    #set multi, prefix
    cache.set_multi({'3':3, '4': 4}, prefix='%sp_' % prefix)
    assert cache.get_multi((1, 2, '3', 4), prefix='%sp_' % prefix) == {'3':3, 4: 4}
    assert cache.get_multi(('%sp_3' % prefix, '%sp_4' % prefix)) == {'%sp_3'%prefix: 3,
                                                                     '%sp_4'%prefix: 4}

    # delete
    cache.set('%s1'%prefix, 1)
    assert cache.get('%s1'%prefix) == 1
    cache.delete('%s1'%prefix)
    assert cache.get('%s1'%prefix) is None

    cache.set('%s1'%prefix, 1)
    cache.set('%s2'%prefix, 2)
    cache.set('%s3'%prefix, 3)
    assert cache.get('%s1'%prefix) == 1 and cache.get('%s2'%prefix) == 2
    cache.delete_multi(['%s1'%prefix, '%s2'%prefix])
    assert (cache.get('%s1'%prefix) is None
            and cache.get('%s2'%prefix) is None
            and cache.get('%s3'%prefix) == 3)

    #incr
    cache.set('%s5'%prefix, 1)
    cache.set('%s6'%prefix, 1)
    cache.incr('%s5'%prefix)
    assert cache.get('%s5'%prefix) == 2
    cache.incr('%s5'%prefix,2)
    assert cache.get('%s5'%prefix) == 4
    cache.incr_multi(('%s5'%prefix, '%s6'%prefix), 1)
    assert cache.get('%s5'%prefix) == 5
    assert cache.get('%s6'%prefix) == 2
    cache.incr_multi(('5', '6'), -1, prefix=prefix)
    assert cache.get('%s5'%prefix) == 4
    assert cache.get('%s6'%prefix) == 1
    # c = cache
    # c.flush_all()
    # assert c.get('%s0' % prefix) is None
    # c.add_multi({'0': False}, prefix=prefix)
    # assert c.get('%s0' % prefix) is False
    # c.set('%s0' % prefix, 'False')
    # assert c.get('%s0' % prefix) == 'False'
    #
    # obj = {'0': 'fdsa'}
    # c.set('%sobj' % prefix, obj)
    # assert c.get('%sobj' % prefix) == obj
    #
    # c.set('%s1' % prefix, 1)
    # assert c.get('%s1' % prefix) == 1
    # assert c.add('%s1' % prefix, 2) is False
    # assert c.get('%s1' % prefix) == 1
    #
    # #python data
    # c.set('%s2' % prefix, [1, 2, 3])
    # assert c.get('%s2' % prefix), [1, 2 == 3]
    # assert c.add_multi({'1': 2, '2': 3, '3': '3'}, prefix=prefix) ==\
    #        {'1': False,
    #         '2': False,
    #         '3': True}
    # assert c.get_multi(('%s1' % prefix, '%s2' % prefix, '%s3' % prefix)) ==\
    #        {'%s1' % prefix: 1,
    #         '%s2' % prefix: [1, 2, 3],
    #         '%s3' % prefix: '3'}
    # assert c.get_multi(('1', '2', '3'), prefix=prefix) ==\
    #        {'1': 1,
    #         '2': [1, 2, 3],
    #         '3': '3'}
    #
    # #set multi, no prefix
    # c.set_multi({'%s3' % prefix: 3, '%s4' % prefix: 4})
    # assert c.get_multi(('%s3' % prefix, '%s4' % prefix)) == {'%s3' % prefix: 3,
    #                                                          '%s4' % prefix: 4}
    #
    # #set multi, prefix
    # c.set_multi({'3': 3, '4': 4}, prefix='%sp_' % prefix)
    # assert c.get_multi((1, 2, '3', 4), prefix='%sp_' % prefix) == {'3': 3, 4: 4}
    # assert c.get_multi(('%sp_3' % prefix, '%sp_4' % prefix)) == {'%sp_3' % prefix: 3, '%sp_4' % prefix: 4}
    #
    # # delete
    # c.set('%s1' % prefix, 1)
    # assert c.get('%s1' % prefix) == 1
    # c.delete('%s1' % prefix)
    # assert c.get('%s1' % prefix) is None
    #
    # c.set('%s1' % prefix, 1)
    # c.set('%s2' % prefix, 2)
    # c.set('%s3' % prefix, 3)
    # assert c.get('%s1' % prefix) == 1
    # assert c.get('%s2' % prefix) == 2
    # c.delete_multi(['%s1' % prefix, '%s2' % prefix])
    # assert (c.get('%s1' % prefix) is None and
    #         c.get('%s2' % prefix) is None and
    #         c.get('%s3' % prefix) == 3)
    #
    # c.set_multi({'%s1' % prefix: 1, '%s2' % prefix: 2, '%s3' % prefix: 3})
    # assert c.get_multi((1, '2', 3), prefix=prefix) == {1: 1, '2': 2, 3: 3}
    # c.delete_multi(['1', '2'], prefix=prefix)
    # assert (c.get('%s1' % prefix) is None and
    #         c.get('%s2' % prefix) is None and
    #         c.get('%s3' % prefix) == 3)
    #
    # #incr
    # c.set('%s5' % prefix, 1)
    # c.set('%s6' % prefix, 1)
    # c.incr('%s5' % prefix)
    # assert c.get('%s5' % prefix) == 2
    # c.incr('%s5' % prefix, 2)
    # assert c.get('%s5' % prefix) == 4
    #
    # c.incr_multi(('%s5' % prefix, '%s6' % prefix), 1)
    # assert c.get('%s5' % prefix) == 5
    # assert c.get('%s6' % prefix) == 2
    #
    # c.incr_multi(('5', '6'), 1, prefix=prefix)
    # assert c.get('%s5' % prefix) == 6
    # assert c.get('%s6' % prefix) == 3


# a cache that occasionally dumps itself to be used for long-running
# processes
class SelfEmptyingCache(LocalCache):
    def __init__(self, max_size=10*1000):
        self.max_size = max_size

    def maybe_reset(self):
        if len(self) > self.max_size:
            self.clear()

    def set(self, key, val, time=0):
        self.maybe_reset()
        return LocalCache.set(self, key, val, time)

    def add(self, key, val, time=0):
        self.maybe_reset()
        return LocalCache.add(self, key, val)


def make_key(iden, *a, **kw):
    """
    A helper function for making memcached-usable cache keys out of
    arbitrary arguments. Hashes the arguments but leaves the `iden'
    human-readable
    """
    h = md5()

    def _conv(s):
        if isinstance(s, str):
            return s
        elif isinstance(s, (tuple, list)):
            return ','.join(_conv(x) for x in s)
        elif isinstance(s, dict):
            return ','.join('%s:%s' % (_conv(k), _conv(v))
                            for (k, v) in sorted(s.items()))
        else:
            return str(s)

    iden = _conv(iden)
    h.update(iden)
    h.update(_conv(a))
    h.update(_conv(kw))

    return '%s(%s)' % (iden, h.hexdigest())
