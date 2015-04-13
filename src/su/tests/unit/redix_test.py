from su.tests import test_env
from string import ascii_letters
from su.redix import InterpretedRedis, ConnectionPool, NoneHolder, StrictRedis
from su.g import flush_cache, flush_permacache, permacache_client
from distutils.version import StrictVersion
import redis
import datetime
import binascii
import time
import unittest

iteritems = lambda x: iter(x.items())
iterkeys = lambda x: iter(x.keys())
itervalues = lambda x: iter(x.values())
unichr = chr
u = lambda x: x
sort = lambda x: sorted(x, key=lambda y: str(y))

#b = lambda x: x.encode('iso-8859-1') if not isinstance(x, bytes) else xf
#b = lambda x: x

r = permacache_client
version = r.info()['redis_version']
version_lt = lambda v: StrictVersion(version) < StrictVersion(v)
db_idx = r.connection_pool.connection_kwargs['db']


def redis_server_time():
    seconds, milliseconds = r.time()
    timestamp = float('%s.%s' % (seconds, milliseconds))
    return datetime.datetime.fromtimestamp(timestamp)


class CommandsTests(unittest.TestCase):
    def setUp(self):
        flush_permacache()

    def tearDown(self):
        flush_permacache()

    def test_command_on_invalid_key_type(self):
        r.lpush('a', '1')

        with self.assertRaises(redis.ResponseError):
            r['a']

    ### SERVER INFORMATION ###
    def test_client_list(self):
        clients = r.client_list()
        self.assertTrue(isinstance(clients[0], dict))
        self.assertTrue('addr' in clients[0])

    def test_client_getname(self):
        self.assertTrue(r.client_getname() is None)

    def test_client_setname(self):
        self.assertTrue(r.client_setname('redis_py_test'))
        self.assertEqual(r.client_getname(), 'redis_py_test')

    def test_config_get(self):
        data = r.config_get()
        self.assertTrue('maxmemory' in data)
        self.assertTrue(data['maxmemory'].isdigit())

    def test_config_resetstat(self):
        r.ping()
        self.assertTrue(int(r.info()['total_commands_processed']) > 1)
        r.config_resetstat()
        self.assertEqual(int(r.info()['total_commands_processed']), 1)

    def test_config_set(self):
        data = r.config_get()
        rdbname = data['dbfilename']
        try:
            self.assertTrue(r.config_set('dbfilename', 'redis_py_test.rdb'))
            self.assertEqual(r.config_get()['dbfilename'], 'redis_py_test.rdb')
        finally:
            self.assertTrue(r.config_set('dbfilename', rdbname))

    def test_dbsize(self):
        r['a'] = 'foo'
        r['b'] = 'bar'
        self.assertEqual(r.dbsize(), 2)

    def test_debug_object(self):
        r['a'] = 'foo'
        debug_info = r.debug_object('a')
        self.assertTrue(len(debug_info) > 0)
        self.assertTrue('refcount' in debug_info)
        self.assertEqual(debug_info['refcount'], 1)

    def test_echo(self):
        self.assertEqual(r.echo('foo bar'), 'foo bar')

    def test_info(self):
        r['a'] = 'foo'
        r['b'] = 'bar'
        info = r.info()
        self.assertTrue(isinstance(info, dict))
        self.assertEqual(info['db%s' % db_idx]['keys'], 2)

    def test_lastsave(self):
        self.assertTrue(isinstance(r.lastsave(), datetime.datetime))

    def test_object(self):
        r['a'] = 'foo'
        self.assertTrue(isinstance(r.object('refcount', 'a'), int))
        self.assertTrue(isinstance(r.object('idletime', 'a'), int))
        self.assertEqual(r.object('encoding', 'a'), b'raw')

    def test_ping(self):
        self.assertTrue(r.ping())

    def test_time(self):
        t = r.time()
        self.assertEqual(len(t), 2)
        self.assertTrue(isinstance(t[0], int))
        self.assertTrue(isinstance(t[1], int))

    ### BASIC KEY COMMANDS ###
    def test_append(self):
        self.assertEqual(r.append('a', b'a1'), 2)
        self.assertEqual(r['a'], b'a1')
        self.assertEqual(r.append('a', b'a2'), 4)
        self.assertEqual(r['a'], b'a1a2')

    def test_bitcount(self):
        r.setbit('a', 5, True)
        self.assertEqual(r.bitcount('a'), 1)
        r.setbit('a', 6, True)
        self.assertEqual(r.bitcount('a'), 2)
        r.setbit('a', 5, False)
        self.assertEqual(r.bitcount('a'), 1)
        r.setbit('a', 9, True)
        r.setbit('a', 17, True)
        r.setbit('a', 25, True)
        r.setbit('a', 33, True)
        self.assertEqual(r.bitcount('a'), 5)
        self.assertEqual(r.bitcount('a', 0, -1), 5)
        self.assertEqual(r.bitcount('a', 2, 3), 2)
        self.assertEqual(r.bitcount('a', 2, -1), 3)
        self.assertEqual(r.bitcount('a', -2, -1), 2)
        self.assertEqual(r.bitcount('a', 1, 1), 1)

    def test_bitop_not_empty_string(self):
        r['a'] = b''
        r.bitop('not', 'r', 'a')
        self.assertTrue(r.get('r') is None)

    def test_bitop_not(self):
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r['a'] = test_str
        r.bitop('not', 'r', 'a')
        self.assertEqual(int(binascii.hexlify(r['r']), 16), correct)

    def test_bitop_not_in_place(self):
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r['a'] = test_str
        r.bitop('not', 'a', 'a')
        self.assertEqual(int(binascii.hexlify(r['a']), 16), correct)

    def test_bitop_single_string(self):
        test_str = b'\x01\x02\xFF'
        r['a'] = test_str
        r.bitop('and', 'res1', 'a')
        r.bitop('or', 'res2', 'a')
        r.bitop('xor', 'res3', 'a')
        self.assertEqual(r['res1'], test_str)
        self.assertEqual(r['res2'], test_str)
        self.assertEqual(r['res3'], test_str)

    def test_bitop_string_operands(self):
        r['a'] = b'\x01\x02\xFF\xFF'
        r['b'] = b'\x01\x02\xFF'
        r.bitop('and', 'res1', 'a', 'b')
        r.bitop('or', 'res2', 'a', 'b')
        r.bitop('xor', 'res3', 'a', 'b')
        self.assertEqual(int(binascii.hexlify(r['res1']), 16), 0x0102FF00)
        self.assertEqual(int(binascii.hexlify(r['res2']), 16), 0x0102FFFF)
        self.assertEqual(int(binascii.hexlify(r['res3']), 16), 0x000000FF)

    def test_decr(self):
        self.assertEqual(r.decr('a'), -1)
        self.assertEqual(r['a'], -1)
        self.assertEqual(r.decr('a'), -2)
        self.assertEqual(r['a'], -2)
        self.assertEqual(r.decr('a', amount=5), -7)
        self.assertEqual(r['a'], -7)

    def test_delete(self):
        self.assertEqual(r.delete('a'), 0)
        r['a'] = 'foo'
        self.assertEqual(r.delete('a'), 1)

    def test_delete_with_multiple_keys(self):
        r['a'] = 'foo'
        r['b'] = 'bar'
        self.assertEqual(r.delete('a', 'b'), 2)
        self.assertTrue(r.get('a') is None)
        self.assertTrue(r.get('b') is None)

    def test_delitem(self):
        r['a'] = 'foo'
        del r['a']
        self.assertTrue(r.get('a') is None)

    def test_dump_and_restore(self):
        r['a'] = 'foo'
        dumped = r.dump('a')
        del r['a']
        r.restore('a', 0, dumped)
        self.assertEqual(r['a'], 'foo')

    def test_exists(self):
        self.assertFalse(r.exists('a'))
        r['a'] = 'foo'
        self.assertTrue(r.exists('a'))

    def test_exists_contains(self):
        self.assertTrue('a' not in r)
        r['a'] = 'foo'
        self.assertTrue('a' in r)

    def test_expire(self):
        self.assertFalse(r.expire('a', 10))
        r['a'] = 'foo'
        self.assertTrue(r.expire('a', 10))
        self.assertTrue(0 < r.ttl('a') <= 10)
        self.assertTrue(r.persist('a'))
        self.assertEqual(r.ttl('a'), -1)

    def test_expireat_datetime(self):
        expire_at = redis_server_time() + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        self.assertTrue(r.expireat('a', expire_at))
        self.assertTrue(0 < r.ttl('a') <= 61)

    def test_expireat_no_key(self):
        expire_at = redis_server_time() + datetime.timedelta(minutes=1)
        self.assertFalse(r.expireat('a', expire_at))

    def test_expireat_unixtime(self):
        expire_at = redis_server_time() + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        expire_at_seconds = int(time.mktime(expire_at.timetuple()))
        self.assertTrue(r.expireat('a', expire_at_seconds))
        self.assertTrue(0 < r.ttl('a') <= 61)

    def test_packing(self):
        def test_value(v):
            key = 'key'
            r.set(key, v)
            got = r.get(key)
            self.assertEqual(type(v), type(got))
            self.assertEqual(v, got)
        for value in [True, False, 1, '', b'', b'\x10', b'&', b'&p:', b'&b', '1', '&s:', '%p:', 2.3, 
                      unichr(3456) + u('abcd') + unichr(3421)]:
            test_value(value)

        r['a'] = None
        self.assertTrue(r['a'] is NoneHolder)

        # b'1' -> 1
        # None -> NoneHolder

    def test_get_and_set(self):
        # get and set can't be tested independently of each other
        self.assertTrue(r.get('a') is None)
        byte_string = 'value'
        integer = 5
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        self.assertTrue(r.set('byte_string', byte_string))
        self.assertTrue(r.set('integer', 5))
        self.assertTrue(r.set('unicode_string', unicode_string))
        self.assertEqual(r.get('byte_string'), byte_string)
        self.assertEqual(r.get('integer'), integer)
        self.assertEqual(r.get('unicode_string'), unicode_string)

    def test_getitem_and_setitem(self):
        r['a'] = 'bar'
        self.assertEqual(r['a'], 'bar')

    def test_get_set_bit(self):
        # no value
        self.assertFalse(r.getbit('a', 5))
        # set bit 5
        self.assertFalse(r.setbit('a', 5, True))
        self.assertTrue(r.getbit('a', 5))
        # unset bit 4
        self.assertFalse(r.setbit('a', 4, False))
        self.assertFalse(r.getbit('a', 4))
        # set bit 4curl -sS https://getcomposer.org/installer | php
        self.assertFalse(r.setbit('a', 4, True))
        self.assertTrue(r.getbit('a', 4))
        # set bit 5 again
        self.assertTrue(r.setbit('a', 5, True))
        self.assertTrue(r.getbit('a', 5))

    def test_getrange(self):
        r['a'] = b'foo'
        self.assertEqual(r.getrange('a', 0, 0), b'f')
        self.assertEqual(r.getrange('a', 0, 2), b'foo')
        self.assertEqual(r.getrange('a', 3, 4), b'')

    def test_getset(self):
        self.assertTrue(r.getset('a', 'foo') is None)
        self.assertEqual(r.getset('a', 'bar'), 'foo')
        self.assertEqual(r.get('a'), 'bar')

    def test_incr(self):
        self.assertEqual(r.incr('a'), 1)
        self.assertEqual(r['a'], 1)
        self.assertEqual(r.incr('a'), 2)
        self.assertEqual(r['a'], 2)
        self.assertEqual(r.incr('a', amount=5), 7)
        self.assertEqual(r['a'], 7)

    def test_incrby(self):
        self.assertEqual(r.incrby('a'), 1)
        self.assertEqual(r.incrby('a', 4), 5)
        self.assertEqual(r['a'], 5)

    def test_incrbyfloat(self):
        self.assertEqual(r.incrbyfloat('a'), 1.0)
        self.assertEqual(r['a'], 1)
        self.assertEqual(r.incrbyfloat('a', 1.1), 2.1)
        self.assertEqual(float(r['a']), float(2.1))

    def test_keys(self):
        self.assertEqual(r.keys(), [])
        keys_with_underscores = {'test_a', 'test_b'}
        keys = keys_with_underscores.union({'testc'})
        for key in keys:
            r[key] = 1
        self.assertEqual(set(r.keys(pattern='test_*')), keys_with_underscores)
        self.assertEqual(set(r.keys(pattern='test*')), keys)

    def test_mget(self):
        self.assertEqual(r.mget(['a', 'b']), [None, None])
        r['a'] = '1'
        r['b'] = '2'
        r['c'] = '3'
        self.assertEqual(r.mget('a', 'other', 'b', 'c'), ['1', None, '2', '3'])

    def test_mset(self):
        d = {'a': '1', 'b': '2', 'c': '3'}
        self.assertTrue(r.mset(d))
        for k, v in iteritems(d):
            self.assertEqual(r[k], v)

    def test_mset_kwargs(self):
        d = {'a': '1', 'b': '2', 'c': '3'}
        self.assertTrue(r.mset(**d))
        for k, v in iteritems(d):
            self.assertEqual(r[k], v)

    def test_msetnx(self):
        d = {'a': '1', 'b': '2', 'c': '3'}
        self.assertTrue(r.msetnx(d))
        d2 = {'a': 'x', 'd': '4'}
        self.assertFalse(r.msetnx(d2))
        for k, v in iteritems(d):
            self.assertEqual(r[k], v)
        self.assertTrue(r.get('d') is None)

    def test_msetnx_kwargs(self):
        d = {'a': '1', 'b': '2', 'c': '3'}
        self.assertTrue(r.msetnx(**d))
        d2 = {'a': 'x', 'd': '4'}
        self.assertFalse(r.msetnx(**d2))
        for k, v in iteritems(d):
            self.assertEqual(r[k], v)
        self.assertTrue(r.get('d') is None)

    def test_pexpire(self):
        self.assertFalse(r.pexpire('a', 60000))
        r['a'] = 'foo'
        self.assertTrue(r.pexpire('a', 60000))
        self.assertTrue(0 < r.pttl('a') <= 60000)
        self.assertTrue(r.persist('a'))
        self.assertEqual(r.pttl('a'), -1)

    def test_pexpireat_datetime(self):
        expire_at = redis_server_time() + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        self.assertTrue(r.pexpireat('a', expire_at))
        self.assertTrue(0 < r.pttl('a') <= 61000)

    def test_pexpireat_no_key(self):
        expire_at = redis_server_time() + datetime.timedelta(minutes=1)
        self.assertFalse(r.pexpireat('a', expire_at))

    def test_pexpireat_unixtime(self):
        expire_at = redis_server_time() + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        expire_at_seconds = int(time.mktime(expire_at.timetuple())) * 1000
        self.assertTrue(r.pexpireat('a', expire_at_seconds))
        self.assertTrue(0 < r.pttl('a') <= 61000)

    def test_psetex(self):
        self.assertTrue(r.psetex('a', 1000, 'value'))
        self.assertEqual(r['a'], 'value')
        self.assertTrue(0 < r.pttl('a') <= 1000)

    def test_psetex_timedelta(self):
        expire_at = datetime.timedelta(milliseconds=1000)
        self.assertTrue(r.psetex('a', expire_at, 'value'))
        self.assertEqual(r['a'], 'value')
        self.assertTrue(0 < r.pttl('a') <= 1000)

    def test_randomkey(self):
        self.assertTrue(r.randomkey() is None)
        for key in ('a', 'b', 'c'):
            r[key] = 1
        self.assertTrue(r.randomkey() in ('a', 'b', 'c'))

    def test_rename(self):
        r['a'] = '1'
        self.assertTrue(r.rename('a', 'b'))
        self.assertTrue(r.get('a') is None)
        self.assertEqual(r['b'], '1')

    def test_renamenx(self):
        r['a'] = '1'
        r['b'] = '2'
        self.assertFalse(r.renamenx('a', 'b'))
        self.assertEqual(r['a'], '1')
        self.assertEqual(r['b'], '2')

    def test_set_nx(self):
        self.assertTrue(r.set('a', '1', nx=True))
        self.assertFalse(r.set('a', '2', nx=True))
        self.assertEqual(r['a'], '1')

    def test_set_xx(self):
        self.assertFalse(r.set('a', '1', xx=True))
        self.assertTrue(r.get('a') is None)
        r['a'] = 'bar'
        self.assertTrue(r.set('a', '2', xx=True))
        self.assertEqual(r.get('a'), '2')

    def test_set_px(self):
        self.assertTrue(r.set('a', '1', px=10000))
        self.assertEqual(r['a'], '1')
        self.assertTrue(0 < r.pttl('a') <= 10000)
        self.assertTrue(0 < r.ttl('a') <= 10)

    def test_set_px_timedelta(self):
        expire_at = datetime.timedelta(milliseconds=1000)
        self.assertTrue(r.set('a', '1', px=expire_at))
        self.assertTrue(0 < r.pttl('a') <= 1000)
        self.assertTrue(0 < r.ttl('a') <= 1)

    def test_set_ex(self):
        self.assertTrue(r.set('a', '1', ex=10))
        self.assertTrue(0 < r.ttl('a') <= 10)

    def test_set_ex_timedelta(self):
        expire_at = datetime.timedelta(seconds=60)
        self.assertTrue(r.set('a', '1', ex=expire_at))
        self.assertTrue(0 < r.ttl('a') <= 60)

    def test_set_multipleoptions(self):
        r['a'] = 'val'
        self.assertTrue(r.set('a', '1', xx=True, px=10000))
        self.assertTrue(0 < r.ttl('a') <= 10)

    def test_setex(self):
        self.assertTrue(r.setex('a', 60, '1'))
        self.assertEqual(r['a'], '1')
        self.assertTrue(0 < r.ttl('a') <= 60)

    def test_setnx(self):
        self.assertTrue(r.setnx('a', '1'))
        self.assertEqual(r['a'], '1')
        self.assertFalse(r.setnx('a', '2'))
        self.assertEqual(r['a'], '1')

    def test_setrange(self):
        self.assertEqual(r.setrange('a', 5, b'foo'), 8)
        self.assertEqual(r['a'], b'\0\0\0\0\0foo')
        r['a'] = b'abcdefghijh'
        self.assertEqual(r.setrange('a', 6, b'12345'), 11)
        self.assertEqual(r['a'], b'abcdef12345')

    def test_strlen(self):
        r['a'] = b'foo'
        self.assertEqual(r.strlen('a'), 3)

    def test_substr(self):
        r['a'] = b'0123456789'
        self.assertEqual(r.substr('a', 0), b'0123456789')
        self.assertEqual(r.substr('a', 2), b'23456789')
        self.assertEqual(r.substr('a', 3, 5), b'345')
        self.assertEqual(r.substr('a', 3, -2), b'345678')

    def test_type(self):
        self.assertEqual(r.type('a'), 'none')
        r['a'] = '1'
        self.assertEqual(r.type('a'), 'string')
        del r['a']
        r.lpush('a', '1')
        self.assertEqual(r.type('a'), 'list')
        del r['a']
        r.sadd('a', '1')
        self.assertEqual(r.type('a'), 'set')
        del r['a']
        r.zadd('a', **{'1': 1})
        self.assertEqual(r.type('a'), 'zset')

    #### LIST COMMANDS ####
    def test_blpop(self):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        self.assertEqual(r.blpop(['b', 'a'], timeout=1), ('b', '3'))
        self.assertEqual(r.blpop(['b', 'a'], timeout=1), ('b', '4'))
        self.assertEqual(r.blpop(['b', 'a'], timeout=1), ('a', '1'))
        self.assertEqual(r.blpop(['b', 'a'], timeout=1), ('a', '2'))
        self.assertTrue(r.blpop(['b', 'a'], timeout=1) is None)
        r.rpush('c', '1')
        self.assertEqual(r.blpop('c', timeout=1), ('c', '1'))

    def test_brpop(self):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        self.assertEqual(r.brpop(['b', 'a'], timeout=1), ('b', '4'))
        self.assertEqual(r.brpop(['b', 'a'], timeout=1), ('b', '3'))
        self.assertEqual(r.brpop(['b', 'a'], timeout=1), ('a', '2'))
        self.assertEqual(r.brpop(['b', 'a'], timeout=1), ('a', '1'))
        self.assertTrue(r.brpop(['b', 'a'], timeout=1) is None)
        r.rpush('c', '1')
        self.assertEqual(r.brpop('c', timeout=1), ('c', '1'))

    def test_brpoplpush(self):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        self.assertEqual(r.brpoplpush('a', 'b'), '2')
        self.assertEqual(r.brpoplpush('a', 'b'), '1')
        self.assertTrue(r.brpoplpush('a', 'b', timeout=1) is None)
        self.assertEqual(r.lrange('a', 0, -1), [])
        self.assertEqual(r.lrange('b', 0, -1), ['1', '2', '3', '4'])

    def test_brpoplpush_empty_string(self):
        r.rpush('a', '')
        self.assertEqual(r.brpoplpush('a', 'b'), '')

    def test_lindex(self):
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.lindex('a', '0'), '1')
        self.assertEqual(r.lindex('a', '1'), '2')
        self.assertEqual(r.lindex('a', '2'), '3')

    def test_linsert(self):
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.linsert('a', 'after', '2', '2.5'), 4)
        self.assertEqual(r.lrange('a', 0, -1), ['1', '2', '2.5', '3'])
        self.assertEqual(r.linsert('a', 'before', '2', '1.5'), 5)
        self.assertEqual(r.lrange('a', 0, -1), 
                         ['1', '1.5', '2', '2.5', '3'])

    def test_llen(self):
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.llen('a'), 3)

    def test_lpop(self):
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.lpop('a'), '1')
        self.assertEqual(r.lpop('a'), '2')
        self.assertEqual(r.lpop('a'), '3')
        self.assertTrue(r.lpop('a') is None)

    def test_lpush(self):
        self.assertEqual(r.lpush('a', '1'), 1)
        self.assertEqual(r.lpush('a', '2'), 2)
        self.assertEqual(r.lpush('a', '3', '4'), 4)
        self.assertEqual(r.lrange('a', 0, -1), ['4', '3', '2', '1'])

    def test_lpushx(self):
        self.assertEqual(r.lpushx('a', '1'), 0)
        self.assertEqual(r.lrange('a', 0, -1), [])
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.lpushx('a', '4'), 4)
        self.assertEqual(r.lrange('a', 0, -1), ['4', '1', '2', '3'])

    def test_lrange(self):
        r.rpush('a', '1', '2', '3', '4', '5')
        self.assertEqual(r.lrange('a', 0, 2), ['1', '2', '3'])
        self.assertEqual(r.lrange('a', 2, 10), ['3', '4', '5'])
        self.assertEqual(r.lrange('a', 0, -1), ['1', '2', '3', '4', '5'])

    def test_lrem(self):
        r.rpush('a', '1', '1', '1', '1')
        self.assertEqual(r.lrem('a', 1, '1'), 1)
        self.assertEqual(r.lrange('a', 0, -1), ['1', '1', '1'])
        self.assertEqual(r.lrem('a', 0, '1'), 3)
        self.assertEqual(r.lrange('a', 0, -1), [])

    def test_lset(self):
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.lrange('a', 0, -1), ['1', '2', '3'])
        self.assertTrue(r.lset('a', 1, '4'))
        self.assertEqual(r.lrange('a', 0, 2), ['1', '4', '3'])

    def test_ltrim(self):
        r.rpush('a', '1', '2', '3')
        self.assertTrue(r.ltrim('a', 0, 1))
        self.assertEqual(r.lrange('a', 0, -1), ['1', '2'])

    def test_rpop(self):
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.rpop('a'), '3')
        self.assertEqual(r.rpop('a'), '2')
        self.assertEqual(r.rpop('a'), '1')
        self.assertTrue(r.rpop('a') is None)

    def test_rpoplpush(self):
        r.rpush('a', 'a1', 'a2', 'a3')
        r.rpush('b', 'b1', 'b2', 'b3')
        self.assertEqual(r.rpoplpush('a', 'b'), 'a3')
        self.assertEqual(r.lrange('a', 0, -1), ['a1', 'a2'])
        self.assertEqual(r.lrange('b', 0, -1), ['a3', 'b1', 'b2', 'b3'])

    def test_rpush(self):
        self.assertEqual(r.rpush('a', '1'), 1)
        self.assertEqual(r.rpush('a', '2'), 2)
        self.assertEqual(r.rpush('a', '3', '4'), 4)
        self.assertEqual(r.lrange('a', 0, -1), ['1', '2', '3', '4'])

    def test_rpushx(self):
        self.assertEqual(r.rpushx('a', 'b'), 0)
        self.assertEqual(r.lrange('a', 0, -1), [])
        r.rpush('a', '1', '2', '3')
        self.assertEqual(r.rpushx('a', '4'), 4)
        self.assertEqual(r.lrange('a', 0, -1), ['1', '2', '3', '4'])

    # SCAN COMMANDS
    @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    def test_scan(self):
        r.set('a', 1)
        r.set('b', 2)
        r.set('c', 3)
        cursor, keys = r.scan()
        self.assertEqual(cursor, 0)
        self.assertEqual(set(keys), {'a', 'b', 'c'})
        _, keys = r.scan(match='a')
        self.assertEqual(set(keys), {'a'})

    # @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    # def test_scan_iter(self):
    #     r.set('a', 1)
    #     r.set('b', 2)
    #     r.set('c', 3)
    #     keys = list(r.scan_iter())
    #     self.assertEqual(set(keys), {'a', 'b', 'c'})
    #     keys = list(r.scan_iter(match='a'))
    #     self.assertEqual(set(keys), {'a'})

    @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    def test_sscan(self):
        r.sadd('a', 1, 2, 3)
        cursor, members = r.sscan('a')
        self.assertEqual(cursor, 0)
        self.assertEqual(set(members), {'1', '2', '3'})
        _, members = r.sscan('a', match='1')
        self.assertEqual(set(members), {'1'})

    # @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    # def test_sscan_iter(self):
    #     r.sadd('a', 1, 2, 3)
    #     members = list(r.sscan_iter('a'))
    #     self.assertEqual(set(members), {'1', '2', '3'})
    #     members = list(r.sscan_iter('a', match='1'))
    #     self.assertEqual(set(members), {'1'})

    @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    def test_hscan(self):
        r.hmset('a', {'a': '1', 'b': 2, 'c': 3})
        cursor, dic = r.hscan('a')
        self.assertEqual(cursor, 0)
        self.assertEqual(dic, {'a': '1', 'b': 2, 'c': 3})
        _, dic = r.hscan('a', match='a')
        self.assertEqual(dic, {'a': '1'})

    # @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    # def test_hscan_iter(self):
    #     r.hmset('a', {'a': 1, 'b': 2, 'c': 3})
    #     dic = dict(r.hscan_iter('a'))
    #     self.assertEqual(dic, {'a': '1', 'b': '2', 'c': '3'})
    #     dic = dict(r.hscan_iter('a', match='a'))
    #     self.assertEqual(dic, {'a': '1'})

    @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    def test_zscan(self):
        r.zadd('a', a=1, b=2, c=3)
        cursor, pairs = r.zscan('a')
        self.assertEqual(cursor, 0)
        self.assertEqual(set(pairs), {('a', 1), ('b', 2), ('c', 3)})
        _, pairs = r.zscan('a', match='a')
        self.assertEqual(set(pairs), {('a', 1)})

    # @unittest.skipIf(version_lt('2.8.0'), 'redis version not satisfied')
    # def test_zscan_iter(self):
    #     r.zadd('a', 'a', 1, 'b', 2, 'c', 3)
    #     pairs = list(r.zscan_iter('a'))
    #     self.assertEqual(set(pairs), {('a', 1), ('b', 2), ('c', 3)})
    #     pairs = list(r.zscan_iter('a', match='a'))
    #     self.assertEqual(set(pairs), {('a', 1)})

    ### SET COMMANDS ###
    def test_sadd(self):
        members = {'1', '2', '3'}
        r.sadd('a', *members)
        self.assertEqual(r.smembers('a'), members)

    def test_scard(self):
        r.sadd('a', '1', '2', '3')
        self.assertEqual(r.scard('a'), 3)

    def test_sdiff(self):
        r.sadd('a', '1', '2', '3')
        self.assertEqual(r.sdiff('a', 'b'), {'1', '2', '3'})
        r.sadd('b', '2', '3')
        self.assertEqual(r.sdiff('a', 'b'), {'1'})

    def test_sdiffstore(self):
        r.sadd('a', '1', '2', '3')
        self.assertEqual(r.sdiffstore('c', 'a', 'b'), 3)
        self.assertEqual(r.smembers('c'), {'1', '2', '3'})
        r.sadd('b', '2', '3')
        self.assertEqual(r.sdiffstore('c', 'a', 'b'), 1)
        self.assertEqual(r.smembers('c'), {'1'})

    def test_sinter(self):
        r.sadd('a', '1', '2', '3')
        self.assertEqual(r.sinter('a', 'b'), set())
        r.sadd('b', '2', '3')
        self.assertEqual(r.sinter('a', 'b'), {'2', '3'})

    def test_sinterstore(self):
        r.sadd('a', '1', '2', '3')
        self.assertEqual(r.sinterstore('c', 'a', 'b'), 0)
        self.assertEqual(r.smembers('c'), set())
        r.sadd('b', '2', '3')
        self.assertEqual(r.sinterstore('c', 'a', 'b'), 2)
        self.assertEqual(r.smembers('c'), {'2', '3'})

    def test_sismember(self):
        r.sadd('a', '1', '2', '3')
        self.assertTrue(r.sismember('a', '1'))
        self.assertTrue(r.sismember('a', '2'))
        self.assertTrue(r.sismember('a', '3'))
        self.assertFalse(r.sismember('a', '4'))

    def test_smembers(self):
        r.sadd('a', '1', '2', '3')
        self.assertEqual(r.smembers('a'), {'1', '2', '3'})

    def test_smove(self):
        r.sadd('a', 'a1', 'a2')
        r.sadd('b', 'b1', 'b2')
        self.assertTrue(r.smove('a', 'b', 'a1'))
        self.assertEqual(r.smembers('a'), {'a2'})
        self.assertEqual(r.smembers('b'), {'b1', 'b2', 'a1'})

    def test_spop(self):
        s = ['1', '2', '3']
        r.sadd('a', *s)
        value = r.spop('a')
        self.assertTrue(value in s)
        self.assertEqual(r.smembers('a'), set(s) - {value})

    def test_srandmember(self):
        s = ['1', '2', '3']
        r.sadd('a', *s)
        self.assertTrue(r.srandmember('a') in s)

    def test_srandmember_multi_value(self):
        s = ['1', '2', '3']
        r.sadd('a', *s)
        randoms = r.srandmember('a', number=2)
        self.assertEqual(len(randoms), 2)
        self.assertEqual(set(randoms).intersection(s), set(randoms))

    def test_srem(self):
        r.sadd('a', '1', '2', '3', '4')
        self.assertEqual(r.srem('a', '5'), 0)
        self.assertEqual(r.srem('a', '2', '4'), 2)
        self.assertEqual(r.smembers('a'), {'1', '3'})

    def test_sunion(self):
        r.sadd('a', '1', '2')
        r.sadd('b', '2', '3')
        self.assertEqual(r.sunion('a', 'b'), {'1', '2', '3'})

    def test_sunionstore(self):
        r.sadd('a', '1', '2')
        r.sadd('b', '2', '3')
        self.assertEqual(r.sunionstore('c', 'a', 'b'), 3)
        self.assertEqual(r.smembers('c'), {'1', '2', '3'})

    ### SORTED SET COMMANDS ###
    def test_zadd(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zrange('a', 0, -1), ['a1', 'a2', 'a3'])

    def test_zcard(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zcard('a'), 3)

    def test_zcount(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zcount('a', '-inf', '+inf'), 3)
        self.assertEqual(r.zcount('a', 1, 2), 2)
        self.assertEqual(r.zcount('a', 10, 20), 0)

    def test_zincrby(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zincrby('a', 'a2'), 3.0)
        self.assertEqual(r.zincrby('a', 'a3', amount=5), 8.0)
        self.assertEqual(r.zscore('a', 'a2'), 3.0)
        self.assertEqual(r.zscore('a', 'a3'), 8.0)

    @unittest.skipIf(version_lt('2.8.9'), 'redis version not satisfied')
    def test_zlexcount(self):
        r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        self.assertEqual(r.zlexcount('a', '-', '+'), 7)
        self.assertEqual(r.zlexcount('a', '[b', '[f'), 5)

    def test_zinterstore_sum(self):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zinterstore('d', ['a', 'b', 'c']), 2)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a3', 8), ('a1', 9)])

    def test_zinterstore_max(self):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zinterstore('d', ['a', 'b', 'c'], aggregate='MAX'), 2)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a3', 5), ('a1', 6)])

    def test_zinterstore_min(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        r.zadd('b', a1=2, a2=3, a3=5)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zinterstore('d', ['a', 'b', 'c'], aggregate='MIN'), 2)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a1', 1), ('a3', 3)])

    def test_zinterstore_with_weight(self):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zinterstore('d', {'a': 1, 'b': 2, 'c': 3}), 2)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a3', 20), ('a1', 23)])

    def test_zrange(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zrange('a', 0, 1), ['a1', 'a2'])
        self.assertEqual(r.zrange('a', 1, 2), ['a2', 'a3'])

        # withscores
        self.assertEqual(r.zrange('a', 0, 1, withscores=True), 
                         [('a1', 1.0), ('a2', 2.0)])
        self.assertEqual(r.zrange('a', 1, 2, withscores=True), 
                         [('a2', 2.0), ('a3', 3.0)])

        # custom score function
        self.assertEqual(r.zrange('a', 0, 1, withscores=True, score_cast_func=int), 
                         [('a1', 1), ('a2', 2)])

    @unittest.skipIf(version_lt('2.8.9'), 'redis version not satisfied')
    def test_zrangebylex(self):
        r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        self.assertEqual(r.zrangebylex('a', '-', '[c'), ['a', 'b', 'c'])
        self.assertEqual(r.zrangebylex('a', '-', '(c'), ['a', 'b'])
        self.assertEqual(r.zrangebylex('a', '[aaa', '(g'),
                         ['b', 'c', 'd', 'e', 'f'])
        self.assertEqual(r.zrangebylex('a', '[f', '+'), ['f', 'g'])
        self.assertEqual(r.zrangebylex('a', '-', '+', start=3, num=2), ['d', 'e'])

    def test_zrangebyscore(self):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        self.assertEqual(r.zrangebyscore('a', 2, 4), ['a2', 'a3', 'a4'])

        # slicing with start/num
        self.assertEqual(r.zrangebyscore('a', 2, 4, start=1, num=2), 
                         ['a3', 'a4'])

        # withscores
        self.assertEqual(r.zrangebyscore('a', 2, 4, withscores=True), 
                         [('a2', 2.0), ('a3', 3.0), ('a4', 4.0)])

        # custom score function
        self.assertEqual(r.zrangebyscore('a', 2, 4, withscores=True, score_cast_func=int),
                         [('a2', 2), ('a3', 3), ('a4', 4)])

    def test_zrank(self):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        self.assertEqual(r.zrank('a', 'a1'), 0)
        self.assertEqual(r.zrank('a', 'a2'), 1)
        self.assertTrue(r.zrank('a', 'a6') is None)

    def test_zrem(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zrem('a', 'a2'), 1)
        self.assertEqual(r.zrange('a', 0, -1), ['a1', 'a3'])
        self.assertEqual(r.zrem('a', 'b'), 0)
        self.assertEqual(r.zrange('a', 0, -1), ['a1', 'a3'])

    def test_zrem_multiple_keys(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zrem('a', 'a1', 'a2'), 2)
        self.assertEqual(r.zrange('a', 0, 5), ['a3'])

    @unittest.skipIf(version_lt('2.8.9'), 'redis version not satisfied')
    def test_zremrangebylex(self):
        r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        self.assertEqual(r.zremrangebylex('a', '-', '[c'), 3)
        self.assertEqual(r.zrange('a', 0, -1), ['d', 'e', 'f', 'g'])
        self.assertEqual(r.zremrangebylex('a', '[f', '+'), 2)
        self.assertEqual(r.zrange('a', 0, -1), ['d', 'e'])
        self.assertEqual(r.zremrangebylex('a', '[h', '+'), 0)
        self.assertEqual(r.zrange('a', 0, -1), ['d', 'e'])

    def test_zremrangebyrank(self):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        self.assertEqual(r.zremrangebyrank('a', 1, 3), 3)
        self.assertEqual(r.zrange('a', 0, 5), ['a1', 'a5'])

    def test_zremrangebyscore(self):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        self.assertEqual(r.zremrangebyscore('a', 2, 4), 3)
        self.assertEqual(r.zrange('a', 0, -1), ['a1', 'a5'])
        self.assertEqual(r.zremrangebyscore('a', 2, 4), 0)
        self.assertEqual(r.zrange('a', 0, -1), ['a1', 'a5'])

    def test_zrevrange(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zrevrange('a', 0, 1), ['a3', 'a2'])
        self.assertEqual(r.zrevrange('a', 1, 2), ['a2', 'a1'])

        # withscores
        self.assertEqual(r.zrevrange('a', 0, 1, withscores=True), 
                         [('a3', 3.0), ('a2', 2.0)])
        self.assertEqual(r.zrevrange('a', 1, 2, withscores=True), 
                         [('a2', 2.0), ('a1', 1.0)])

        # custom score function
        self.assertEqual(r.zrevrange('a', 0, 1, withscores=True, score_cast_func=int),
                         [('a3', 3.0), ('a2', 2.0)])

    def test_zrevrangebyscore(self):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        self.assertEqual(r.zrevrangebyscore('a', 4, 2), ['a4', 'a3', 'a2'])

        # slicing with start/num
        self.assertEqual(r.zrevrangebyscore('a', 4, 2, start=1, num=2), 
                         ['a3', 'a2'])

        # withscores
        self.assertEqual(r.zrevrangebyscore('a', 4, 2, withscores=True), 
                         [('a4', 4.0), ('a3', 3.0), ('a2', 2.0)])

        # custom score function
        self.assertEqual(r.zrevrangebyscore('a', 4, 2, withscores=True, score_cast_func=int),
                         [('a4', 4), ('a3', 3), ('a2', 2)])

    def test_zrevrank(self):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        self.assertEqual(r.zrevrank('a', 'a1'), 4)
        self.assertEqual(r.zrevrank('a', 'a2'), 3)
        self.assertTrue(r.zrevrank('a', 'a6') is None)

    def test_zscore(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        self.assertEqual(r.zscore('a', 'a1'), 1.0)
        self.assertEqual(r.zscore('a', 'a2'), 2.0)
        self.assertTrue(r.zscore('a', 'a4') is None)

    def test_zunionstore_sum(self):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zunionstore('d', ['a', 'b', 'c']), 4)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a2', 3), ('a4', 4), ('a3', 8), ('a1', 9)])

    def test_zunionstore_max(self):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zunionstore('d', ['a', 'b', 'c'], aggregate='MAX'), 4)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a2', 2), ('a4', 4), ('a3', 5), ('a1', 6)])

    def test_zunionstore_min(self):
        r.zadd('a', a1=1, a2=2, a3=3)
        r.zadd('b', a1=2, a2=2, a3=4)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zunionstore('d', ['a', 'b', 'c'], aggregate='MIN'), 4)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a1', 1), ('a2', 2), ('a3', 3), ('a4', 4)])

    def test_zunionstore_with_weight(self):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        self.assertEqual(r.zunionstore('d', {'a': 1, 'b': 2, 'c': 3}), 4)
        self.assertEqual(r.zrange('d', 0, -1, withscores=True), 
                         [('a2', 5), ('a4', 12), ('a3', 20), ('a1', 23)])

    # HYPERLOGLOG TESTS
    @unittest.skipIf(version_lt('2.8.9'), 'redis version not satisfied')
    def test_pfadd(self):
        members = {'1', '2', '3'}
        self.assertEqual(r.pfadd('a', *members), 1)
        self.assertEqual(r.pfadd('a', *members), 0)
        self.assertEqual(r.pfcount('a'), len(members))

    @unittest.skipIf(version_lt('2.8.9'), 'redis version not satisfied')
    def test_pfcount(self):
        members = {'1', '2', '3'}
        r.pfadd('a', *members)
        self.assertEqual(r.pfcount('a'), len(members))

    @unittest.skipIf(version_lt('2.8.9'), 'redis version not satisfied')
    def test_pfmerge(self,):
        mema = {'1', '2', '3'}
        memb = {'2', '3', '4'}
        memc = {'5', '6', '7'}
        r.pfadd('a', *mema)
        r.pfadd('b', *memb)
        r.pfadd('c', *memc)
        r.pfmerge('d', 'c', 'a')
        self.assertEqual(r.pfcount('d'), 6)
        r.pfmerge('d', 'b')
        self.assertEqual(r.pfcount('d'), 7)
        
    ### HASH COMMANDS ###
    def test_hget_and_hset(self):
        r.hmset('a', {'1': 1, '2': 2, '3': '3v'})
        self.assertEqual(r.hget('a', '1'), 1)
        self.assertEqual(r.hget('a', '2'), 2)
        self.assertEqual(r.hget('a', '3'), '3v')

        # field was updated, redis returns 0
        self.assertEqual(r.hset('a', '2', 5), 0)
        self.assertEqual(r.hget('a', '2'), 5)

        # field is new, redis returns 1
        self.assertEqual(r.hset('a', '4', 4), 1)
        self.assertEqual(r.hget('a', '4'), 4)

        # key inside of hash that doesn't exist returns null value
        self.assertTrue(r.hget('a', 'b') is None)

    def test_hdel(self):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        self.assertEqual(r.hdel('a', '2'), 1)
        self.assertTrue(r.hget('a', '2') is None)
        self.assertEqual(r.hdel('a', '1', '3'), 2)
        self.assertEqual(r.hlen('a'), 0)

    def test_hexists(self):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        self.assertTrue(r.hexists('a', '1'))
        self.assertFalse(r.hexists('a', '4'))

    def test_hgetall(self):
        h = {'a1': '1', 'a2': '2', 'a3': '3'}
        r.hmset('a', h)
        self.assertEqual(r.hgetall('a'), h)

    def test_hincrby(self):
        self.assertEqual(r.hincrby('a', '1'), 1)
        self.assertEqual(r.hincrby('a', '1', amount=2), 3)
        self.assertEqual(r.hincrby('a', '1', amount=-2), 1)

    def test_hincrbyfloat(self):
        self.assertEqual(r.hincrbyfloat('a', '1'), 1.0)
        self.assertEqual(r.hincrbyfloat('a', '1'), 2.0)
        self.assertEqual(r.hincrbyfloat('a', '1', 1.2), 3.2)

    def test_hkeys(self):
        h = {'a1': '1', 'a2': '2', 'a3': '3'}
        r.hmset('a', h)
        local_keys = list(iterkeys(h))
        remote_keys = r.hkeys('a')
        self.assertEqual(sorted(local_keys), sorted(remote_keys))

    def test_hlen(self):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        self.assertEqual(r.hlen('a'), 3)

    def test_hmget(self):
        self.assertTrue(r.hmset('a', {'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual(r.hmget('a', 'a', 'b', 'c'), [1, 2, 3])

    def test_hmset(self):
        h = {'a': '1', 'b': '2', 'c': '3'}
        self.assertTrue(r.hmset('a', h))
        self.assertEqual(r.hgetall('a'), h)

    def test_hsetnx(self):
        # Initially set the hash field
        self.assertTrue(r.hsetnx('a', '1', 1))
        self.assertEqual(r.hget('a', '1'), 1)
        self.assertFalse(r.hsetnx('a', '1', 2))
        self.assertEqual(r.hget('a', '1'), 1)

    def test_hvals(self):
        h = {'a1': '1', 'a2': '2', 'a3': '3'}
        r.hmset('a', h)
        local_vals = list(itervalues(h))
        remote_vals = r.hvals('a')
        self.assertEqual(sorted(local_vals), sorted(remote_vals))

    ### SORT ###
    def test_sort_basic(self):
        r.rpush('a', 3, 2, 1, 4)
        self.assertEqual(r.sort('a'), [1, 2, 3, 4])

    def test_sort_limited(self):
        r.rpush('a', 3, 2, 1, 4)
        self.assertEqual(r.sort('a', start=1, num=2), [2, 3])

    def test_sort_by(self):
        r['score:1'] = 8
        r['score:2'] = 3
        r['score:3'] = 5
        r.rpush('a', 3, 2, 1)
        self.assertEqual(r.sort('a', by='score:*'), [2, 3, 1])

    def test_sort_get(self):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', 2, 3, 1)
        self.assertEqual(r.sort('a', get='user:*'), ['u1', 'u2', 'u3'])

    def test_sort_get_multi(self):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', 2, 3, 1)
        self.assertEqual(r.sort('a', get=('user:*', '#')), 
                         ['u1', 1, 'u2', 2, 'u3', 3])

    def test_sort_get_groups_two(self):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', 2, 3, 1)
        self.assertEqual(r.sort('a', get=('user:*', '#'), groups=True), 
                         [('u1', 1), ('u2', 2), ('u3', 3)])

    def test_sort_groups_string_get(self):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with self.assertRaises(redis.DataError):
            r.sort('a', get='user:*', groups=True)

    def test_sort_groups_just_one_get(self):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with self.assertRaises(redis.DataError):
            r.sort('a', get=['user:*'], groups=True)

    def test_sort_groups_no_get(self):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with self.assertRaises(redis.DataError):
            r.sort('a', groups=True)

    def test_sort_groups_three_gets(self):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r['door:1'] = 'd1'
        r['door:2'] = 'd2'
        r['door:3'] = 'd3'
        r.rpush('a', 2, 3, 1)
        self.assertEqual(r.sort('a', get=('user:*', 'door:*', '#'), groups=True),
                         [
                             ('u1', 'd1', 1),
                             ('u2', 'd2', 2),
                             ('u3', 'd3', 3)
                         ])

    def test_sort_desc(self):
        r.rpush('a', 2, 3, 1)
        self.assertEqual(r.sort('a', desc=True), [3, 2, 1])

    def test_sort_alpha(self):
        r.rpush('a', 'e', 'c', 'b', 'd', 'a')
        self.assertEqual(r.sort('a', alpha=True), 
                         ['a', 'b', 'c', 'd', 'e'])

    def test_sort_store(self):
        r.rpush('a', 2, 3, 1)
        self.assertEqual(r.sort('a', store='sorted_values'), 3)
        self.assertEqual(r.lrange('sorted_values', 0, -1), [1, 2, 3])

    def test_sort_all_options(self):
        r['user:1:username'] = 'zeus'
        r['user:2:username'] = 'titan'
        r['user:3:username'] = 'hermes'
        r['user:4:username'] = 'hercules'
        r['user:5:username'] = 'apollo'
        r['user:6:username'] = 'athena'
        r['user:7:username'] = 'hades'
        r['user:8:username'] = 'dionysus'

        r['user:1:favorite_drink'] = 'yuengling'
        r['user:2:favorite_drink'] = 'rum'
        r['user:3:favorite_drink'] = 'vodka'
        r['user:4:favorite_drink'] = 'milk'
        r['user:5:favorite_drink'] = 'pinot noir'
        r['user:6:favorite_drink'] = 'water'
        r['user:7:favorite_drink'] = 'gin'
        r['user:8:favorite_drink'] = 'apple juice'

        r.rpush('gods', 5, 8, 3, 1, 2, 7, 6, 4)
        num = r.sort('gods', start=2, num=4, by='user:*:username',
                     get='user:*:favorite_drink', desc=True, alpha=True,
                     store='sorted')
        self.assertEqual(num, 4)
        self.assertEqual(r.lrange('sorted', 0, 10), 
                         ['vodka', 'milk', 'gin', 'apple juice'])

    ## extra tests
    def test_large_responses(self):
        """The PythonParser has some special cases for return values > 1MB"""
        # load up 5MB of data into a key
        data = ''.join([ascii_letters] * (5000000 // len(ascii_letters)))
        r['a'] = data
        self.assertEqual(r['a'], data)

    def test_floating_point_encoding(self):
        """
        High precision floating point values sent to the server should keep
        precision.
        """
        timestamp = 1349673917.939762
        r.zadd('a', timestamp, 'a1')
        self.assertEqual(r.zscore('a', 'a1'), timestamp)

    def test_key_error(self):
        with self.assertRaises(KeyError):
            check = r['a']
        r['a'] = None
        check = r['a']

    ## pipeline
    def test_msetex(self):
        obj = {1: 2}
        items = {'a': 1, 'b': 'str', 'c': True, 'd': obj}
        r.msetex(items, 10)
        self.assertEqual(sort(r.mget('a', 'b', 'c', 'd')), sort(items.values()))
        for key in items:
            self.assertTrue(0 < r.ttl(key) <= 10)

    def test_pipelineget(self):
        obj = {1: 2}
        items = {'a': 1, 'b': 'str', 'c': True, 'd': obj}
        r.msetex(items, 20)

        with r.pipeline() as pipe:
            pipe.get('a')
            pipe.get('b')
            pipe.get('c')
            pipe.get('d')
            self.assertEqual(sort(pipe.execute()), sort(items.values()))

        items['e'] = 'asdf'
        self.assertEqual(sort(r.maddex(items, 10)), sort([False, False, False, False, True]))
        self.assertTrue(0 < r.ttl('e') <= 10)
        self.assertTrue(10 < r.ttl('a') <= 20)

    def test_pipelinehashes(self):
        h1 = {'a1': '1', 'a2': '2', 'a3': '3'}
        h2 = {'a1': '1', 'a2': '3', 'a4': '4'}
        r.hmset('a', h1)
        r.hmset('b', h2)
        self.assertEqual(r.hgetall('a'), h1)
        self.assertEqual(r.hgetall('b'), h2)

        with r.pipeline() as pipe:
            pipe.hgetall('a')
            pipe.hgetall('b')
            self.assertEqual(sort(pipe.execute()), sort([h1, h2]))

    def test_mincr(self):
        items = {'a': 0, 'b': 1, 'c': -10, 'd': 9999999999}
        r.msetnx(items)

        keys = list(items.keys())
        keys.extend(['e', 'f'])
        result = r.mincr(keys)
        result_remote = r.mget(keys)
        self.assertEqual(result, result_remote)
        for i, key in enumerate(keys):
            if key not in items:
                self.assertEqual(result[i], 1)
            else:
                self.assertEqual(items[key] + 1, result[i])