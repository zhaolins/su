from su.tests import test_env
import time
import unittest
from su.g import redis_cache, redis_db, cache, permacache


def reset():
    redis_db.client.flushdb()
    redis_cache.flush()


def without_local(func, c, *args, **kwargs):
    localcache = c.caches[0]
    c.caches = c.caches[1:]
    func(c, *args, **kwargs)
    c.caches = (localcache.__class__(),) + c.caches


class CacheChainTests(unittest.TestCase):
    def setUp(self):
        reset()

    def tearDown(self):
        #reset()
        pass

    def test_redis(self):
        self._test_general(permacache, 'test1')
        without_local(self._test_general, permacache, 'test2')
        without_local(self._test_expire, permacache, 'test3')

    def test_memcache(self):
        self._test_general(cache, 'test1')
        without_local(self._test_general, cache, 'test2')
        without_local(self._test_expire, cache, 'test3')

    def _test_general(self, c, prefix):
        self.assertEqual(c.get('%s0' % prefix), None)
        c.add_multi({'0': False}, prefix=prefix)
        self.assertEqual(c.get('%s0' % prefix), False)
        c.set('%s0' % prefix, 'False')
        self.assertEqual(c.get('%s0' % prefix), 'False')

        c.set('%sobj' % prefix, reset)
        self.assertEqual(c.get('%sobj' % prefix), reset)

        c.set('%s1' % prefix, 1)
        self.assertEqual(c.get('%s1' % prefix), 1)
        self.assertFalse(c.add('%s1' % prefix, 2))
        self.assertEqual(c.get('%s1' % prefix), 1)

        #python data
        c.set('%s2' % prefix, [1, 2, 3])
        self.assertEqual(c.get('%s2' % prefix), [1, 2, 3])
        self.assertEqual(c.add_multi({'1': 2, '2': 3, '3': '3'}, prefix=prefix),
                         {'1': False,
                          '2': False,
                          '3': True})
        self.assertEqual(c.get_multi(('%s1' % prefix, '%s2' % prefix, '%s3' % prefix)),
                         {'%s1' % prefix: 1,
                          '%s2' % prefix: [1, 2, 3],
                          '%s3' % prefix: '3'})
        self.assertEqual(c.get_multi(('1', '2', '3'), prefix=prefix), {'1': 1,
                                                                       '2': [1, 2, 3],
                                                                       '3': '3'})

        #set multi, no prefix
        c.set_multi({'%s3' % prefix: 3, '%s4' % prefix: 4})
        self.assertEqual(c.get_multi(('%s3' % prefix, '%s4' % prefix)), {'%s3' % prefix: 3,
                                                                         '%s4' % prefix: 4})

        #set multi, prefix
        c.set_multi({'3': 3, '4': 4}, prefix='%sp_' % prefix)
        self.assertEqual(c.get_multi((1, 2, '3', 4), prefix='%sp_' % prefix), {'3': 3, 4: 4})
        self.assertEqual(c.get_multi(('%sp_3' % prefix, '%sp_4' % prefix)), {'%sp_3' % prefix: 3, '%sp_4' % prefix: 4})

        # delete
        c.set('%s1' % prefix, 1)
        self.assertEqual(c.get('%s1' % prefix), 1)
        c.delete('%s1' % prefix)
        self.assertTrue(c.get('%s1' % prefix) is None)

        c.set('%s1' % prefix, 1)
        c.set('%s2' % prefix, 2)
        c.set('%s3' % prefix, 3)
        self.assertEqual(c.get('%s1' % prefix), 1)
        self.assertEqual(c.get('%s2' % prefix), 2)
        c.delete_multi(['%s1' % prefix, '%s2' % prefix])
        self.assertTrue((c.get('%s1' % prefix) is None and
                         c.get('%s2' % prefix) is None and
                         c.get('%s3' % prefix) == 3))

        c.set_multi({'%s1' % prefix: 1, '%s2' % prefix: 2, '%s3' % prefix: 3})
        self.assertEqual(c.get_multi((1, '2', 3), prefix=prefix), {1: 1, '2': 2, 3: 3})
        c.delete_multi(['1', '2'], prefix=prefix)
        self.assertTrue((c.get('%s1' % prefix) is None and
                         c.get('%s2' % prefix) is None and
                         c.get('%s3' % prefix) == 3))

        #incr
        c.set('%s5' % prefix, 1)
        c.set('%s6' % prefix, 1)
        c.incr('%s5' % prefix)
        self.assertEqual(c.get('%s5' % prefix), 2)
        c.incr('%s5' % prefix, 2)
        self.assertEqual(c.get('%s5' % prefix), 4)

        c.incr_multi(('%s5' % prefix, '%s6' % prefix), 1)
        self.assertEqual(c.get('%s5' % prefix), 5)
        self.assertEqual(c.get('%s6' % prefix), 2)

        c.incr_multi(('5', '6'), 1, prefix=prefix)
        self.assertEqual(c.get('%s5' % prefix), 6)
        self.assertEqual(c.get('%s6' % prefix), 3)

    def _test_expire(self, c, prefix):
        c.set_multi({'10': 11, 12: '13'}, prefix='%sp_' % prefix, time=1)
        self.assertEqual(c.get_multi((1, 2, 10, '12'), prefix='%sp_' % prefix), {10: 11, '12': '13'})
        time.sleep(1.1)
        self.assertEqual(c.get_multi((1, 2, 10, '12'), prefix='%sp_' % prefix), {})

        c.add_multi({'10': 11, 12: '13'}, prefix='%sp_' % prefix, time=1)
        self.assertEqual(c.get_multi((1, 2, 10, '12'), prefix='%sp_' % prefix), {10: 11, '12': '13'})
        time.sleep(1.1)
        self.assertEqual(c.get_multi((1, 2, 10, '12'), prefix='%sp_' % prefix), {})

        c.set('expire', 2, 1)
        c.set('persistence', 100)
        self.assertEqual(c.get('expire'), 2)
        c.incr('expire')
        self.assertEqual(c.get('expire'), 3)
        time.sleep(1)
        c.incr_multi(('expire', 'persistence'), 1)
        # increase keys already exists
        self.assertEqual(c.get_multi(('expire', 'persistence')), {'persistence': 101})
