
from su.tests import test_env
from su.db.operators import desc, asc
from su.tests.test_models import User, Post, Comment, Friendship
from su.db.cached_object import CachedList, CachedAttr, filter_entity2
from su.g import flush_cache, flush_permacache
import unittest


def test_query():
    return User._query(User.c._id > 5, sort=(asc('test_order1'), asc('test_order2')))


class CachedObjectTests(unittest.TestCase):
    def setUp(self):
        flush_cache()
        flush_permacache()

        for i in range(1, 11):
            user = User(role=0)
            user._id = i
            user._cache_self()

    def tearDown(self):
        pass

    def test_FilteredCachedList(self):
        users = []
        for i in range(1, 11):
            users.append(User._by_id(i))

        fs = []
        for i in range(1, 5):
            new_fs = Friendship(users[1], users[i], 'friend')
            fs.append(new_fs)

        fs[0].test_order1 = 1  # entity2=2
        fs[1].test_order1 = 1  # entity2=3
        fs[3].test_order1 = 2  # entity2=5

        fs[0].test_order2 = 3
        fs[1].test_order2 = 2
        fs[3].test_order2 = 1

        # basic insert & sort test
        cached_list = CachedList('fs', test_query()._sort, filter_fn=filter_entity2)
        cached_list.set([fs[0], fs[1], fs[3], fs[0]])
        """                                            fs[1],fs[0],fs[3]"""
        self.assertEqual(list(cached_list.data.keys()), ['3', '2', '5'])
        cached_list.fetch()
        self.assertEqual(list(cached_list.data.keys()), ['3', '2', '5'])
        self.assertTrue(cached_list._hit)

        cached_list.delete(fs[0])
        self.assertEqual(list(cached_list.data.keys()), ['3', '5'])
        cached_list.fetch(update=True)
        self.assertEqual(list(cached_list.data.keys()), ['3', '5'])
        cached_list.set(fs[0])
        self.assertEqual(list(cached_list.data.keys()), ['3', '2', '5'])
        cached_list.fetch(update=True)
        self.assertEqual(list(cached_list.data.keys()), ['3', '2', '5'])

        cached_list.reset_anchor(after=fs[0])
        self.assertEqual(list(cached_list.data.keys()), ['5'])
        cached_list.reset_anchor(before=fs[0])
        self.assertEqual(list(cached_list.data.keys()), ['3'])
        cached_list.reset_anchor(after=fs[1], before=fs[3])
        self.assertEqual(list(cached_list.data.keys()), ['2'])
        cached_list.reset_anchor(before=fs[1], after=fs[3])
        self.assertEqual(list(cached_list.data.keys()), [])

        reversed_list = CachedList('fs', (desc('test_order1'), desc('test_order2')), filter_fn=filter_entity2)
        reversed_list.fetch()
        """                                              fs[3],fs[0],fs[1]"""
        self.assertEqual(list(reversed_list.data.keys()), ['5', '2', '3'])
        reversed_list.reset_anchor(after=fs[0])
        self.assertEqual(list(reversed_list.data.keys()), ['3'])
        reversed_list.reset_anchor(before=fs[0])
        self.assertEqual(list(reversed_list.data.keys()), ['5'])
        reversed_list.reset_anchor(after=fs[3], before=fs[1])
        self.assertEqual(list(reversed_list.data.keys()), ['2'])
        reversed_list.reset_anchor(before=fs[3], after=fs[1])
        self.assertEqual(list(reversed_list.data.keys()), [])

    def test_CachedList(self):
        users = []
        for i in range(1, 11):
            users.append(User._by_id(i))

        users[0].test_order1 = 1
        users[1].test_order1 = 1
        users[7].test_order1 = 2

        users[0].test_order2 = 3
        users[1].test_order2 = 2
        users[7].test_order2 = 1

        # basic insert & sort test
        cached_list = CachedList('test_query', test_query()._sort)
        cached_list.set([users[0], users[1], users[7], users[0]])
        self.assertEqual(list(cached_list.data.keys()), ['2', '1', '8'])
        cached_list.fetch()
        self.assertEqual(list(cached_list.data.keys()), ['2', '1', '8'])
        self.assertTrue(cached_list._hit)

        # remote test
        tmp = CachedList('test_query', test_query()._sort)
        tmp.fetch()
        self.assertEqual(list(tmp.data.keys()), ['2', '1', '8'])
        self.assertTrue(tmp._hit)

        # update test
        users[1].test_order1 = 3
        cached_list.set([users[1]])
        self.assertEqual(list(cached_list.data.keys()), ['1', '8', '2'])
        cached_list.fetch(True)
        self.assertEqual(list(cached_list.data.keys()), ['1', '8', '2'])
        self.assertTrue(cached_list._hit)

        # different key test
        cached_list2 = CachedList('test_query2', test_query()._sort)
        cached_list2.set([users[7], users[1]])
        self.assertEqual(list(cached_list2.data.keys()), ['8', '2'])
        cached_list2.fetch()
        self.assertEqual(list(cached_list2.data.keys()), ['8', '2'])
        self.assertTrue(cached_list2._hit)

        # reset test
        cached_list.reset([users[7], users[1]])
        self.assertEqual(list(cached_list.data.keys()), ['8', '2'])
        cached_list.fetch(True)
        self.assertEqual(list(cached_list.data.keys()), ['8', '2'])
        self.assertTrue(cached_list._hit)

        # reset an empty list
        cached_list_new = CachedList('test_query_new', test_query()._sort)
        cached_list_new.reset([users[7], users[1]])
        self.assertEqual(list(cached_list_new.data.keys()), ['8', '2'])
        self.assertTrue(cached_list_new._hit)

        # set test
        cached_list.set([users[0]])
        self.assertEqual(list(cached_list.data.keys()), ['1', '8', '2'])
        self.assertTrue(cached_list.timestamps['1'] > cached_list.timestamps['2'])
        cached_list.fetch(True)
        self.assertEqual(list(cached_list.data.keys()), ['1', '8', '2'])
        # timestamp test
        self.assertTrue(cached_list.timestamps['1'] > cached_list.timestamps['2'])
        self.assertTrue(cached_list._hit)

        # delete test
        cached_list.delete([users[7], users[0]])
        self.assertEqual(list(cached_list.data.keys()), list(cached_list.timestamps.keys()))
        self.assertEqual(list(cached_list.data.keys()), ['2'])
        cached_list.fetch(True)
        self.assertEqual(list(cached_list.data.keys()), list(cached_list.timestamps.keys()))
        self.assertEqual(list(cached_list.data.keys()), ['2'])
        self.assertTrue(cached_list._hit)

        # empty list test
        cached_list3 = CachedList('test_query3', test_query()._sort)
        cached_list4 = CachedList('test_query4', test_query()._sort)
        cached_list3.set(None)
        cached_list3.fetch()
        cached_list4.fetch()
        self.assertTrue(cached_list3._hit)
        self.assertEqual(cached_list4._hit, False)

        # abolish test
        cached_list3.abolish()
        cached_list3.fetch(True)
        self.assertEqual(cached_list3._hit, False)

    def test_CachedItem(self):
        key_space = 'user:1'
        ca1 = CachedAttr(key_space)
        # basic set&fetch
        ca1.set({'father': 1, 'mother': '2'})
        self.assertEqual(ca1.data, {'father': 1, 'mother': '2'})
        self.assertTrue(ca1._hit)
        ca1.fetch()
        self.assertEqual(ca1.data, {'father': 1, 'mother': '2'})
        self.assertTrue(ca1._hit)

        # remote
        ca1_remote = CachedAttr(key_space)
        ca1_remote.fetch()
        self.assertEqual(ca1_remote.data, ca1.data)
        self.assertTrue(ca1_remote._hit)

        # partial fetch
        ca1_partial = CachedAttr(key_space)
        self.assertEqual(ca1_partial.data, {})
        ca1_partial.fetch(fields='father')
        self.assertEqual(ca1_partial.data, {'father': 1})
        self.assertTrue(ca1_partial._hit)

        # NoneHolder
        ca1.set({'child': None})
        self.assertEqual(ca1.data, {'father': 1, 'mother': '2', 'child': None})
        ca1.fetch(update=True)
        self.assertEqual(ca1.data, {'father': 1, 'mother': '2', 'child': None})

        # remote confirm
        ca1_partial = CachedAttr(key_space)
        self.assertEqual(ca1_partial.data, {})
        ca1_partial.fetch(fields='child')
        self.assertEqual(ca1_partial.data, {'child': None})
        self.assertTrue(ca1_partial._hit)
        ca1_partial.fetch(fields='nonexists', update=True)
        self.assertEqual(ca1_partial.data, {})
        self.assertEqual(ca1_partial._hit, False)

        # reset
        ca1.reset({'father2': 1, 'mother': '3'})
        self.assertEqual(ca1.data, {'father2': 1, 'mother': '3'})
        ca1.fetch(update=True)
        self.assertEqual(ca1.data, {'father2': 1, 'mother': '3'})

        # delete
        ca1.delete(['father2'])
        self.assertEqual(ca1.data, {'mother': '3'})
        ca1.fetch(update=True)
        self.assertEqual(ca1.data, {'mother': '3'})
        ca1.set({'child': None})
        self.assertEqual(ca1.data, {'mother': '3', 'child': None})

        # abolish
        ca1.abolish()
        self.assertEqual(ca1.data, {})
        ca1.fetch(update=True)
        self.assertEqual(ca1.data, {})

        ca1.set({'father': 1, 'mother': '2'})
        self.assertEqual(ca1.data, {'father': 1, 'mother': '2'})
        ca1.fetch(update=True)
        self.assertEqual(ca1.data, {'father': 1, 'mother': '2'})

    def test_Counter(self):
        key_space = 'user:2'
        ca1 = CachedAttr(key_space)
        ca1.set({'father': 1, 'mother': '2', 'son': -10, 'daughter': '0'})
        self.assertEqual(ca1.data, {'father': 1, 'mother': '2', 'son': -10, 'daughter': '0'})
        ca1.incr(['father', 'son'])  # , 'mother', 'daughter'])
        self.assertEqual(ca1.data, {'father': 2, 'mother': '2', 'son': -9, 'daughter': '0'})

        ca1_remote = CachedAttr(key_space)
        ca1_remote.fetch()
        self.assertEqual(ca1_remote.data, ca1.data)

        ca1_remote2 = CachedAttr(key_space)
        ca1_remote2.incr(['father', 'son'])
        self.assertEqual(ca1_remote2.data, {'son': -8, 'father': 3})

        ca1_remote3 = CachedAttr(key_space)
        ca1_remote3.incr(['neighbor', 'son'])
        self.assertEqual(ca1_remote3.data, {'son': -7, 'neighbor': 1})
        ca1_remote3.incr(['neighbor', 'guest'], -1)
        self.assertEqual(ca1_remote3.data, {'son': -7, 'neighbor': 0, 'guest': -1})

        ca1_remote4 = CachedAttr(key_space)
        ca1_remote4.fetch()
        ca1_remote.fetch(update=True)
        self.assertEqual(ca1_remote4.data, ca1_remote.data)
        self.assertEqual(ca1_remote.data,
                         {'father': 3, 'mother': '2', 'son': -7, 'daughter': '0', 'neighbor': 0, 'guest': -1})

