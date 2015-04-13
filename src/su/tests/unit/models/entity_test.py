__author__ = 'zhaolin'

from su.tests import test_env
import unittest
from su.model.relative import HasMany, Counter
from su.tests.test_models import User, Post, Comment, Friendship, Vote, UserPostVote, UserCommentVote
from su.g import flush_cache, flush_permacache, backend, cache, reset_cache_chains
from su.db.operators import desc, asc
from datetime import datetime
from su.env import TIMEZONE


def init_db():
    # todo: sometimes process stopped here, why?
    backend.create_tables(reset_tables=True)
    users = []
    for i in range(1, 40):
        attrs = {'ups': i, 'downs': i}
        group = ['member会員'] if i % 2 else ['admin']
        user = User(role=i+1000, followers=i, followings=i, password=User.make_password(i), group=group, **attrs)
        user._commit()
        users.append(user)

    posts = []
    for i in range(1, 40):
        attrs = {'ups': i, 'downs': i}
        post = Post(user_id=int(i/4)+1, **attrs)
        post._commit()
        posts.append(post)

    friendships = []
    friendships.append(Friendship(users[1], users[9], 'follow'))
    friendships.append(Friendship(users[3], users[1], 'follow'))
    friendships.append(Friendship(users[3], users[9], 'follow'))
    friendships.append(Friendship(users[0], users[3], 'follow'))
    friendships.append(Friendship(users[0], users[1], 'follow'))
    friendships.append(Friendship(users[0], users[9], 'follow'))
    for fs in friendships:
        fs._commit()
    friendships.append(Friendship(users[0], users[8], 'follow'))

    post_votes = []
    post_votes.append(UserPostVote(users[0], posts[8], '1'))
    post_votes.append(UserPostVote(users[0], posts[2], '1'))
    for vote in post_votes:
        vote._commit()

    return users, posts, friendships, post_votes


class EntityTests(unittest.TestCase):
    def setUp(self):
        flush_cache()
        flush_permacache()
        self.users, self.posts, self.friendships, self.post_votes = init_db()

    def tearDown(self):
        pass

    def test_cached_attr_init(self):
        for u in self.users:
            self.assertTrue(u.follower_count.data == 0)
            self.assertTrue(u.following_count.data == 0)
            self.assertTrue(u.followers.data == [])
            self.assertTrue(u.followings.data == [])
        for u in self.users:
            u.followers.sync(True)
            u.followings.sync(True)
            u.follower_count.sync(True)
            u.following_count.sync(True)
            self.assertIsNone(u.followers.sync())
            self.assertIsNone(u.followings.sync())
            self.assertIsNone(u.follower_count.sync())
            self.assertIsNone(u.following_count.sync())
        for u in self.users:
            _u = User._by_id(u._id)
            self.assertIsNone(_u.followers.sync())
            self.assertIsNone(_u.followings.sync())
            self.assertIsNone(_u.follower_count.sync())
            self.assertIsNone(_u.following_count.sync())

    def test_RelativeList(self):
        user = User._by_id(1)
        friendship_commited = self.friendships[0]
        friendship_not_commited = self.friendships[-1]

        user.followings.sync(True)
        self.assertEqual([u._id for u in user.followings.data], [10, 2, 4])

        user.following_count.sync(True)
        self.assertEqual(user.following_count.data, 3)

        user.follower_count.sync(True)
        self.assertEqual(user.follower_count.data, 0)

        user.followings.set(friendship_not_commited)
        user.followings.fetch(True)
        self.assertEqual([u._id for u in user.followings.data], [9, 10, 2, 4])
        self.assertEqual([u._id for u in user.followings.sync()], [10, 2, 4])
        user.followings.sync(True)
        self.assertEqual([u._id for u in user.followings.data], [10, 2, 4])
        self.assertIsNone(user.followings.sync())

        user.followings.set(friendship_not_commited)
        user.followings.fetch(True)
        user.followings.delete(friendship_commited)
        user.followings.fetch(True)
        self.assertEqual([u._id for u in user.followings.data], [9, 2, 4])

        user.followings.abolish()
        user.followings.sync(True)
        self.assertEqual([u._id for u in user.followings.data], [10, 2, 4])

        user10 = User._by_id(10)
        user10.followers.sync(True)
        self.assertEqual([u._id for u in user10.followers.data], [1, 4, 2])
        user10.follower_count.sync(True)
        self.assertEqual(user10.follower_count.data, 3)

        flush_permacache()
        uninited = User._by_id(1)
        self.assertIsNotNone(uninited.followings.sync())

    def test_RedisAttr(self):
        user = User._by_id(1)

        self.assertEqual(user.redis_attr.data, None)
        user.redis_attr.set([1, 2, 3, 3, 4, 5])
        self.assertEqual(user.redis_attr.data, [1, 2, 3, 3, 4, 5])
        user.redis_attr.fetch(True)
        self.assertEqual(user.redis_attr.data, [1, 2, 3, 3, 4, 5])

        user.redis_attr.set([3, 4, 5])
        self.assertEqual(user.redis_attr.data, [3, 4, 5])
        user.redis_attr.fetch(True)
        self.assertEqual(user.redis_attr.data, [3, 4, 5])
        
        user.redis_attr.set(None)
        self.assertEqual(user.redis_attr.data, None)

        user = User._by_id(2)
        user.redis_attr.set([1, 2, 3, 3, 4, 5])
        user.redis_attr.delete()
        self.assertEqual(user.redis_attr.data, None)
        remote = User._by_id(2)
        self.assertEqual(remote.redis_attr.data, None)

        user = User._by_id(3)
        user.redis_attr.set([1, 2, 3, 3, 4, 5])
        user.redis_attr.abolish()
        self.assertEqual(user.redis_attr.data, None)

    def test_Stats(self):
        self.assertEqual(Post._stat(Post.c._user_id == 1).fetch(), 3)
        self.assertEqual(User._stat(User.c.followings == 3).fetch(), 1)
        self.assertEqual(Friendship._stat(Friendship.c._entity1_id == 1).fetch(), 3)

    def test_join_query(self):
        q = User._query(User.c._id < 5)
        self.assertEqual(len(q._list()), 4)
        q = User._query(User.c.followings < '5')
        self.assertEqual(len(q._list()), 4)

        q = User._query(User.c._id == 1)
        self.assertEqual(len(q._list()), 1)
        self.assertEqual(q._first()._id, 1)

        q = User._query(User.c.followings == 3)
        self.assertEqual(len(q._list()), 1)
        self.assertEqual(q._first()._id, 3)

        q = User._query(User.c.followings == '3')
        self.assertEqual(len(q._list()), 1)
        self.assertEqual(q._first()._id, 3)

        q = User._query(User.c.followings < 5)
        self.assertEqual([u._id for u in q], [1, 2, 3, 4])
        q = User._query(User.c.followings < 5, sort=desc('_created_at'))  # sort only works on entity attributes
        self.assertEqual([u._id for u in q], [4, 3, 2, 1])
        q = User._query(User.c.followings < 5, sort=desc('_ups'), limit=3)
        self.assertEqual([u._id for u in q], [4, 3, 2])

    def test_Counter(self):
        user = User._by_id(1)
        self.assertTrue(user.following_count._cached_attr._hit)
        self.assertEqual(user.following_count.data, 0)
        user.following_count.sync(True)
        self.assertEqual(user.following_count.data, 3)

        user.following_count.incr()
        self.assertEqual(user.following_count.data, 4)
        user.following_count.incr(2)
        self.assertEqual(user.following_count.data, 6)
        user.following_count.decr()
        self.assertEqual(user.following_count.data, 5)

        Friendship(user, User._by_id(3), 'follow')._commit()
        self.assertEqual(user.following_count.data, 5)
        self.assertEqual(user.following_count.sync(), 4)
        self.assertEqual(user.following_count.sync(True), 4)
        self.assertEqual(user.following_count.data, 4)
        self.assertIsNone(user.following_count.sync())

        self.assertEqual(user.follow(User._by_id(3)), False)
        user.follow(User._by_id(6))
        self.assertEqual(user.following_count.data, 5)
        self.assertIsNone(user.following_count.sync())

        other1 = User._by_id(6)
        other2 = User._by_id(7)
        other1.follow(other2)
        self.assertEqual(other1.following_count.data, 1)
        self.assertEqual(other2.follower_count.data, 1)

        other1 = User._by_id(8)
        other2 = User._by_id(9)
        self.assertEqual(other1.following_count.data, 0)
        self.assertEqual(other2.follower_count.data, 0)
        other1.follow(other2)
        self.assertEqual(other1.following_count.data, 1)
        self.assertEqual(other2.follower_count.data, 1)

    def test_attr_batch_query(self):
        users = [1, 2]
        loaded_users = [User._by_id(u) for u in users]

        # will do initialization in load_relatives()
        [u.load_relatives() for u in loaded_users]
        data = Counter.batch_get(User, users)
        self.assertEqual(data[1], {'comments_count': 0, 'following_count': 0, 'follower_count': 0})
        self.assertEqual(data[2], {'comments_count': 0, 'following_count': 0, 'follower_count': 0})

        # sync with db
        Counter.sync_multi([getattr(u, 'comments_count') for u in loaded_users], True)
        Counter.sync_multi([getattr(u, 'following_count') for u in loaded_users], True)
        Counter.sync_multi([getattr(u, 'follower_count') for u in loaded_users], True)
        data = Counter.batch_get(User, users)
        self.assertEqual(data[1], {'comments_count': 0, 'following_count': 3, 'follower_count': 0})
        self.assertEqual(data[2], {'comments_count': 0, 'following_count': 1, 'follower_count': 2})

        Counter.batch_set(User, {1: {'comments_count': 10}, 2: {'following_count': 0, 'follower_count': 2}})
        data = Counter.batch_get(User, users)
        self.assertEqual(data[1], {'comments_count': 10, 'following_count': 3, 'follower_count': 0})
        self.assertEqual(data[2], {'comments_count': 0, 'following_count': 0, 'follower_count': 2})

        Counter.batch_incr(User, users, 'comments_count')
        data = Counter.batch_get(User, users)
        self.assertEqual(data[1], {'comments_count': 11, 'following_count': 3, 'follower_count': 0})
        self.assertEqual(data[2], {'comments_count': 1, 'following_count': 0, 'follower_count': 2})

    def test_list_batch_query(self):
        user = User._by_id(2)
        user._created_at = datetime.now(TIMEZONE)
        others = [1, 3, 2, 4, 5, 5]
        for uid in others:
            u = User._by_id(uid)
            u.load_relatives()
        data = {entity_id: Friendship(User._by_id(entity_id), user, 'follow') for entity_id in others}

        HasMany.batch_set(User, 'followings', data)
        HasMany.batch_set(User, 'followers', data)
        for uid in others:
            u = User._by_id(uid)
            self.assertEqual(u.followings.data[0]._id, user._id)

        self.assertNotIn('followings', user._loaded_relatives)
        user.load_relatives()
        self.assertIn('followings', user._loaded_relatives)
        self.assertNotIn('followings', user.followings._data[0]._loaded_relatives)
        self.assertIn('followers', user.followings._data[0]._loaded_relatives)

        HasMany.batch_delete(User, 'followings', data)
        for uid in others:
            u = User._by_id(uid)
            self.assertNotIn(user._id, [r._id for r in u.followings.data])

    def test_follow(self):
        user = self.users[0]
        for u in self.users:
            u.following_count.sync(True)
            u.follower_count.sync(True)
            u.followings.sync(True)
            u.followers.sync(True)
            self.assertIsNone(u.following_count.sync())
            self.assertIsNone(u.follower_count.sync())
            self.assertIsNone(u.followings.sync())
            self.assertIsNone(u.followers.sync())

        user.follow(self.users[6])

        for u in self.users:
            self.assertIsNone(u.following_count.sync())
            self.assertIsNone(u.follower_count.sync())
            self.assertIsNone(u.followings.sync())
            self.assertIsNone(u.followers.sync())
        for u in self.users:
            remote = User._by_id(u._id)
            self.assertIsNone(remote.following_count.sync())
            self.assertIsNone(remote.follower_count.sync())
            self.assertIsNone(remote.followings.sync())
            self.assertIsNone(remote.followers.sync())

    def test_delete_user(self):
        user = User._by_id(1)
        Friendship(self.users[7], user, 'follow')._commit()
        user.load_relatives()

        followings = HasMany.batch_get(User, [1], 'followings')[1]
        followers = HasMany.batch_get(User, [1], 'followers')[1]

        counter1_before = {f: User._by_id(f).follower_count.data for f in followings}
        counter2_before = {f: User._by_id(f).following_count.data for f in followers}

        for f in followings:
            self.assertIn(user._id, [r._id for r in User._by_id(f).followers.data])
        for f in followers:
            self.assertIn(user._id, [r._id for r in User._by_id(f).followings.data])

        user.delete()

        self.assertEqual(User._by_id(1)._deleted, True)
        self.assertEqual(User._query(User.c._id == 1)._first(), None)

        for f in followings:
            self.assertNotIn(user._id, [r._id for r in User._by_id(f).followers.data])
        for f in followers:
            self.assertNotIn(user._id, [r._id for r in User._by_id(f).followings.data])

        follower_rels = Friendship._query(Friendship.c._entity2_id == user._id)._list()
        following_rels = Friendship._query(Friendship.c._entity1_id == user._id)._list()
        self.assertEqual(follower_rels, [])
        self.assertEqual(following_rels, [])

        counter1_after = {f: User._by_id(f).follower_count.data for f in followings}
        counter2_after = {f: User._by_id(f).following_count.data for f in followers}

        for u in followings:
            self.assertIsNone(User._by_id(u).following_count.sync())
            self.assertEqual(counter1_before[u], counter1_after[u]+1)
        for u in followers:
            self.assertIsNone(User._by_id(u).follower_count.sync())
            self.assertEqual(counter2_before[u], counter2_after[u]+1)

    def test_transaction(self):
        user = User._by_id(1)
        user.following_count.sync(True)
        self.assertEqual(user.following_count.data, 3)
        user.followings.sync(True)
        self.assertEqual([u._id for u in user.followings.data], [10, 2, 4])

        self.assertFalse(user.follow(User._by_id(10)))
        self.assertTrue(user.follow(User._by_id(5)))
        self.assertEqual(user.following_count.data, 4)
        self.assertEqual([u._id for u in user.followings.data], [5, 10, 2, 4])

        self.assertTrue(user.unfollow(User._by_id(10)))
        self.assertFalse(user.unfollow(User._by_id(6)))
        self.assertTrue(user.unfollow(User._by_id(4)))
        self.assertEqual(user.following_count.data, 2)
        self.assertEqual([u._id for u in user.followings.data], [5, 2])
        self.assertIsNone(user.following_count.sync())

        remote_user = User._by_id(1)
        self.assertNotEqual(user, remote_user)
        self.assertEqual(remote_user.following_count.data, 2)
        self.assertEqual([u._id for u in remote_user.followings.data], [5, 2])
        self.assertTrue(remote_user.follow(User._by_id(10)))
        self.assertEqual(remote_user.following_count.data, 3)
        self.assertEqual([u._id for u in remote_user.followings.data], [10, 5, 2])
        self.assertIsNone(remote_user.following_count.sync())

        alone = User._by_id(3)
        self.assertEqual(alone.following_count.data, 0)
        self.assertEqual(set([u._id for u in alone.followings.data]), set())

    def test_EntityDependency(self):
        origin_user = User._by_id(1, read_only=True)
        origin_user2 = User._by_id(1, read_only=True)
        self.assertEqual(id(origin_user), id(origin_user2))

        copy_user = User._by_id(1, read_only=False)
        copy_user2 = User._by_id(1)
        self.assertNotEqual(id(copy_user), id(origin_user))
        self.assertNotEqual(id(copy_user), id(copy_user2))

    def test_Relatives(self):
        user = User._by_id(1)
        user.load_relatives()
        self.assertTrue('posts', 'followings' in user.data)
        for post in user.posts.data:
            self.assertEqual(post._user_id, user._id)

    def test_fast_query(self):
        votes = UserPostVote._fast_query(self.users, self.posts, '1')
        for (u, p, l), v in votes.items():
            for _v in self.post_votes:
                if isinstance(v, UserPostVote):
                    self.assertEqual(u._id, v._entity1._id)
                    self.assertEqual(p._id, v._entity2._id)
                    if _v._entity1._id == u._id and _v._entity2._id == p._id:
                        self.assertEqual(_v._id, v._id)
                else:
                    if u._id == _v._entity1._id and p._id == _v._entity2._id:
                        print("%s == %s, %s == %s" % (u._id, _v._entity1, p._id, _v._entity2._id))
                    self.assertFalse(u._id == _v._entity1._id and p._id == _v._entity2._id)
