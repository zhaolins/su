from su.tests import test_env
import bcrypt
from su.model.entity import Entity, make_relation_cls, make_multi_relation_cls
from su.model.relative import HasMany, HasOne, Counter, Attr, ListAttr
from su.db.cached_object import filter_entity1, filter_entity2
from su.db.operators import desc, asc
from su import g
from su.env import LOGGER


class User(Entity):
    _body_attrs = Entity._body_attrs + ('_role',)
    _int_attrs = Entity._int_attrs + ('_role',)
    _essentials = ('name',)
    _defaults = {'default_value_1': '本気です', 'default_value_2': '弱気です', 'default_value_3': '強気です'}
    _render_rules = Entity._render_rules + (
        ('password', 'INVISIBLE'),
        ('*relatives*', 'RELATIVES', {'scenarios2': 'fdsa'}),
        ('role', 'RENAME', {'value': 'role_desu'}),
        ('role_desu', 'RENAME', {'value': 'role_desune'}),
        ('*body*', 'INVISIBLE', {'on': 'bodyinvisible'}),
    )

    @classmethod
    def _construct(cls, _id, record):
        return cls(record.role, record.ups, record.downs, record.created_at, record.updated_at, record.deleted, record.spam, _id)

    def __init__(self, role, ups=0, downs=0, created_at=None, updated_at=None, deleted=False, spam=False, _id=None, **props):
        Entity.__init__(self, ups, downs, created_at, updated_at, deleted, spam, _id, **props)

        with self.safe_set_data:
            self._role = role

    @classmethod
    def sample(cls):
        return cls('__sample__')

    @property
    def _relative_rules(self):
        # todo: what if using a same name from props?
        return {
            'posts': (HasMany, Post, '_user_id', {
                'relatives': ['owner'],
                'sort': desc('_created_at'),
            }),
            'comments': (HasMany, Comment, '_user_id', {
                'sort': desc('_created_at'),
            }),
            'followings': (HasMany, Friendship, '_entity1_id', {
                'condition': [Friendship.c._label == 'follow'],
                'sort': desc('_created_at'),
                'relatives': {'followers': {'followers': 'followings'}},
                # 'limit': 1,
                'filter_fn': filter_entity2,
                'result_cls': User,
            }),
            'followers': (HasMany, Friendship, '_entity2_id', {
                'condition': [Friendship.c._label == 'follow'],
                'sort': desc('_created_at'),
                #'relatives': ['followings'],
                # 'limit': 1,
                'filter_fn': filter_entity1,
                'result_cls': User,
            }),
            'following_count': (Counter, Friendship, '_entity1_id', {
                'condition': [Friendship.c._label == 'follow'],
            }),
            'follower_count': (Counter, Friendship, '_entity2_id', {
                'condition': [Friendship.c._label == 'follow'],
            }),
            'redis_attr': (ListAttr,),
            'comments_count': (Counter, Comment, '_user_id')
        }

    @classmethod
    def make_password(cls, password):
        password = password if isinstance(password, str) else str(password)
        salt = bcrypt.gensalt(log_rounds=g.bcrypt_salt_log_rounds)
        return bcrypt.hashpw(password, salt)

    def validate_password(self, received):
        received = received if isinstance(received, str) else str(received)

        if not bcrypt.checkpw(received, self.password):
            return False

        salt_log_rounds = int(self.password.split("$")[2])
        if g.bcrypt_salt_log_rounds >= 4 and salt_log_rounds != g.bcrypt_salt_log_rounds:
            LOGGER.info('updating password due to salt changes: %s => %s' % (salt_log_rounds, g.bcrypt_salt_log_rounds))
            self.password = self.make_password(received)
            self._commit()
        return True

    def comment(self, target, content):
        c = Comment(self._id, target._type, target._id, content=content)
        c._commit()
        self.comments.set(c, True)
        self.comments_count.incr()
        return c

    def follow(self, other):
        with g.make_lock('follow', 'follow_%s' % self._id):
            if Friendship._query(Friendship.c._entity1_id == self._id, Friendship.c._entity2_id == other._id)._first():
                return False
            f = Friendship(self, other, 'follow')
            f._commit()
            self.following_count.incr()
            self.followings.set(f, True)
            other.follower_count.incr()
            other.followers.set(f, True)
            return f

    def unfollow(self, other):
        with g.make_lock('unfollow', 'unfollow_%s' % self._id):
            f = Friendship._query(Friendship.c._entity1_id == self._id, Friendship.c._entity2_id == other._id)._first()
            if not f:
                return False
            f._delete()
            self.following_count.decr()
            self.followings.delete(f, True)
            other.follower_count.decr()
            other.followers.delete(f, True)
            return True

    def delete(self):
        with g.make_lock('%s_delete' % self._type, 'delete_%s' % self._id):
            follower_rels = Friendship._query(Friendship.c._entity2_id == self._id)._list()
            following_rels = Friendship._query(Friendship.c._entity1_id == self._id)._list()
            follower_ids = [rel._entity1_id for rel in follower_rels]
            following_ids = [rel._entity2_id for rel in following_rels]

            for rel in follower_rels:
                rel._delete()
            for rel in following_rels:
                rel._delete()
            self._deleted = True
            self._commit()

            HasMany.batch_delete(User, 'followings', {rel._entity1_id: rel for rel in follower_rels})
            HasMany.batch_delete(User, 'followers', {rel._entity2_id: rel for rel in following_rels})
            Counter.batch_incr(User, follower_ids, 'following_count', amount=-1)
            Counter.batch_incr(User, following_ids, 'follower_count', amount=-1)


class Comment(Entity):
    _render_rules = Entity._render_rules + ()
    _body_attrs = Entity._body_attrs + ('_user_id', '_target_type', '_target_id')
    _int_attrs = Entity._int_attrs + ('_user_id', '_target_id')
    _essentials = ('content',)

    @classmethod
    def _construct(cls, _id, record):
        return cls(record.user_id, record.target_type, record.target_id,
                   record.ups, record.downs, record.created_at, record.updated_at, record.deleted, record.spam, _id)

    def __init__(self, user_id, target_type, target_id, ups=0, downs=0, created_at=None, updated_at=None, deleted=False, spam=False, _id=None, **props):
        Entity.__init__(self, ups, downs, created_at, updated_at, deleted, spam, _id, **props)

        with self.safe_set_data:
            self._user_id = user_id
            self._target_type = target_type
            self._target_id = target_id

    @classmethod
    def sample(cls):
        return cls(0, '__sample__', 0)


class Post(Entity):
    _render_rules = Entity._render_rules + (
        ('*relatives*', 'RELATIVES'),
    )
    _body_attrs = Entity._body_attrs + ('_user_id',)
    _int_attrs = Entity._int_attrs + ('_user_id',)
    _essentials = ('content',)

    @classmethod
    def _construct(cls, _id, record):
        return cls(record.user_id, record.ups, record.downs, record.created_at, record.updated_at,
                   record.deleted, record.spam, _id)

    def __init__(self, user_id, ups=0, downs=0, created_at=None, updated_at=None,
                 deleted=False, spam=False, _id=None, **props):
        Entity.__init__(self, ups, downs, created_at, updated_at, deleted, spam, _id, **props)

        with self.safe_set_data:
            self._user_id = user_id

    @classmethod
    def sample(cls):
        return cls(0)

    @property
    def _relative_rules(self):
        return {
            'owner': (HasOne, User, '_user_id'),
        }


class Friendship(make_relation_cls(User, User)):
    pass


class Vote(make_multi_relation_cls('vote', make_relation_cls(User, Post), make_relation_cls(User, Comment))):
    pass

UserPostVote = Vote.rel(User, Post)
UserCommentVote = Vote.rel(User, Comment)