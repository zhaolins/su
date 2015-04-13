import pickle
from beaker.ext import database
from beaker.cache import clsmap
from beaker.exceptions import InvalidCacheBackendError
from beaker.container import NamespaceManager, Container
from beaker.synchronization import file_synchronizer
from beaker.util import verify_directory
from beaker.exceptions import MissingCacheParameter

try:
    from redis import StrictRedis
except ImportError:
    raise InvalidCacheBackendError("Redis cache backend requires the 'redis' library")

from su.g import redis_session
from su.env import LOGGER


class NoSqlManager(NamespaceManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NamespaceManager.__init__(self, namespace)
        self.conn = None
        self.expires = int(params.get('expires', 0))

        if lock_dir:
            self.lock_dir = lock_dir
        elif data_dir:
            self.lock_dir = data_dir + "/container_tcd_lock"
        else:
            self.lock_dir = None

        if self.lock_dir:
            verify_directory(self.lock_dir)

        conn_params = {}
        host = port = None
        if url:
            parts = url.split('?', 1)
            url = parts[0]
            if len(parts) > 1:
                conn_params = dict(p.split('=', 1) for p in parts[1].split('&'))

            host, port = url.split(':', 1)
            port = int(port)

        self.open_connection(host, port, **conn_params)

    def open_connection(self, host, port, **conn_params):
        raise NotImplementedError

    def get_creation_lock(self, key):
        return file_synchronizer(
            identifier="tccontainer/funclock/%s" % self.namespace,
            lock_dir=self.lock_dir)

    def _format_key(self, key):
        return self.namespace + '_'

    def __getitem__(self, key):
        return pickle.loads(self.conn.get(self._format_key(key)))

    def __contains__(self, key):
        return self._format_key(key) in self.conn

    def has_key(self, key):
        return key in self

    def set_value(self, key, value, expiretime=None):
        self.conn[self._format_key(key)] = pickle.dumps(value)

    def __setitem__(self, key, value):
        self.set_value(key, value)

    def __delitem__(self, key):
        del self.conn[self._format_key(key)]

    def do_remove(self):
        raise NotImplementedError

    def keys(self):
        return self.conn.keys()


class NoSqlManagerContainer(Container):
    namespace_manager = NoSqlManager


class RedisManager(NoSqlManager):
    def open_connection(self, host, port, **params):
        self.conn = redis_session

    def __contains__(self, key):
        contains = True if self.conn.get(self._format_key(key)) else False
        LOGGER.debug('%s contained in redis cache (as %s) : %s' %
                     (key, self._format_key(key), contains))
        return contains

    def __getitem__(self, key):
        LOGGER.debug("session key: %s" % self._format_key(key))
        return self.conn.get(self._format_key(key))

    def set_value(self, key, value, expiretime=None):
        expiretime = self.expires if expiretime is None else 0
        key = self._format_key(key)
        self.conn.set(key, value, expiretime)

    def __delitem__(self, key):
        key = self._format_key(key)
        self.conn.delete(key)

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        self.conn.flush()

    def keys(self):
        raise self.conn.keys('beaker:%s:*' % self.namespace)


class RedisContainer(Container):
    namespace_manager = RedisManager

clsmap._clsmap['redis'] = RedisContainer.namespace_manager
