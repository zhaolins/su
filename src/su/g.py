__author__ = 'zhaolin.su'

from su.db.backends import KVSBackend
from su.stats import Stats, CacheStats
from su.redix import ConnectionPool
from su.cache import LocalCache, RedisCache, RedisChain
from su.lock import make_lock_factory
from su import env


bcrypt_salt_log_rounds = env.PASSWORD_STRENGTH

stats = Stats(env.STATSD['url'], env.STATSD['sample_rate'])

backend = KVSBackend(env.DB)

main_redispool = ConnectionPool(**env.REDIS_SERVERS['main'])
cache_redispool = ConnectionPool(**env.REDIS_SERVERS['cache'])
session_redispool = ConnectionPool(**env.REDIS_SERVERS['session'])
lock_redispool = ConnectionPool(**env.REDIS_SERVERS['lock'])

redis_db = RedisCache(pool=main_redispool)
redis_cache = RedisCache(pool=cache_redispool)
redis_session = RedisCache(pool=session_redispool)
redis_lock = RedisCache(pool=lock_redispool)

permacache_client = redis_db.client

cache = RedisChain((LocalCache(), redis_cache))
permacache = RedisChain((LocalCache(), redis_db))
make_lock = make_lock_factory(redis_lock, stats)

cache_chains = {
    'cache': cache,
    'permacache': permacache,
}


def reset_cache_chains():
    for name, cache_chain in cache_chains.items():
        cache_chain.reset_local()
        cache_chain.stats = CacheStats(stats, name)


def flush_cache():
    for c in cache.caches[1:]:
        c.flush()


def flush_permacache():
    for c in permacache.caches[1:]:
        c.flush()


def flush_session():
    redis_session.flush()

entity_cls_lookup = {}