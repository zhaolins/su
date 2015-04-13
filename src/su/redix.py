from redis import StrictRedis, ConnectionPool
from redis.exceptions import *
from redis.client import string_keys_to_dict, dict_merge, BasePipeline, pairs_to_dict
import pickle


class NoneHolder:
    pass

verbose = False
echo = lambda x: print(x) if verbose else x
_b = lambda s: bytes(s, 'ascii')

PACK_PREFIX = '\x02'  # Start of text
NONE_HOLDER = NoneHolder
TYPE_STR = '*'
TYPE_PICKLE = '^'
TYPE_NONE = '!'


class _MergeFunc:
    def __init__(self, funcs):
        self.funcs = funcs

    def __call__(self, r, **options):
        for func in self.funcs:
            if func.__name__ == "<lambda>":
                r = func(r)
            else:
                r = func(r, **options)
        return r


def _merge_dict(dict1, dict2):
    merged = dict1.copy()
    for k2, v2 in dict2.items():
        if k2 in dict1:
            v1 = dict1[k2]
            v2 = list([v2]) if not isinstance(v2, list) else v2
            val = [v1] + v2 if not isinstance(v1, list) else v1 + v2
        else:
            val = v2
        merged[k2] = _MergeFunc(val) if isinstance(val, list) else val
    return merged


decode_bytes = lambda x: x.decode('utf-8') if isinstance(x, bytes) else x


def parse_zrange(response, **options):
    if options.get('withscores'):
        return [(decode_bytes(k), score) for k, score in response]
    else:
        return [decode_bytes(k) for k in response]


class InterpretedRedis(StrictRedis):
    """
    commands like getrange, append should be treated as bit operation
    """
    KIND_PACKERS = {
        str: lambda x: PACK_PREFIX + TYPE_STR + x,
        int: lambda x: x,
        bytes: lambda x: x,
        type(None): lambda x: _b(PACK_PREFIX + TYPE_NONE),
        float: lambda x: x,
        'default': lambda x: _b(PACK_PREFIX + TYPE_PICKLE) + pickle.dumps(x),
    }

    KIND_UNPACKERS = {
        _b(TYPE_STR): lambda x: x[2:].decode('utf-8'),
        _b(TYPE_PICKLE): lambda x: pickle.loads(x[2:]),
        _b(TYPE_NONE): lambda x: NONE_HOLDER,
        'default': lambda x: x,
    }

    PACK_COMMANDS = dict_merge(
        string_keys_to_dict(
            'ECHO',
            lambda x: (1, InterpretedRedis.pack_value(x[0]))
        ),
        string_keys_to_dict(
            'SET GETSET SETNX LPUSHX RPUSHX SISMEMBER',
            lambda x: (2, InterpretedRedis.pack_value(x[1]))
        ),
        string_keys_to_dict(
            'PSETEX SETEX LSET LREM SMOVE HSET HSETNX',
            lambda x: (3, InterpretedRedis.pack_value(x[2]))
        ),
        string_keys_to_dict(
            'LINSERT',
            [lambda x: (4, InterpretedRedis.pack_value(x[3])),
            lambda x: (3, InterpretedRedis.pack_value(x[2]))]
        ),
        string_keys_to_dict(
            'MSET MSETNX',
            lambda x: ((1, None), [InterpretedRedis.pack_value(arg) if i % 2 else arg for i, arg in enumerate(x)])
        ),
        string_keys_to_dict(
            'HMSET',
            lambda x: ((2, None), [InterpretedRedis.pack_value(arg) if i % 2 else arg for i, arg in enumerate(x[1:])])
        ),
        string_keys_to_dict(
            'LPUSH RPUSH SADD SREM',
            lambda x: ((2, None), [InterpretedRedis.pack_value(arg) for arg in x[1:]])
        )
    )

    RESPONSE_CALLBACKS = _merge_dict(StrictRedis.RESPONSE_CALLBACKS, dict_merge(
        string_keys_to_dict(
            'RANDOMKEY TYPE',
            decode_bytes
        ),
        string_keys_to_dict(
            'KEYS HKEYS',
            lambda x: [decode_bytes(v) for v in x] if x else []
        ),
        string_keys_to_dict(
            'ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZRANGEBYLEX',
            parse_zrange
        ),
        string_keys_to_dict(
            'BLPOP BRPOP',
            lambda x: (decode_bytes(x[0]), x[1]) if x else x
        ),
        string_keys_to_dict(
            'SCAN SSCAN',
            lambda x: (int(x[0]), [decode_bytes(k) for k in x[1]])
        ),
        string_keys_to_dict(
            'HSCAN',
            lambda x: (int(x[0]), {decode_bytes(k): InterpretedRedis.unpack_value(v) for k, v in x[1].items()})
        ),
        string_keys_to_dict(
            'ZSCAN',
            lambda x: (int(x[0]), {(decode_bytes(item[0]), InterpretedRedis.unpack_value(item[1])) for item in x[1]})
        ),
        string_keys_to_dict(
            'GET HGET GETSET LINDEX LPOP RPOP RPOPLPUSH BRPOPLPUSH SPOP SRANDMEMBER \
            HVALS SORT ECHO',
            lambda x: InterpretedRedis.unpack_value(x)
        ),
        string_keys_to_dict(
            'SMEMBERS SDIFF SINTER SUNION',
            lambda x: {InterpretedRedis.unpack_value(v) for v in x}
        ),
        string_keys_to_dict(
            'HGETALL',
            lambda x: {decode_bytes(k): InterpretedRedis.unpack_value(v) for k, v in x.items()}
        ),
        string_keys_to_dict(
            'MGET LRANGE HMGET',
            lambda x: [InterpretedRedis.unpack_value(v) for v in x]
        ),
        string_keys_to_dict(
            'BLPOP BRPOP',
            lambda x: tuple([InterpretedRedis.unpack_value(v) if i % 2 else decode_bytes(v) for i, v in enumerate(x)])
            if x else None
        ),
    ))

    @classmethod
    def pack_value(cls, value):
        t = type(value)
        if t not in cls.KIND_PACKERS:
            t = 'default'
        echo('packing value: %s %s -> %s' % (t, value, cls.KIND_PACKERS[t](value)))
        return cls.KIND_PACKERS[t](value)

    @classmethod
    def unpack_value(cls, value):
        if not isinstance(value, bytes):
            if isinstance(value, list):
                return [cls.unpack_value(_v) for _v in value]
            elif isinstance(value, tuple):
                return tuple(cls.unpack_value(_v) for _v in value)
            else:
                return value
        elif not value.startswith(_b(PACK_PREFIX)):
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError:
                    unpack_fn = 'default'
        elif len(value) < 2 or value[1:2] not in cls.KIND_UNPACKERS:
            unpack_fn = 'default'
        else:
            unpack_fn = value[1:2]
        echo('unpacking value: %s %s -> %s' % (unpack_fn, value, cls.KIND_UNPACKERS[unpack_fn](value)))
        return cls.KIND_UNPACKERS[unpack_fn](value)

    @classmethod
    def pack_args(cls, *args):
        packed_args = list(args)
        command_name = packed_args[0]
        if command_name in cls.PACK_COMMANDS:
            p = cls.PACK_COMMANDS[command_name]
            packers = p if isinstance(p, list) else [p]
            for packer in packers:
                offset, packed = packer(packed_args[1:])
                if isinstance(offset, tuple):
                    start, end = offset
                    packed_args[start:end] = packed
                else:
                    packed_args[offset] = packed
        echo("args before:" + str(args))
        echo("args after:" + str(packed_args))
        return packed_args

    def __getitem__(self, name):
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def execute_command(self, *args, **options):
        packed_args = self.pack_args(*args)
        return StrictRedis.execute_command(self, *packed_args, **options)

    def pipeline(self, transaction=True, shard_hint=None):
        return InterpretedPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)

    def msetex(self, items, time):
        with self.pipeline() as pipe:
            for k, v in items.items():
                pipe.set(k, v, ex=time)
            return pipe.execute()

    def madd(self, items):
        with self.pipeline() as pipe:
            for k, v in items.items():
                pipe.set(k, v, nx=True)
            rets = pipe.execute()
            return [False if ret is None else ret for ret in rets]

    def maddex(self, items, time):
        with self.pipeline() as pipe:
            for k, v in items.items():
                if time == 0:
                    pipe.set(k, v, nx=True)
                else:
                    pipe.set(k, v, nx=True, ex=time)
            rets = pipe.execute()
            return [False if ret is None else ret for ret in rets]

    def mincr(self, items, amount=1):
        with self.pipeline() as pipe:
            for k in items:
                pipe.incr(k, amount)
            return pipe.execute()


class InterpretedPipeline(BasePipeline, InterpretedRedis):
    def pipeline_execute_command(self, *args, **options):
        packed_args = self.pack_args(*args)
        return BasePipeline.pipeline_execute_command(self, *packed_args, **options)
