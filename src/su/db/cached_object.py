import datetime
from collections import OrderedDict
from su.util import flatten, tup, slice_seq, alnum, epoch_seconds
from su.db.operators import desc
from su.g import permacache_client
from su.env import LOGGER
from su.redix import NoneHolder
#from redis.exceptions import ResponseError

MAX_ITEMS = 1000


def compare(a, b, orders):
    for i, s in enumerate(orders):
        v1 = a[1][i]
        v2 = b[1][i]
        if v1 != v2:
            result = (v1 > v2) - (v1 < v2)
            result = -1 * result if s else result  # s: True -> desc, False -> asc
            return result
    return 0


def sorts_comparator(orders):
    class SortsComparator:
        def __init__(self, item):
            self.item = item
            self.orders = orders

        def cmp(self, a, b):
            return compare(a, b, self.orders)

        def __lt__(self, other):
            return self.cmp(self.item, other.item) < 0
    return SortsComparator


class RedisHashesBackend:
    # space is the suffix of the hash key
    spaces = []
    # filter function for spaces before set
    space_set_filter = {}

    def __init__(self, redis_instance=None):
        self.redis = permacache_client if not redis_instance else redis_instance

    @classmethod
    def make_key(cls, key, space):
        return "%s:%s" % (key, space)

    def get_multi(self, keys, **kwargs):
        spaces = tup(kwargs.pop('spaces', self.spaces))
        fields = tup(kwargs.pop('fields', None))

        if not spaces:
            return {}

        with self.redis.pipeline() as pipe:
            for key in keys:
                for space in spaces:
                    if fields:
                        pipe.hmget(self.make_key(key, space), fields)
                    else:
                        pipe.hgetall(self.make_key(key, space))
            cached = pipe.execute()

        results = {}
        block_size = len(spaces)
        for i, key in enumerate(keys):
            offset = i*block_size
            # offset_end = (i+1)*block_size-1
            # if all(not got for got in cached[offset_start:offset_end]):
            #     continue
            results[key] = {}
            for j, k in enumerate(spaces):
                if fields:
                    results[key][k] = {field: cached[offset+j][idx] for idx, field in enumerate(fields)}
                else:
                    results[key][k] = cached[offset+j]

        return results

    def get(self, key, **kwargs):
        result = self.get_multi([key], **kwargs)
        return result[key] if key in result else None

    def abolish_multi(self, keys, **kwargs):
        if not keys:
            return

        spaces = tup(kwargs.pop('spaces', self.spaces))
        all_keys = []
        for key in keys:
            for space in spaces:
                all_keys.append(self.make_key(key, space))
        return self.redis.delete(*all_keys)

    def reset_multi(self, keys, **kwargs):
        return self.abolish_multi(keys, **kwargs)

    def set(self, key, item, **kwargs):
        setted = self.set_multi({key: item}, **kwargs)
        return setted[key] if key in setted else None

    def set_multi(self, data, **kwargs):
        # data is dict: {key: item}, item: {field: value}
        if not data:
            return
        spaces = tup(kwargs.pop('spaces', self.spaces))
        with self.redis.pipeline() as pipe:
            setted = {}
            for key, item in data.items():
                if not item:
                    continue
                setted[key] = {}
                for space in spaces:
                    to_set_value = self.space_set_filter[space](item) if space in self.space_set_filter \
                        else item.get(space, None)
                    assert isinstance(to_set_value, (dict, type(None)))
                    if to_set_value:
                        pipe.hmset(self.make_key(key, space), to_set_value)
                        setted[key][space] = to_set_value
            pipe.execute()
            return setted

    def incr(self, key, field, amount=1, **kwargs):
        return self.incr_multi(((key, field, amount),), **kwargs)

    def incr_multi(self, data, **kwargs):
        # data is tuple: (key, field, amount)
        if not data:
            return

        spaces = tup(kwargs.pop('spaces', self.spaces))
        block_size = len(spaces)
        with self.redis.pipeline() as pipe:
            for key, field, amount in data:
                # todo: assuming field always exists?
                assert field is not None
                for space in spaces:
                    pipe.hincrby(self.make_key(key, space), field, amount)

            results = {}
            # todo: handle ReponseError exception
            response = pipe.execute()
            for i, item in enumerate(data):
                offset = i*block_size
                key, field, amount = item
                results.setdefault(key, {})
                for j, space in enumerate(spaces):
                    results[key].setdefault(space, {})
                    results[key][space][field] = response[offset+j]
            return results

    def delete(self, key, fields, **kwargs):
        if not fields:
            return
        return self.delete_multi({key: fields}, **kwargs)

    def delete_multi(self, data, **kwargs):
        if not data:
            return

        spaces = tup(kwargs.pop('spaces', self.spaces))
        with self.redis.pipeline() as pipe:
            for key, fields in data.items():
                if fields:
                    for space in spaces:
                        pipe.hdel(self.make_key(key, space), *fields)
            return pipe.execute()


class RedisBackedList(RedisHashesBackend):
    spaces = ['status', 'data', 'timestamps']
    space_set_filter = {
        'timestamps': lambda item: RedisBackedList.make_timestamps_for_item(item)
    }

    @classmethod
    def make_timestamps_for_item(cls, item):
        t = cls.make_timestamp()
        return {k: t for k in item['data']}

    @classmethod
    def make_timestamp(cls):
        return epoch_seconds()

    def get_multi(self, keys, **kwargs):
        result = super().get_multi(keys, **kwargs)
        filtered = {}
        for key, spaces in result.items():
            if spaces.get('status') and spaces['status'].get('init'):
                filtered[key] = spaces

        LOGGER.debug('querying redis@%s [list], hit:%s/%s keys: %s' % (self.redis, len(filtered), len(keys), keys))
        return filtered

    def delete_multi(self, data, **kwargs):
        return super().delete_multi(data, spaces=['data', 'timestamps'], **kwargs)

    def reset_multi(self, keys, **kwargs):
        return super().reset_multi(keys, spaces=['data', 'timestamps'], **kwargs)

    def incr_multi(self, data, **kwargs):
        return super().incr_multi(data, spaces='data', **kwargs)


class RedisBackedAttribute(RedisHashesBackend):
    def __init__(self, space, redis_instance=None):
        RedisHashesBackend.__init__(self, redis_instance)
        self.space = space

    def get_multi(self, keys, **kwargs):
        space = kwargs.pop('spaces', self.space)
        result = super().get_multi(keys, spaces=space, **kwargs)
        filtered = {}
        for key, spaces in result.items():
            filtered[key] = {}
            for k, v in spaces[space].items():
                if v is NoneHolder:
                    filtered[key][k] = None
                elif v is not None:
                    filtered[key][k] = v

        LOGGER.debug('querying redis@%s [space:%s], hit:%s/%s keys: %s' %
                     (self.redis, space, len(filtered), len(keys), keys))
        return filtered

    def set_multi(self, data, **kwargs):
        space = kwargs.pop('spaces', self.space)
        result = super().set_multi(data, spaces=space, **kwargs)
        filtered = {}
        for key, spaces in result.items():
            filtered[key] = {k: v for k, v in spaces[space].items()}
        return filtered

    def get(self, key, **kwargs):
        space = kwargs.pop('spaces', self.space)
        return super().get(key, spaces=space, **kwargs)

    def abolish_multi(self, keys, **kwargs):
        space = kwargs.pop('spaces', self.space)
        return super().abolish_multi(keys, spaces=space, **kwargs)

    def reset_multi(self, keys, **kwargs):
        space = kwargs.pop('spaces', self.space)
        return super().reset_multi(keys, spaces=space, **kwargs)

    def set(self, key, item, **kwargs):
        space = kwargs.pop('spaces', self.space)
        return super().set(key, item, spaces=space, **kwargs)

    def incr(self, key, fields, **kwargs):
        space = kwargs.pop('spaces', self.space)
        return super().incr(key, fields, spaces=space, **kwargs)

    def incr_multi(self, data, **kwargs):
        space = kwargs.pop('spaces', self.space)
        result = super().incr_multi(data, spaces=space, **kwargs)
        filtered = {}
        for key, spaces in result.items():
            filtered[key] = {k: v for k, v in spaces[space].items()}
        return filtered

    def delete(self, key, fields, **kwargs):
        space = kwargs.pop('spaces', self.space)
        return super().delete(key, fields, spaces=space, **kwargs)

    def delete_multi(self, data, **kwargs):
        space = kwargs.pop('spaces', self.space)
        return super().delete_multi(data, spaces=space, **kwargs)


class CachedObjectBase:
    cache = None

    def __init__(self, key):
        self.key = key
        self._fetched = False
        self._hit = False
        self._data = None

    def _load(self, cached_data, reset=False):
        raise NotImplementedError

    @property
    def data(self):
        return self._data

    def fetch(self, update=False, **kwargs):
        self.fetch_multi([self], update=update, **kwargs)
        return self

    @classmethod
    def fetch_multi(cls, cached_objects, update=False, **kwargs):
        to_fetch = []
        for obj in cached_objects:
            if not update and obj._fetched:
                continue
            to_fetch.append(obj)
        if not to_fetch:
            return

        fetched = cls.cache.get_multi([q.key for q in to_fetch], **kwargs)

        for obj in to_fetch:
            if obj.key in fetched:
                obj._load(fetched[obj.key], reset=True)
            obj._fetched = True
        return {cl.key: cl for cl in cached_objects if cl._hit}

    @classmethod
    def _wrap(cls, data, **kwargs):
        raise NotImplementedError

    def set(self, items, **kwargs):
        result = self.set_multi({self: items}, **kwargs)
        return result[self.key] if self.key in result else None

    @classmethod
    def set_multi(cls, data, **kwargs):
        if not data:
            return

        setted = cls.cache.set_multi(cls._wrap(data, **kwargs))
        ret = {}

        for obj in data:
            if obj.key in setted:
                obj._load(setted[obj.key])
                ret[obj.key] = obj._data
        return ret

    def reset(self, items):
        return self.reset_multi({self: items})

    @classmethod
    def reset_multi(cls, data):
        # data: {object: value}
        raise NotImplementedError

    def incr(self, fields, amount=1, **kwargs):
        return self.incr_multi(list((self, field, amount) for field in tup(fields)), **kwargs)

    @classmethod
    def incr_multi(cls, data, **kwargs):
        raise NotImplementedError

    def delete(self, fields):
        if not fields:
            return

        return self.delete_multi({self: fields})

    @classmethod
    def delete_multi(cls, data):
        # data: {object: [fields}
        raise NotImplementedError

    def abolish(self):
        return self.abolish_multi([self])

    @classmethod
    def abolish_multi(cls, objects):
        raise NotImplementedError


def make_cached_attribute_cls(space):
    class CachedAttribute(CachedObjectBase):
        cache = RedisBackedAttribute(space)

        def __init__(self, key):
            CachedObjectBase.__init__(self, key)
            self._data = {}

        def _load(self, cached_data, reset=False):
            if not reset:
                self._data.update(cached_data)
            else:
                #print("load without reset: %s -> %s " % (self.data, cached_data))
                self._data = cached_data

            self._hit = True if self._data else False

        @classmethod
        def _wrap(cls, data, **kwargs):
            wrapped = {}
            for ca, items in data.items():
                wrapped[ca.key] = {space: tup(items)}
            return wrapped

        @classmethod
        def incr_multi(cls, data, **kwargs):
            if not data:
                return

            packed_data = list((ca.key, field, amount) for ca, field, amount in data)
            ret = cls.cache.incr_multi(packed_data)
            for ca, field, amount in data:
                ca._load(ret[ca.key])
            return ret

        @classmethod
        def reset_multi(cls, data):
            cls.cache.reset_multi([cl.key for cl in data])
            for ca in data:
                ca._data = {}
            ret = cls.set_multi(data)
            for ca in data:
                ca._hit = True
                ca._fetched = True
            return ret

        @classmethod
        def delete_multi(cls, data):
            if not data:
                return

            packed_data = {ca.key: tup(fields) for ca, fields in data.items()}
            cls.cache.delete_multi(packed_data)
            for ca in data:
                for item in packed_data[ca.key]:
                    if item in ca._data:
                        ca._data.pop(item)

        @classmethod
        def abolish_multi(cls, objects):
            keys = []
            for cl in objects:
                cl._data = {}
                cl._hit = False
                cl._fetched = False
                keys.append(cl.key)
            cls.cache.abolish_multi(keys)

    return CachedAttribute

CachedAttr = make_cached_attribute_cls(space='attr')


class _CachedListBase(CachedObjectBase):
    # shared by CachedList/MergedCachedList
    def __init__(self, key, sort=None, filter_fn=None, after=None, before=None):
        CachedObjectBase.__init__(self, key)
        self._data = {}
        self.sort = sort
        self.filter = filter_fn
        self._sort_cols = [s.col for s in self.sort]
        self._sort_orders = [isinstance(sort, desc) for sort in self.sort]
        self._sorted_data = OrderedDict()
        self._after = None
        self._before = None
        self.reset_anchor(after=after, before=before, resort=False)

    @property
    def data(self):
        return self._sorted_data

    def _pack_item(self, item):
        filtered_item = self.filter(item) if self.filter else item
        lst = []
        for col in self._sort_cols:
            attr = getattr(item, col)
            if isinstance(attr, datetime.datetime):
                attr = epoch_seconds(attr)
            lst.append(attr)
        return filtered_item._id, lst

    def _pack_items(self, items):
        data = {}
        if items:
            items_tup = tup(items)
            for item in items_tup:
                k, v = self._pack_item(item)
                data[str(k)] = v
        return data

    def _sort_data(self):
        if self._data:
            if not self._sort_orders:
                self._sorted_data = self._data
                return
            items = self._data.items()

            # if self._after:
            #     items = list(filter(lambda x: compare(x, self._after, self._sort_orders) > 0, items))
            # if self._before:
            #     items = list(filter(lambda x: compare(self._before, x, self._sort_orders) > 0, items))
            # self._sorted_data = OrderedDict(sorted(items, key=sorts_comparator(self._sort_orders)))
            sorted_data = OrderedDict(sorted(items, key=sorts_comparator(self._sort_orders)))
            lt_fn = lambda x, y: compare(x, y, orders=self._sort_orders) < 0

            if self._after:
                sorted_data = slice_seq(sorted_data, self._after, lt_fn=lt_fn)
            if self._before:
                sorted_data = slice_seq(sorted_data, self._before, lt_fn=lt_fn, direction='before')
            self._sorted_data = sorted_data

    def reset_anchor(self, after=None, before=None, resort=True):
        self._after = self._pack_item(after) if after else None
        self._before = self._pack_item(before) if before else None
        if resort:
            self._sort_data()

    def __iter__(self):
        self.fetch()
        if self._sorted_data:
            for data in self._sorted_data[:MAX_ITEMS]:
                yield data[0]

    def list(self):
        return list(self)

list_backend = RedisBackedList()


class CachedList(_CachedListBase):
    cache = list_backend

    def __init__(self, key, sort=None, filter_fn=None, is_precomputed=False, after=None, before=None):
        self.key = key
        self.timestamps = None
        self.is_precomputed = is_precomputed
        _CachedListBase.__init__(self, key, sort=sort, filter_fn=filter_fn, after=after, before=before)

    def _load(self, cached_data, reset=False):
        inited = cached_data['status']['init'] if 'status' in cached_data else None
        data = cached_data['data'] if 'data' in cached_data else OrderedDict()
        timestamps = cached_data['timestamps'] if 'timestamps' in cached_data else None

        if not reset:
            self._data.update(data)
        else:
            self._data = data
        if timestamps and self.timestamps and not reset:
            self.timestamps.update(timestamps)
        else:
            self.timestamps = timestamps

        if self._data or inited:
            self._hit = True
        else:
            self._hit = False

        self._sort_data()

    @classmethod
    def _wrap(cls, data, **kwargs):
        wrapped = {}
        timestamp = kwargs.pop('timestamp', None)
        for cl, items in data.items():
            record = {'data': cl._pack_items(items)}
            if timestamp:
                record['timestamp'] = timestamp
            if not cl._hit:
                record['status'] = {'init': 1}
            wrapped[cl.key] = record
        return wrapped

    @classmethod
    def reset_multi(cls, data):
        cls.cache.reset_multi([cl.key for cl in data])
        for cl in data:
            cl._data = {}
            cl._sorted_data = OrderedDict()
            cl.timestamps = None
        ret = cls.set_multi(data)
        for cl in data:
            cl._hit = True
            cl._fetched = True
        return ret

    @classmethod
    def delete_multi(cls, data):
        if not data:
            return

        packed_data = {cl.key: [str(cl._pack_item(entity)[0]) for entity in tup(items)] for cl, items in data.items()}
        ret = cls.cache.delete_multi(packed_data)
        for cl in data:
            for item in packed_data[cl.key]:
                if cl._data:
                    cl._data.pop(item, None)
                if cl.timestamps:
                    cl.timestamps.pop(item, None)
            cl._sort_data()
        return ret

    @classmethod
    def abolish_multi(cls, cached_lists):
        keys = []
        for cl in cached_lists:
            cl._data = {}
            cl._sorted_data = OrderedDict()
            cl.timestamps = None
            cl._hit = False
            cl._fetched = False
            keys.append(cl.key)
        cls.cache.abolish_multi(keys)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other.key

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.key)


class MergedCachedList(_CachedListBase):
    def __init__(self, lists):
        self.lists = lists

        if lists:
            sort = lists[0].sort
            assert all(sort == cl.sort for cl in lists)
        else:
            sort = []
        _CachedListBase.__init__(self, lists[0].key, sort=sort)

    def fetch(self, update=False, **kwargs):
        CachedList.fetch_multi(self.lists, update)
        self._data = flatten([cl._data for cl in self.lists])


def filter_dummy(item):
    return item


def filter_entity(item):
    return item.entity

def filter_entity1(item):
    return item._entity1

def filter_entity2(item):
    return item._entity2


def cached_query(filter_fn=filter_dummy, sort=None, is_precomputed=False):
    def cached_query_decorator(fn):
        def cached_query_wrapper(*args):
            assert fn.__name__.startswith("get_")
            row_key_components = [fn.__name__[len('get_'):]]

            if len(args) > 0:
                # we want to accept either a Thing or a thing's ID at this
                # layer, but the query itself should always get just an ID
                if hasattr(args[0], '_id'):
                    args = list(args)
                    args[0] = args[0]._id

                entity_id = alnum(args[0])
                row_key_components.append(entity_id)

            row_key_components.extend(str(x) for x in args[1:])
            row_key = '.'.join(row_key_components)

            query = fn(*args)

            if query:
                # db-backed query
                query_sort = query._sort
            else:
                # redis-backed query
                assert sort
                query_sort = sort

            return CachedList(row_key, query_sort, filter_fn, is_precomputed)
        return cached_query_wrapper
    return cached_query_decorator


def merged_cached_query(fn):
    def merge_wrapper(*args, **kwargs):
        queries = fn(*args, **kwargs)
        return MergedCachedList(queries)
    return merge_wrapper
