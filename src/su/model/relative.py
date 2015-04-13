__author__ = 'zhaolin'

from su.util import tup
from su.db.cached_object import CachedList, CachedAttr
from su.model.renderer import ENTITIES, ENTITY, STRING, INT, LIST, INTLIST
from su.util import diff_entities, flatten
from su.env import LOGGER


class RelativeBase:
    renderer = staticmethod(STRING)

    def __init__(self, entity, name):
        self._name = name
        self._data = None
        self._rule = self._parse(entity, name)
        self._fetched = False

    @property
    def data(self):
        # accessed by utils/renderer
        raise NotImplementedError

    @classmethod
    def _parse(cls, entity, name):
        # parse rules in entity._loaded_relatives[name]
        raise NotImplementedError

    def fetch(self, update=False, child_relatives=None):
        self.fetch_multi([self], update=update, child_relatives=child_relatives)

    @classmethod
    def fetch_multi(cls, relatives, update=False, child_relatives=None):
        # accessed by Entity.load_relatives_multi
        raise NotImplementedError


class HasOne(RelativeBase):
    renderer = staticmethod(ENTITY)

    @property
    def data(self):
        return self._data

    @classmethod
    def _parse(cls, entity, name):
        relative_cls, query_cls, foreign_key, *additions = entity._relative_rules[name]
        assert relative_cls == cls
        if not isinstance(foreign_key, str):
            raise AssertionError('%s not supported' % str(foreign_key))

        options = additions[0] if len(additions) else {}
        rule = {
            'query_cls': query_cls,
            'fk': getattr(entity, foreign_key),
            'relatives': options['relatives'] if 'relatives' in options else None,
            'return_attr': options['return_attr'] if 'return_attr' in options else None
        }
        return rule

    @classmethod
    def fetch_multi(cls, relatives, update=False, child_relatives=None):
        to_fetch = [r for r in relatives if not r._fetched or update]
        if not to_fetch:
            return
        query_cls = to_fetch[0]._rule['query_cls']
        child_relatives = to_fetch[0]._rule['relatives'] if child_relatives is None else child_relatives
        assert all(query_cls == rel._rule['query_cls'] for rel in to_fetch)

        found = set()
        foreign_ids = [rel._rule['fk'] for rel in to_fetch if rel not in found and not found.add(rel)]
        entities = query_cls._by_id(foreign_ids, load_prop=True, return_dict=False, read_only=True)

        if child_relatives:
            query_cls.load_relatives_multi(entities, child_relatives)

        for rel in to_fetch:
            rel._fetched = True
            rel._data = None
            for entity in entities:
                if entity._id == rel._rule['fk']:
                    if rel._rule['return_attr']:
                        rel._data = getattr(entity, rel._rule['return_attr'])
                    else:
                        rel._data = entity


class HasMany(RelativeBase):
    renderer = staticmethod(ENTITIES)

    def __init__(self, entity, name):
        RelativeBase.__init__(self, entity, name)
        self._cache_key = self.make_cache_key(entity._type, entity._id, name)
        self._cached_list = CachedList(self._cache_key, sort=self._rule['sort'], filter_fn=self._rule['filter_fn'])

    @property
    def data(self):
        return self._data

    @classmethod
    def make_cache_key(cls, entity_type, entity_id, name):
        return "%s:%d:%s" % (entity_type, entity_id, name)

    @classmethod
    def _parse(cls, entity, name):
        relative_cls, query_cls, key_mapping, *additions = entity._relative_rules[name]
        assert relative_cls == cls
        options = additions[0] if len(additions) else {}

        condition = []
        relative_columns = query_cls.c
        if isinstance(key_mapping, str):
            condition.append(entity._id == getattr(relative_columns, key_mapping))
        elif isinstance(key_mapping, dict):
            for key, mapto in key_mapping.items():
                condition.append(getattr(entity, key) == getattr(relative_columns, mapto))
        else:
            raise AssertionError('%s not supported' % str(key_mapping))

        if 'condition' in options:
            condition.extend(options['condition'])

        rule = {
            'query_cls': query_cls,
            'condition': condition,
            'sort': tup(options.get('sort', ())),
            'relatives': options.get('relatives', None),
            'result_cls': options.get('result_cls', query_cls),
            'filter_fn': options.get('filter_fn', None),
            'return_attr': options.get('return_attr', None),
            'limit': options.get('limit', None),
        }
        return rule

    def _query_backend(self):
        return self._query_multi_backend([self])[0]

    @classmethod
    def _query_multi_backend(cls, relatives, return_dict=True):
        if not relatives:
            return

        results = {}
        #TODO batch operation (preload?)
        for relative in relatives:
            rule = relative._rule
            conditions = rule['condition']
            sort = rule['sort']
            query_cls = rule['query_cls']

            query = query_cls._query(*conditions, sort=sort)
            results[relative._cache_key] = query._list()

        if return_dict:
            return results
        else:
            return [results.get(rel._cache_key) for rel in relatives]

    def sync(self, update=False):
        return self.sync_multi([self], update=update).get(self._cache_key, None)

    @classmethod
    def sync_multi(cls, relatives, update=False):
        rets = {}
        if not relatives:
            return rets

        filter_fn = relatives[0]._rule['filter_fn']
        authorities = cls._query_multi_backend(relatives)
        filtered_authorities = {k: [filter_fn(auth) if filter_fn else auth for auth in v]
                                for k, v in authorities.items()}

        cache_update = {}
        for r in relatives:
            if diff_entities(r.data, filtered_authorities[r._cache_key]):
                if r.data is not None:
                    LOGGER.warning("HasMany %s differed from db, %s => %s" %
                                   (r._name, r.data, filtered_authorities[r._cache_key]))
                rets[r._cache_key] = filtered_authorities[r._cache_key]
                if update:
                    cache_update[r] = authorities[r._cache_key]
            # else:
            #     LOGGER.warning("HasMany %s not differed from db, %s = %s" %
            #                    (r._name, r.data, filtered_authorities[r._cache_key]))

        if cache_update:
            CachedList.reset_multi({r._cached_list: v for r, v in cache_update.items()})
            cls.load_data_multi([r for r in cache_update],
                                flatten(filtered_authorities.values(), True, lambda x: x._id))
        return rets

    def fetch(self, update=False, child_relatives=None):
        self.fetch_multi([self], update=update, child_relatives=child_relatives)

    @classmethod
    def fetch_multi(cls, relatives, update=False, child_relatives=None):
        to_fetch = [r for r in relatives if not r._fetched or update]
        if not to_fetch:
            return

        # results: {relative._cache_key: CachedList}
        results = CachedList.fetch_multi([r._cached_list for r in to_fetch], update=update)
        to_init = []
        to_load = []
        for r in to_fetch:
            if r._cache_key not in results:
                to_init.append(r)
            else:
                to_load.append(r)

        if to_init:
            cls.set_multi({r: [] for r in to_init}, True)
            # cls.sync_multi(to_sync, update=True)
        if to_load:
            cls.load_data_multi(to_load, child_relatives=child_relatives)

    def _relative_entity_ids(self):
        return [int(k) for k in self._cached_list.data.keys()]

    @classmethod
    def load_data_multi(cls, relatives, loaded_entities=None, child_relatives=None):
        if not relatives:
            return
        result_cls = relatives[0]._rule['result_cls']
        child_relatives = relatives[0]._rule['relatives'] if child_relatives is None else child_relatives
        limit = relatives[0]._rule['limit']
        assert all(result_cls == rel._rule['result_cls'] for rel in relatives)

        #todo deepcopy here?
        entity_lst = [] if not loaded_entities else loaded_entities
        entity_map = {} if not entity_lst else {e._id: e for e in entity_lst}
        ids = []
        found_ids = set(entity_map)

        for rel in relatives:
            _ids = list(rel._cached_list.data.keys())[:limit]
            ids.extend([int(k) for k in _ids if k not in found_ids and not found_ids.add(k)])

        if ids:
            cached = result_cls._by_id(ids, read_only=False, return_dict=False, load_prop=True, ignore_missing=True)
            for entity in cached:
                i = entity._id
                if entity and i not in found_ids and not found_ids.add(i):
                    entity_lst.append(entity)
                    entity_map[i] = entity

        if child_relatives is not None:
            result_cls.load_relatives_multi(entity_lst, child_relatives)

        for rel in relatives:
            rel._fetched = True
            rel._data = []
            for i in rel._relative_entity_ids()[:limit]:
                if i in entity_map:
                    if rel._rule['return_attr']:
                        rel._data.append(getattr(entity_map[i], rel._rule['return_attr']))
                    else:
                        rel._data.append(entity_map[i])
                else:
                    #todo: error handling
                    LOGGER.warning('not found %s' % i)

    def set(self, items, update=False):
        self.set_multi({self: items}, update=update)

    @classmethod
    def set_multi(cls, data, update=False):
        CachedList.set_multi({r._cached_list: value for r, value in data.items()})
        if update:
            cls.fetch_multi((r for r in data), update=True)

    def delete(self, items, update=False):
        self.delete_multi({self: items}, update=update)

    @classmethod
    def delete_multi(cls, data, update=False):
        CachedList.delete_multi({r._cached_list: value for r, value in data.items()})
        if update:
            cls.fetch_multi((r for r in data), update=True)

    def abolish(self):
        self.abolish_multi([self])

    @classmethod
    def abolish_multi(cls, relatives):
        CachedList.abolish_multi((r._cached_list for r in relatives))

    @classmethod
    def make_cached_lists(cls, entity_cls, entity_ids, key):
        sample = entity_cls.sample()
        rule = cls._parse(sample, key)
        sort = rule['sort']
        filter_fn = rule['filter_fn']
        return {entity_id: CachedList(cls.make_cache_key(entity_cls._type, entity_id, key),
                                      sort=sort, filter_fn=filter_fn)
                for entity_id in entity_ids}

    @classmethod
    def batch_get(cls, entity_cls, entity_ids, key):
        cached_lists = cls.make_cached_lists(entity_cls, entity_ids, key)
        CachedList.fetch_multi(cached_lists.values())
        return {entity_id: [int(k) for k in cl.data.keys()] for entity_id, cl in cached_lists.items()}

    @classmethod
    def batch_set(cls, entity_cls, key, data):
        cached_lists = cls.make_cached_lists(entity_cls, data, key)
        CachedList.set_multi({cl: data[entity_id] for entity_id, cl in cached_lists.items()})

    @classmethod
    def batch_delete(cls, entity_cls, key, data):
        cached_lists = cls.make_cached_lists(entity_cls, data, key)
        CachedList.delete_multi({cl: data[entity_id] for entity_id, cl in cached_lists.items()})


class Attr(RelativeBase):
    renderer = staticmethod(STRING)

    def __init__(self, entity, name):
        RelativeBase.__init__(self, entity, name)
        self._cache_key = self.make_cache_key(entity._type, entity._id)
        self._cached_attr = CachedAttr(self._cache_key)

    @classmethod
    def make_cache_key(cls, entity_type, entity_id):
        return "%s:%d" % (entity_type, entity_id)

    @property
    def data(self):
        self._data = self._cached_attr.data.get(self._name, None)
        return self._data

    @classmethod
    def _parse(cls, entity, name):
        relative_cls, *additions = entity._relative_rules[name]
        assert relative_cls == cls
        return

    @classmethod
    def fetch_multi(cls, relatives, update=False, child_relatives=None):
        to_fetch = [r for r in relatives if not r._fetched or update]
        if not to_fetch:
            return
        fields = set([r._name for r in to_fetch])

        CachedAttr.fetch_multi([r._cached_attr for r in to_fetch], update=update, fields=fields)

        for rel in to_fetch:
            rel._fetched = True

    def set(self, value):
        self.set_multi({self: value})

    @classmethod
    def set_multi(cls, data):
        CachedAttr.set_multi({attr._cached_attr: {attr._name: value} for attr, value in data.items()})

    def delete(self):
        self.delete_multi([self])

    @classmethod
    def delete_multi(cls, attrs):
        CachedAttr.delete_multi({attr._cached_attr: attr._name for attr in attrs})

    def abolish(self):
        self.abolish_multi([self])

    @classmethod
    def abolish_multi(cls, attrs):
        CachedAttr.abolish_multi([attr._cached_attr for attr in attrs])

    @classmethod
    def make_cached_attrs(cls, entity_cls, entity_ids):
        return {entity_id: CachedAttr(cls.make_cache_key(entity_cls._type, entity_id)) for entity_id in entity_ids}

    @classmethod
    def batch_get(cls, entity_cls, entity_ids):
        cached_attrs = cls.make_cached_attrs(entity_cls, entity_ids)
        CachedAttr.fetch_multi(cached_attrs.values())
        return {entity_id: ca.data for entity_id, ca in cached_attrs.items()}

    @classmethod
    def batch_set(cls, entity_cls, data):
        cached_attrs = cls.make_cached_attrs(entity_cls, data)
        CachedAttr.set_multi({ca: data[entity_id] for entity_id, ca in cached_attrs.items()})

    @classmethod
    def batch_delete(cls, entity_cls, data):
        cached_attrs = cls.make_cached_attrs(entity_cls, data)
        CachedAttr.delete_multi({ca: data[entity_id] for entity_id, ca in cached_attrs.items()})


class NumAttr(Attr):
    renderer = staticmethod(INT)


class ListAttr(Attr):
    renderer = staticmethod(LIST)


class IntListAttr(Attr):
    renderer = staticmethod(INTLIST)


class Counter(NumAttr):
    def incr(self, amount=1):
        self.incr_multi({self: amount})

    @classmethod
    def incr_multi(cls, data):
        CachedAttr.incr_multi(list((attr._cached_attr, attr._name, amount) for attr, amount in data.items()))

    @classmethod
    def batch_incr(cls, entity_cls, entity_ids, name, amount=1):
        cached_attrs = cls.make_cached_attrs(entity_cls, entity_ids)
        CachedAttr.incr_multi(list((ca, name, amount) for entity_id, ca in cached_attrs.items()))

    def decr(self, amount=1):
        self.incr(amount*-1)

    @classmethod
    def _parse(cls, entity, name):
        relative_cls, query_cls, key_mapping, *additions = entity._relative_rules[name]
        assert relative_cls == cls
        options = additions[0] if len(additions) else {}

        condition = []
        relative_columns = query_cls.c
        if isinstance(key_mapping, str):
            condition.append(entity._id == getattr(relative_columns, key_mapping))
        elif isinstance(key_mapping, dict):
            for key, mapto in key_mapping.items():
                condition.append(getattr(entity, key) == getattr(relative_columns, mapto))
        else:
            raise AssertionError('%s not supported' % str(key_mapping))

        if 'condition' in options:
            condition.extend(options['condition'])

        rule = {
            'query_cls': query_cls,
            'condition': condition,
        }
        return rule

    def _query_backend(self):
        return self._query_multi_backend([self])[0]

    @classmethod
    def _query_multi_backend(cls, relatives, return_dict=True):
        if not relatives:
            return

        results = {}
        #TODO batch operation (preload?)
        for relative in relatives:
            rule = relative._rule
            conditions = rule['condition']
            query_cls = rule['query_cls']
            ret = query_cls._stat(*conditions).fetch()
            results[relative._cache_key] = ret

        if return_dict:
            return results
        else:
            return [results.get(rel._cache_key) for rel in relatives]

    @classmethod
    def fetch_multi(cls, relatives, update=False, child_relatives=None):
        super().fetch_multi(relatives, update=update)
        to_init = [r for r in relatives if r._cached_attr._fetched and not r._cached_attr._hit]
        if to_init:
            cls.set_multi({r: 0 for r in to_init})
            # rets = cls._query_multi_backend(to_set)
            # for r in to_set:
            #     val = rets[r._cache_key]
            #     #LOGGER.warning("Counter %s inited, value: %s" % (r._name, val))
            #     r.set(val)

    def sync(self, update=False):
        results = self.sync_multi([self], update=update)
        return results.get(self._cache_key, None)

    @classmethod
    def sync_multi(cls, relatives, update=False):
        authorities = cls._query_multi_backend(relatives)
        rets = {}
        for r in relatives:
            if r.data != authorities[r._cache_key]:
                # print("Counter %s differed from db, %s => %s" % (r._name, r.data, authorities[r._cache_key]))
                if update:
                    LOGGER.warning("Counter %s differed from db, %s => %s" %
                                   (r._name, r.data, authorities[r._cache_key]))
                    r.set(authorities[r._cache_key])
                rets[r._cache_key] = authorities[r._cache_key]
        return rets