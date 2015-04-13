__author__ = 'zhaolin'

import sys
import hashlib
from copy import copy, deepcopy
from datetime import datetime
from functools import reduce
from su.g import backend, make_lock, entity_cls_lookup
from su.db import operators
from su.db.backends import WrappedResultsProxy
from su.util import tup, cache_retriever
from su.model.base import ModelBase, NotFoundError
from su.env import LOGGER, TIMEZONE


class EntityMeta(type):
    def __init__(cls, name, bases, dct):
        if name == 'Entity':
            return

        cls._type = name.lower()
        entity_cls_lookup[cls._type] = cls

        super(EntityMeta, cls).__init__(name, bases, dct)


class Entity(ModelBase, metaclass=EntityMeta):
    _body_attrs = ('_ups', '_downs', '_created_at', '_updated_at', '_deleted', '_spam')
    _int_attrs = ('_ups', '_downs')
    _filter_attrs = {'_deleted': False, '_spam': False}
    _render_rules = (
        ('id, ups, downs', 'INT'),
        ('created_at, updated_at', 'DATETIME'),
    )
    _get_body = backend.get_entity_body
    _get_prop = backend.get_entity_prop
    _insert_body = backend.insert_entity_body
    _insert_prop = backend.insert_entity_prop
    _update_prop = backend.update_entity_prop
    _update_body = backend.update_entity_body
    _incr_attr = backend.incr_entity_body_attr
    _incr_prop = backend.incr_entity_prop

    @classmethod
    def _construct(cls, _id, record):
        return cls(record.ups, record.downs, record.created_at, record.updated_at, record.deleted, record.spam, _id)

    def __init__(self, ups=0, downs=0, created_at=None, updated_at=None, deleted=False, spam=False, _id=None, **props):
        ModelBase.__init__(self)

        with self.safe_set_data:
            if _id:
                self._id = _id
                self._created = True
                self._loaded = False

            self._ups = ups
            self._downs = downs
            self._created_at = created_at if created_at else datetime.now(TIMEZONE)
            self._updated_at = updated_at if updated_at else self._created_at
            self._deleted = deleted
            self._spam = spam

        for k, v in sorted(props.items()):
            record_changes = True if not self._created else False
            self.__setattr__(k, v, record_changes)

    def __repr__(self):
        return '<%s %s>' % (self.__class__._type,
                            self._id if self._created else '[unsaved]')

    @classmethod
    def _filter_rules(cls, *rules):
        default_rules = {filter_attr: cls.c[filter_attr] == default
                         for filter_attr, default in cls._filter_attrs.items()}
        filtered_rules = []

        for rule in rules:
            if not isinstance(rule, operators.op):
                continue
            if rule.lval_name in default_rules and isinstance(rule, operators.eq):
                default_rules.pop(rule.lval_name)
                if rule.rval == (True, False) or rule.rval == (1, 0):
                    continue
            filtered_rules.append(rule)

        for rule in default_rules.values():
            filtered_rules.append(rule)
        return filtered_rules

    @classmethod
    def _query(cls, *rules, **kwargs):
        filtered_rules = cls._filter_rules(*rules)
        return Entities(cls, *filtered_rules, **kwargs)

    @classmethod
    def _stat(cls, *rules, **kwargs):
        filtered_rules = cls._filter_rules(*rules)
        return Stat(cls, *filtered_rules, **kwargs)


class RelationMeta(type):
    def __init__(cls, name, bases, dct):
        if name == 'Relation':
            return

        cls._type = name.lower()
        entity_cls_lookup[cls._type] = cls

        super(RelationMeta, cls).__init__(name, bases, dct)

    def __repr__(cls):
        return '<relation: %s>' % cls._type


def make_relation_cls(entity1_cls, entity2_cls):
    class Relation(ModelBase, metaclass=RelationMeta):
        if not issubclass(entity1_cls, Entity) or not issubclass(entity2_cls, Entity):
            raise TypeError("Cannot create relationship between %s and %s" % (entity1_cls, entity2_cls))

        _entity1_cls = entity1_cls
        _entity2_cls = entity2_cls

        _body_attrs = ('_entity1_id', '_entity2_id', '_label', '_created_at', '_updated_at')
        _get_body = backend.get_relation_body
        _get_prop = backend.get_relation_prop
        _insert_body = backend.insert_relation_body
        _insert_prop = backend.insert_relation_prop
        _update_prop = backend.update_relation_prop
        _update_body = backend.update_relation_body
        _incr_prop = backend.incr_relation_prop
        _incr_attr = lambda *args, **kwargs: 'method not exist'
        _eagerly_loaded_prop = False

        @classmethod
        def _construct(cls, _id, record):
            return cls(record.entity1_id, record.entity2_id, record.label, record.created_at, record.updated_at, _id)

        def __init__(self, entity1, entity2, label, created_at=None, updated_at=None, _id=None, **attrs):
            ModelBase.__init__(self)

            with self.safe_set_data:
                if _id:
                    self._id = _id
                    self._created = True
                    self._loaded = False

                def get_id(entity_or_id):
                    if isinstance(entity_or_id, int):
                        return entity_or_id
                    else:
                        return entity_or_id._id
                        #raise TypeError('Cannot initialize relationship with %s and %s' % (entity1, entity2))

                self._entity1_id = get_id(entity1)
                self._entity2_id = get_id(entity2)
                self._label = label
                self._created_at = created_at if created_at else datetime.now(TIMEZONE)
                self._updated_at = updated_at if updated_at else self._created_at

            for k, v in attrs.items():
                record_changes = True if not self._created else False
                self.__setattr__(k, v, record_changes)

        #todo read_only flg
        @property
        def _entity1(self):
            return self._entity1_cls._by_id(self._entity1_id, self._eagerly_loaded_prop, read_only=False)

        @property
        def _entity2(self):
            return self._entity2_cls._by_id(self._entity2_id, self._eagerly_loaded_prop, read_only=False)

        def __getattr__(self, item):
            if item.startswith('_e1_'):
                return getattr(self._entity1, item[4:])
            elif item.startswith('_e2_'):
                return getattr(self._entity2, item[4:])
            else:
                return ModelBase.__getattr__(self, item)

        def __repr__(self):
            return ('<%s %s #%s: <%s %s> - <%s %s> %s>' %
                    (self._type, self._label, self._id,
                     self._entity1_cls._type, self._entity1_id,
                     self._entity2_cls._type, self._entity2_id,
                     '[unsaved]' if not self._created else '\b'))

        def _cache_key_relation_id(self):
            key = self._type + ':id:' + str((self._entity1_id, self._entity2_id, self._label))
            return key.replace(' ', '')

        def _commit(self, keys=None):
            ModelBase._commit(self)
            self._cache.set(self._cache_key_relation_id(), self._id)

        def _delete(self):
            backend.delete_rel(self._type, self._id)
            self._cache.delete(self._cache_key())
            self._cache.delete(self._cache_key_relation_id())
            self._label = 'un_' + self._label

        @classmethod
        def _by_id_with_entity(cls, ids, load_prop=False, return_dict=True, extra_props=None, eager_load=False,
                               load_entity_prop=False, ignore_missing=False, read_only=False):
            ids, is_single = tup(ids, True)
            records = cls._by_id(ids, load_prop=load_prop, return_dict=True, extra_props=extra_props,
                                 ignore_missing=ignore_missing, read_only=read_only)

            if records and eager_load:
                for record in records.values():
                    record._eagerly_loaded_prop = True
                load_entities(list(records.values()), load_entity_prop)

            if is_single:
                return records[ids[0]]
            elif return_dict:
                return records
            else:
                return [records.get(i) for i in ids]

        @classmethod
        def _fast_query(cls, entities1, entities2, label, load_prop=True, eager_load=True, 
                        load_entity_prop=False, read_only=False):
            entity1_lookup = {entity._id: entity for entity in tup(entities1)}
            entity2_lookup = {entity._id: entity for entity in tup(entities2)}

            entity1_ids = entity1_lookup.keys()
            entity2_ids = entity2_lookup.keys()

            label = tup(label)
            relation_dict = set((e1, e2, l)
                                for e1 in entity1_ids
                                for e2 in entity2_ids
                                for l in label)

            def db_retriever(lookup):
                rel_ids = {}
                e1_ids = set()
                e2_ids = set()
                labels = set()
                for e1, e2, l in lookup:
                    e1_ids.add(e1)
                    e2_ids.add(e2)
                    labels.add(l)

                if e1_ids and e2_ids and labels:
                    query = cls._query(cls.c._entity1_id == e1_ids,
                                       cls.c._entity2_id == e2_ids,
                                       cls.c._label == labels,
                                       read_only=True)
                else:
                    query = []

                for result in query:
                    rel_ids[(result._entity1_id, result._entity2_id, result._label)] = result._id
                for key in relation_dict:
                    if key not in rel_ids:
                        rel_ids[key] = None
                return rel_ids

            # retrieve by _cache_key_relation_id
            records = cache_retriever(cls._cache, relation_dict, miss_fn=db_retriever, prefix=(cls._type+':id:'))
            rel_ids = {rel_id for rel_id in records.values() if rel_id is not None}
            rels = cls._by_id_with_entity(rel_ids, load_prop=load_prop, eager_load=eager_load,
                                          load_entity_prop=load_entity_prop, read_only=read_only)
            results = {}
            for k, rel_id in records.items():
                e1, e2, l = k
                key = (entity1_lookup[e1], entity2_lookup[e2], l)
                results[key] = rels[rel_id] if rel_id is not None else None
            return results

        @classmethod
        def _is_mobius(cls):
            return cls._entity1_cls == cls._entity2_cls

        @classmethod
        def _query(cls, *args, **kwargs):
            return Relations(cls, *args, **kwargs)

        @classmethod
        def _stat(cls, *args, **kwargs):
            return Stat(cls, *args, is_relation=True, **kwargs)

    return Relation


class Stat(object):
    def __init__(self, cls, *rules, **kwargs):
        self._use_prop = False
        self._rules = []
        self._entity_cls = cls
        self._stat_func = None
        self._filter(*rules)
        self._is_relation = kwargs.pop('is_relation', False)

    def _filter(self, *rules):
        for op in operators.op_iter(rules):
            if not op.lval_name.startswith('_'):
                self._use_prop = True
        self._rules += rules

    def fetch(self):
        args = (self._entity_cls._type, self._rules)
        if self._use_prop:
            rp = backend.stat_props(*args, is_relation=self._is_relation)
        else:
            rp = backend.stat_entities(*args, is_relation=self._is_relation)
        ret = rp.first()
        return ret[0] if ret else None


class Query(object):
    def __init__(self, cls, *rules, **kwargs):
        self._rules = []
        self._entity_cls = cls

        self._read_cache = kwargs.get('read_cache')
        self._write_cache = kwargs.get('write_cache')
        self._cache_time = kwargs.get('cache_time', 10)
        self._limit = kwargs.get('limit')
        self._load_prop = kwargs.get('load_prop')
        self._sort_param = []
        self._sort = kwargs.get('sort', ())
        self._filter_primary_sort_only = kwargs.get('filter_primary_sort_only', False)

        self._filter(*rules)

    def _filter(self, *args, **kwargs):
        raise NotImplementedError

    def _cursor(self, *args, **kwargs):
        raise NotImplementedError

    def _fetch_proxy(self):
        raise NotImplementedError

    def _set_sort(self, sorts):
        sorts = tup(sorts)
        date_col = None
        op_sorts = []
        for sort in sorts:
            if not isinstance(sort, operators.sort):
                sort = operators.asc(sort)
            op_sorts.append(sort)

            if sort.col.endswith('_at'):
                date_col = sort.col

        if op_sorts and not date_col:
            op_sorts.append(operators.desc('_created_at'))

        self._sort_param = op_sorts

    def _get_sort(self):
        return self._sort_param

    _sort = property(_get_sort, _set_sort)

    def _reverse(self):
        for sort in self._sort:
            sort.__class__ = operators.desc if isinstance(sort, operators.asc) else operators.asc

    def _list(self, _load_prop=None):
        if _load_prop:
            self._load_prop = _load_prop
        return list(self)

    def _first(self, _load_prop=None):
        lst = self._list(_load_prop)
        return lst[0] if len(lst) else None

    def _token(self):
        string = str(self._sort) + str(self._entity_cls) + str(self._limit)
        if self._rules:
            rules = copy(self._rules)
            rules.sort()
            for rule in rules:
                string += str(rule)
        return "%s:%s" % (str(self._entity_cls._type), hashlib.sha1(string.encode('UTF-8')).hexdigest())

    def __iter__(self):
        records = []
        cached_identifiers = self._cache.get(self._token()) if self._read_cache else None

        if cached_identifiers is None and not self._write_cache:
            records = self._fetch_proxy().fetchall()
        elif cached_identifiers is None:
            with make_lock('entity_query', 'cache_%s' % self._token()):
                cached_identifiers = self._cache.get(self._token(), allow_local=False) if self._read_cache else None
                if cached_identifiers is None:
                    records = self._fetch_proxy().fetchall()
                    self._cache.set(self._token(), [record._identifier for record in records], self._cache_time)

        if cached_identifiers and not records:
            records = Entity._by_identifier(cached_identifiers, return_dict=False, load_prop=self._load_prop)

        for record in records:
            yield record

    def _after(self, entity, reverse=False):
        ors = []

        for i in range(len(self._sort)):
            sort = self._sort[i]
            LOGGER.debug('sorti: ' + str(sort))
            if isinstance(sort, operators.desc):
                operator = operators.lt
            else:
                operator = operators.gt

            if reverse:
                operator = operators.gt if operator == operators.lt else operator == operators.lt

            ands = [operator(sort.col, sort.col, getattr(entity, sort.col))]

            for j in range(0, i):
                s = self._sort[j]
                ands.append(entity.c[s.col] == getattr(entity, s.col))
            ors.append(operators.and_(*ands))
        return self._filter(operators.or_(*ors))

    def _before(self, entity):
        return self._after(entity, True)

    def _count(self):
        return self._fetch_proxy().rowcount()


class Entities(Query):
    def __init__(self, cls, *rules, **kwargs):
        self._use_prop = False
        Query.__init__(self, cls, *rules, **kwargs)

    def _filter(self, *rules):
        for op in operators.op_iter(rules):
            if not op.lval_name.startswith('_'):
                self._use_prop = True
        self._rules += rules
        return self

    def _fetch_proxy(self):
        args = (self._entity_cls._type, self._sort, self._limit, self._rules)
        if self._use_prop:
            rp = backend.find_props(*args)
        else:
            rp = backend.find_entities(*args)

        callback = lambda rows: self._entity_cls._by_id(rows, self._load_prop, return_dict=False)

        return WrappedResultsProxy(rp, callback, True)


class Relations(Query):
    def __init__(self, cls, *rules, **kwargs):
        self._eager_load = kwargs.get('eager_load')
        self._load_entity_prop = kwargs.get('load_entity_data')
        Query.__init__(self, cls, *rules, **kwargs)

    def _filter(self, *rules):
        self._rules += rules
        return self

    def _set_eager_load(self, eager_load, load_entity_prop=False):
        self._eager_load = eager_load
        self._load_entity_prop = load_entity_prop

    def _make_relation(self, rows):
        relations = self._entity_cls._by_id(rows, self._load_prop, return_dict=False, ignore_missing=True)
        if relations and self._eager_load:
            for relation in relations:
                relation._eagerly_loaded_prop = True
            load_entities(relations, load_prop=self._load_entity_prop)
        return relations

    def _fetch_proxy(self):
        rp = backend.find_rels(self._entity_cls._type, sort=self._sort, limit=self._limit, constraints=self._rules)
        return WrappedResultsProxy(rp, self._make_relation, True)


class MultiFetchProxy(object):
    def __init__(self, *params):
        self._params = params
        self._fetch_proxy = None

    def fetchone(self):
        if not self._fetch_proxy:
            self._fetch_proxy = self._execute(*self._params)

        return self._fetch_proxy.next()

    def fetchall(self):
        if not self._fetch_proxy:
            self._fetch_proxy = self._execute(*self._params)

        return [record for record in self._fetch_proxy]

    def _execute(self, *params):
        raise NotImplementedError


class MergeFetchProxy(MultiFetchProxy):
    def _execute(self, fetch_proxies, sorts):
        def safe_next(fp):
            try:
                while True:
                    try:
                        return [fp, fp.fetchone(), False]
                    except NotFoundError:
                        pass
            except StopIteration:
                return fp, None, True

        def undone(pairs):
            return [p for p in pairs if not p[2]]

        pairs = undone(safe_next(fp) for fp in fetch_proxies)

        while pairs:
            if len(pairs) == 1:
                fp, item, done = pair = pairs[0]
                while not done:
                    yield item
                    fp, item, done = safe_next(fp)
                    pair[:] = fp, item, done
            else:
                yield_pair = pairs[0]
                for s in sorts:
                    column = s.col
                    max_fn = min if isinstance(s, operators.asc) else max

                    vals = [(getattr(i[1], column), i) for i in pairs]
                    max_pair = vals[0]
                    all_equal = True
                    for pair in vals[1:]:
                        if all_equal and pair[0] != max_pair[0]:
                            all_equal = False
                        max_pair = max_fn(max_pair, pair, key=lambda p: p[0])

                    if not all_equal:
                        yield_pair = max_pair[1]
                        break
                fp, item, done = yield_pair
                yield item
                yield_pair[:] = safe_next(fp)

            pairs = undone(pairs)
        raise StopIteration


class MultiQuery(Query):
    def __init__(self, queries, *rules, **kwargs):
        self._queries = queries
        Query.__init__(self, None, *rules, **kwargs)

    def _token(self):
        return ''.join(q._token() for q in self._queries)

    def _fetch_proxy(self):
        raise NotImplementedError

    def _reverse(self):
        for q in self._queries:
            q._reverse()

    def _set_load_prop(self, load_prop):
        for q in self._queries:
            q._load_prop = load_prop

    def _get_load_prop(self):
        return self._queries[0]._load_prop if self._queries else None

    _load_prop = property(_get_load_prop, _set_load_prop)

    def _set_sort(self, sorts):
        for q in self._queries:
            q._sort = deepcopy(sorts)

    def _get_sort(self):
        return self._queries[0]._sort if self._queries else None

    _sort = property(_get_sort, _set_sort)

    def _filter(self, *rules):
        for q in self._queries:
            q._filter(*rules)

    def _set_rules(self, rules):
        for q, rule in zip(self._queries, rules):
            q._rules = rule

    def _get_rules(self):
        return [q._rules for q in self._queries]

    _rules = property(_get_rules, _set_rules)

    def _set_limit(self, limit):
        for q in self._queries:
            q._limit = limit

    def _get_limit(self):
        return self._queries[0]._limit if self._queries else None

    _limit = property(_get_limit, _set_limit)


class Merge(MultiQuery):
    def _fetch_proxy(self):
        if (any(q._sort for q in self._queries) and
            not reduce(lambda x, y: (x == y) and x,
                       (q._sort for q in self._queries))):
            raise ValueError('The sorts in queries should be the same')
        return MergeFetchProxy((q._fetch_proxy() for q in self._queries),
                               self._sort)


def make_multi_relation_cls(name, *relations):
    tmp_rels = {}
    for rel in relations:
        entity1_cls, entity2_cls = rel._entity1_cls, rel._entity2_cls
        cls_name = '%s_%s_%s' % (name, entity1_cls._type, entity2_cls._type)
        cls = type(cls_name, tup(rel), {'__module__': entity1_cls.__module__})
        setattr(sys.modules[entity1_cls.__module__], cls_name, cls)
        tmp_rels[(entity1_cls, entity2_cls)] = cls

    class MultiRelation(object):
        c = operators.Slots()
        rels = tmp_rels

        def __init__(self, entity1, entity2, *args, **kwargs):
            r = self.rel(entity1, entity2)
            self.__class__ = r
            self.__init__(entity1, entity2, *args, **kwargs)

        @classmethod
        def rel(cls, entity1, entity2):
            entity1_cls = entity1 if isinstance(entity1, EntityMeta) else entity1.__class__
            entity2_cls = entity2 if isinstance(entity2, EntityMeta) else entity2.__class__
            return cls.rels[(entity1_cls, entity2_cls)]

        @classmethod
        def _query(cls, *rules, **kwargs):
            #TODO it should be possible to send the rules and kw to the merge constructor
            queries = [r._query(*rules, **kwargs) for r in cls.rels.values()]
            if 'sort' in kwargs:
                LOGGER.error('sorting MultiRelations is not supported')
            return Merge(queries)

        @classmethod
        def _fast_query(cls, sub, obj, label, load_prop=True, eager_load=True, load_entity_prop=False):
            def type_dict(items):
                types = {}
                for i in items:
                    types.setdefault(i.__class__, []).append(i)
                return types

            sub_dict = type_dict(tup(sub))
            obj_dict = type_dict(tup(obj))

            result = {}
            for types, rel in cls.rels.items():
                entity1_cls, entity2_cls = types
                if entity1_cls in sub_dict and entity2_cls in obj_dict:
                    result.update(rel._fast_query(sub_dict[entity1_cls], obj_dict[entity2_cls], label,
                                                  load_prop=load_prop, load_entity_prop=load_entity_prop,
                                                  eager_load=eager_load))
            return result
    return MultiRelation


def load_entities(relations, load_prop=False):
    ids1 = set()
    relation_cls = relations[0].__class__

    ids2 = ids1 if relation_cls._is_mobius() else set()
    for relation in relations:
        ids1.add(relation._entity1_id)
        ids2.add(relation._entity2_id)
    relation_cls._entity1_cls._by_id(ids1, load_prop=load_prop, read_only=True)
    if not relation_cls._is_mobius():
        relation_cls._entity2_cls._by_id(ids2, load_prop=load_prop, read_only=True)
