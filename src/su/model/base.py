__author__ = 'zhaolin.su'

from copy import deepcopy
from collections import OrderedDict

from su.g import backend, make_lock, cache, entity_cls_lookup
from su.db import operators
from su.util import alnum, tup, cache_retriever, explode
from su.model import renderer
from su.env import LOGGER


def start_transaction():
    backend.transactions.start()


def commit_transaction():
    backend.transactions.commit()


def rollback_transaction():
    backend.transactions.rollback()


class NotFoundError(Exception):
    pass


class SafeSetData:
    def __init__(self, cls):
        self.cls = cls

    def __enter__(self):
        self.cls.__safe__ = True

    def __exit__(self, type, value, tb):
        self.cls.__safe__ = False


class ModelBase(object):
    _body_attrs = ()
    _int_attrs = ()
    _int_props = ()
    _defaults = {}
    _essentials = ()
    c = operators.Slots()
    __safe__ = False
    _cache = cache
    _type = None  # will be initialized in metaclass
    _render_rules = ()

    @property
    def _relative_rules(self):
        return {}

    def __init__(self):
        safe_set_data = SafeSetData(self)
        with safe_set_data:
            self.safe_set_data = safe_set_data
            self._id = None
            self._changed_data = {}
            self._props = OrderedDict()
            self._created = False
            self._loaded = True
            self._loaded_relatives = OrderedDict()

    def __setstate__(self, state):
        # deepcopy() will call __setstate__ if it exists.
        # if we don't implement __setstate__ the check for existence will fail
        # in an atypical (and not properly handled) way because we override
        # __getattr__. the implementation provided here is identical to what
        # would happen in the default unimplemented case.
        self.__dict__ = state

    def __setattr__(self, key, value, record_changes=True):
        if key.startswith('__') or self.__safe__:
            object.__setattr__(self, key, value)
            return

        old_val = None
        if key.startswith('_'):
            #assume baseprops has the attr
            if record_changes and hasattr(self, key):
                old_val = getattr(self, key)
            object.__setattr__(self, key, value)
            if not key in self._body_attrs:
                return
        else:
            old_val = self._props.get(key, self._defaults.get(key))
            self._props[key] = value

        if record_changes and value != old_val:
            self._changed_data[key] = (old_val, value)

    def __getattr__(self, item):
        try:
            if item in self._relative_rules:
                if item not in self._loaded_relatives:
                    self.load_relatives(item)
                return self._loaded_relatives[item]
            elif hasattr(self, '_props'):
                data = self._props[item]
                return data
        except KeyError:
            try:
                return self._defaults[item]
            except KeyError:
                pass

        try:
            _id = object.__getattribute__(self, "_id")
        except AttributeError:
            _id = "???"

        try:
            cl = object.__getattribute__(self, "__class__").__name__
        except AttributeError:
            cl = "???"

        if self._loaded:
            nl = "it IS loaded"
        else:
            nl = "it is NOT loaded"

        # The %d format is nicer since it has no "L" at the
        # end, but if we can't do that, fall back on %r.
        try:
            id_str = "%d" % int(_id)
        except TypeError:
            id_str = "%r" % _id

        descr = '%s(%s).%s' % (cl, id_str, item)

        try:
            essentials = object.__getattribute__(self, "_essentials")
        except AttributeError:
            print("%s has no _essentials" % descr)
            essentials = ()

        deleted = False
        try:
            deleted = object.__getattribute__(self, "_deleted")

            if deleted:
                nl += " and IS deleted."
            else:
                nl += " and is NOT deleted."
        except AttributeError:
            nl += " and NO deleted attribute."

        if item in essentials and not deleted:
            LOGGER.warn("essentials-bandaid-reload: %s not found; %s Forcing reload." % (descr, nl))
            self._load()

            try:
                return self._props[item]
            except KeyError:
                LOGGER.error("essentials-bandaid-failed: Reload of %s didn't help. I recommend deletion." % descr)

        raise AttributeError('%s not found; %s' % (descr, nl))

    @property
    def _id36(self):
        return alnum(self._id)

    @classmethod
    def _make_identifier(cls, _id):
        return cls._type + '_' + alnum(_id)

    @property
    def _identifier(self):
        return self._make_identifier(self._id)

    @property
    def _is_changed(self):
        return bool(len(self._changed_data))

    @property
    def data(self):
        if not self._loaded:
            self._load()
        data = OrderedDict({'id': self._id})
        for attr in self._body_attrs:
            data[attr[1:]] = getattr(self, attr)
        data.update(sorted(self._defaults.items()))
        data.update(self._props)
        data.update(self._loaded_relatives)
        return data

    @classmethod
    def _by_id(cls, ids, load_prop=False, return_dict=True, extra_props=None,
               ignore_missing=False, ignore_cache=False, read_only=False):
        ids, is_single = tup(ids, True)

        if not all(x <= backend.MAX_ID for x in ids):
            raise NotFoundError('big id in %s' % ids)

        def count_found(result, missed):
            cls._cache.stats.cache_report(hits=len(result), misses=len(missed),
                                          cache_name='cache_retriever.%s' % cls._type)

        if not cls._cache.stats:
            count_found = None

        def get_body_from_db(body_ids):
            bodies = cls._get_body(cls._type, body_ids)
            for body_id in bodies.keys():
                bodies[body_id] = cls._construct(body_id, bodies[body_id])._self_only()

            return bodies

        if not ignore_cache:
            records = cache_retriever(cls._cache, ids,
                                      miss_fn=get_body_from_db, prefix=(cls._type+':'), found_fn=count_found)
        else:
            records = get_body_from_db(ids)

        missing = []
        for i in ids:
            if i not in records:
                missing.append(i)
            elif records[i] and records[i]._id != i:
                LOGGER.error('wrong record found in cache: expected %s, got %s' % (i, records[i]._id))
                records[i] = get_body_from_db([i]).values()[0]
                records[i]._cache_self()

        if missing and not ignore_missing:
            raise NotFoundError("%s %s" % (cls._type, missing))

        for i in missing:
            LOGGER.warning('rel missing: %s' % i)
            ids.remove(i)

        if load_prop:
            needs = []
            for record in records.values():
                if not record._loaded:
                    needs.append(record)
            if needs:
                cls._load_multi(needs)

        if not read_only:
            records = deepcopy(records)

        if extra_props:
            for entity_id, props in extra_props.items():
                for k, v in props.items():
                    records[entity_id].__setattr__(k, v, False)

        if is_single:
            return records[ids[0]] if ids else None
        elif return_dict:
            return records
        else:
            return [records.get(i) for i in ids]

    @classmethod
    def _by_id36(cls, id36s, return_dict=True, **kwargs):
        id36s, is_single = tup(id36s, True)

        ids = [int(i, 36) for i in id36s]

        records = cls._by_id(ids, return_dict=True, **kwargs)
        records = {record._id36: record for record in records.values()}

        if is_single:
            values = list(records.values())
            return values[0]
        elif return_dict:
            return records
        else:
            return [record for record in records.values()]

    @classmethod
    def _by_identifier(cls, identifiers, return_dict=True, ignore_missing=False, **kwargs):
        identifiers, is_single = tup(identifiers, True)

        tables = {}
        lookup = {}
        for identifier in identifiers:
            try:
                entity_type, entity_id = identifier.split('_')
                check = entity_cls_lookup[entity_type]
                entity_id = int(entity_id, 36)
                lookup[identifier] = (entity_type, entity_id)
                tables.setdefault(entity_type, []).append(entity_id)
            except (KeyError, ValueError) as e:
                #if is_single:
                raise NotFoundError

        records = {}
        for entity_type, ids in tables.items():
            entity_cls = entity_cls_lookup[entity_type]
            records[entity_type] = entity_cls._by_id(ids, ignore_missing=ignore_missing, **kwargs)

        results = []
        for identifier in identifiers:
            if identifier in lookup:
                entity_type, entity_id = lookup[identifier]
                record = records.get(entity_type, {}).get(entity_id)
                if not record and ignore_missing:
                    continue
                results.append((identifier, record))

        if is_single:
            return results[0][1] if results else None
        elif return_dict:
            return dict(results)
        else:
            return [record for identifier, record in results]

    @classmethod
    def _load_multi(cls, entities):
        entities = tup(entities)
        entity_ids = [e._id for e in entities]
        props = cls._get_prop(cls._type, entity_ids)
        try:
            # to avoid __getattr__()
            essentials = object.__getattribute__(cls, "_essentials")
        except AttributeError:
            essentials = ()

        to_set = {}
        for entity in entities:
            entity._props.update(sorted(props.get(entity._id, entity._props).items()))
            entity._loaded = True

            for prop in essentials:
                if prop not in entity._props:
                    print("warning %s is missing %s" % (entity._identifier, prop))
            entity._asked_for_prop = True
            to_set[entity._cache_key()] = entity._self_only()

        cls._cache.set_multi(to_set)

    def _cache_key(self):
        return self._type + ':' + (str(self._id) if self._id else '')

    def _remote_self(self):
        result = self._cache.get(self._cache_key(), allow_local=False)
        if result and result._id != self._id:
            LOGGER.warn('invalid_cache: base.py: Doppleganger on read: got %s for %s', (result, self))
            self._cache.delete(self._cache_key())
            return
        return result

    def _self_only(self):
        if not self._loaded:
            self._load()
        c = deepcopy(self)
        c._loaded_relatives = OrderedDict()
        return c

    def _cache_self(self):
        self._cache.set(self._cache_key(), self._self_only())

    def _sync_latest(self):
        remote_self = self._remote_self()
        if not remote_self:
            return self._is_changed

        for attr in self._body_attrs:
            self.__setattr__(attr, getattr(remote_self, attr), False)

        if remote_self._loaded:
            self._props = remote_self._props

        old_changed_data = self._changed_data
        self._changed_data = {}
        for k, (old_val, new_val) in old_changed_data.items():
            setattr(self, k, new_val)

        return self._is_changed

    @classmethod
    def _update_by_id(cls, _id, updates):
        lock = None
        try:
            lock = make_lock(cls._type + '_commit', 'commit_' + cls._make_identifier(_id))
            lock.acquire()
            start_transaction()

            props = {}
            body_attrs = {}
            for k, v in updates.items():
                if k.startswith('_'):
                    body_attrs[k[1:]] = v
                else:
                    props[k] = v

            if props:
                cls._update_prop(cls._type, _id, **props)

            if body_attrs:
                cls._update_body(cls._type, _id, **body_attrs)
        except:
            rollback_transaction()
            raise
        else:
            commit_transaction()
        finally:
            if lock:
                lock.release()

    def _commit(self, keys=None):
        lock = None

        try:
            is_new = False
            if not self._created:
                start_transaction()
                self._create()
                is_new = True

            lock = make_lock(self._type + '_commit', 'commit_' + self._identifier)
            lock.acquire()

            if not is_new and not self._sync_latest():
                self._cache_self()
                return

            start_transaction()

            to_set = self._changed_data.copy()
            if keys:
                keys = tup(keys)
                for key in to_set.keys():
                    if key not in keys:
                        del to_set[key]

            props = {}
            body_attrs = {}
            for k, (old_value, new_value) in to_set.items():
                if k.startswith('_'):
                    body_attrs[k[1:]] = new_value
                else:
                    props[k] = new_value

            if props:
                if is_new:
                    self._insert_prop(self._type, self._id, **props)
                else:
                    self._update_prop(self._type, self._id, **props)

            if body_attrs:
                self._update_body(self._type, self._id, **body_attrs)

            if keys:
                for k in keys:
                    if k in self._changed_data.keys():
                        del self._changed_data[k]
            else:
                self._changed_data.clear()

            self._cache_self()
        except:
            rollback_transaction()
            raise
        else:
            commit_transaction()
        finally:
            if lock:
                lock.release()

    @classmethod
    def _query(cls, *args, **kwargs):
        raise NotImplementedError()

    def _load(self):
        self._load_multi(self)

    def _safe_load(self):
        if not self._loaded:
            self._load()

    def _incr(self, prop, offset=1):
        if self._is_changed:
            raise ValueError('cannot increase changed data')

        if prop not in self._int_attrs:
            if prop in self._int_props:
                if not self._loaded:
                    self._load()
            else:
                msg = "cannot increase none-integer property %s" % property
                raise ValueError(msg)

        with make_lock(self._type + '_commit', 'commit_' + self._identifier):
            self._sync_latest()
            old_val = getattr(self, prop)
            if prop in self._defaults and self._defaults[prop] == old_val:
                setattr(self, prop, old_val + offset)
                self._commit(prop)
            else:
                self.__setattr__(prop, old_val + offset, False)
                if prop.startswith('_'):
                    self._incr_attr(self._type, self._id, prop[1:], offset)
                else:
                    self._incr_prop(self._type, self._id, prop, offset)
            self._cache_self()

    def _parse_render_rules(self, scenarios, roles):
        rules = self._render_rules  # for options.pop
        scenarios = set(tup(scenarios)) if scenarios else set()
        roles = set(tup(roles)) if roles else set()
        parsed_rules = []
        for rule in rules:
            if len(rule) < 2:
                raise AssertionError('invalid render rules in %s' + self.__class__.__name__)
            options = rule[2] if len(rule) >= 3 else {}

            on = set(tup(options.get('on', set())))
            except_on = set(tup(options.get('except_on', set())))
            allowed_roles = set(tup(options.get('roles', set())))
            except_roles = set(tup(options.get('except_roles', set())))

            if on and not scenarios.intersection(on):
                continue
            if except_on and scenarios.intersection(except_on):
                continue
            if allowed_roles and not roles.intersection(allowed_roles):
                continue
            if except_roles and roles.intersection(except_roles):
                continue

            keys = explode(rule[0])
            if hasattr(renderer, rule[1]):
                render_fn = getattr(renderer, rule[1])
            elif hasattr(self, rule[1]):
                render_fn = getattr(self, rule[1])
            else:
                raise AssertionError('render function %s not found in %s' % (rule[1], self.__class__.__name__))

            if '*' in keys:
                keys.remove('*')
                keys.extend(self.data.keys())
            else:
                if '*body*' in keys:
                    keys.remove('*body*')
                    keys.extend([attr[1:] for attr in self._body_attrs])
                if '*prop*' in keys:
                    keys.remove('*prop*')
                    keys.extend(self._props.keys())
                if '*relatives*' in keys:
                    keys.remove('*relatives*')
                    keys.extend(self._loaded_relatives.keys())
            for key in keys:
                parsed_rules.append((key, render_fn, options))
        return parsed_rules

    def render(self, scenarios=None, roles=None):
        render_rules = self._parse_render_rules(scenarios, roles)
        data = self.data
        for rule in render_rules:
            key, render_fn, options = rule
            data = renderer.render(data, key, render_fn, roles=roles, **options)
        return data

    @classmethod
    def render_multi(cls, entities, scenarios=None, roles=None):
        return [entity.render(scenarios, roles) for entity in entities]

    def load_relatives(self, relatives=None):
        self.load_relatives_multi([self], relatives=relatives)

    @classmethod
    def load_relatives_multi(cls, entities, relatives=None):
        if not entities or relatives == []:
            return
        if relatives is None:
            relatives = OrderedDict()
            # todo: define order here?
            for name, rule in entities[0]._relative_rules.items():
                if len(rule) > 3 and 'relatives' in rule[3]:
                    relatives[name] = rule[3]['relatives']
                else:
                    relatives[name] = []
        if not all(entity.__class__ == entities[0].__class__ for entity in entities):
            raise AssertionError("Different types of relatives detected: %s" % entities)

        if type(relatives) is str:  # from __getattr__
            relatives = OrderedDict({relatives: []})
        elif type(relatives) is list:
            relatives = OrderedDict({key: [] for key in sorted(relatives)})
        elif type(relatives) is dict:
            relatives = OrderedDict(relatives)
        elif type(relatives) is not OrderedDict:
            raise AssertionError('invalid relatives definition: %s' % str(relatives))
        
        for name, sub_relatives in relatives.items():
            if not name:
                continue
            if name not in entities[0]._relative_rules:
                raise AssertionError("Relative '%s' not defined in [%s]" % (name, entities[0]._type))
            relative_cls = entities[0]._relative_rules[name][0]
            relatives = [relative_cls(entity, name) for entity in entities]
            relative_cls.fetch_multi(relatives, child_relatives=sub_relatives)
            for i, entity in enumerate(entities):
                entity._loaded_relatives[name] = relatives[i]

    @classmethod
    def _get_body(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _get_prop(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _insert_prop(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _update_prop(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _update_body(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _incr_attr(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _incr_prop(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _insert_body(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _construct(cls, *args, **kwargs):
        raise NotImplementedError

    def _create(self):
        attrs = {}
        for attr in self._body_attrs:
            attrs[attr[1:]] = getattr(self, attr)
        self._id = self._insert_body(self._type, **attrs)
        self._created = True
