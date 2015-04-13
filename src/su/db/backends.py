__author__ = 'zhaolin.su'
import threading
import random
import pickle
import time
import binascii
import sqlalchemy
from copy import deepcopy
from su.db import operators
from su.db.exceptions import InvalidDataError, InsertDuplicateError, DBUnavailableError
from su.util import simple_traceback, iters, Storage, tup, explode
from su.env import LOGGER


def value_py2db(val, return_kind=False):
    if isinstance(val, bool):
        val = 't' if val else 'f'
        kind = 'bool'
    elif isinstance(val, str):
        kind = 'str'
    elif isinstance(val, (int, float)):
        kind = 'num'
    elif val is None:
        kind = 'none'
    else:
        kind = 'pickle'
        val = pickle.dumps(val)

    if return_kind:
        return val, kind
    else:
        return val


def value_db2py(val, kind):
    if kind == 'bool':
        val = True if val is 't' else False
    elif kind == 'num':
        try:
            val = int(val)
        except ValueError:
            val = float(val)
    elif kind == 'none':
        val = None
    elif kind == 'pickle':
        if val.startswith('\\x'):
            val = val[2:]
        try:
            val = pickle.loads(binascii.unhexlify(val))
        except (binascii.Error, pickle.UnpicklingError):
            raise InvalidDataError(val)

    return val


def translate_sort(table, column_name, lval=None, rewrite_name=True):
    if isinstance(lval, operators.query_func):
        fn_name = lval.__class__.__name__
        sa_func = getattr(sqlalchemy.func, fn_name)
        return sa_func(translate_sort(table,
                                      column_name,
                                      lval.lval,
                                      rewrite_name))

    if rewrite_name:
        if column_name == 'id':
            return table.c.entity_id
        elif column_name == 'hot':
            return sqlalchemy.func.hot(table.c.ups, table.c.downs, table.c.date)
        elif column_name == 'score':
            return sqlalchemy.func.score(table.c.ups, table.c.downs)
        elif column_name == 'controversy':
            return sqlalchemy.func.controversy(table.c.ups, table.c.downs)
        elif column_name == 'count':
            return sqlalchemy.func.count(table.c.entity_id)
    # else
    return table.c[column_name]


def translate_body_value(rval):
    if isinstance(rval, operators.timeago):
        return sqlalchemy.text("current_timestamp - interval '%s'" % rval.interval)
    else:
        return rval

max_val_len = 1000


def translate_prop_value(alias, op):
    lval = op.lval
    need_substr = False if isinstance(lval, operators.query_func) else True
    lval = translate_sort(alias, 'value', lval, False)

    if isinstance(op, (operators.lt, operators.lte, operators.gt, operators.gte)):
        # cast value to float for comparison
        lval = sqlalchemy.cast(lval, sqlalchemy.Float)
    elif need_substr:
        # add the substring func
        lval = sqlalchemy.func.substring(lval, 1, max_val_len)

    op.lval = lval

    #convert the rval to db types
    #convert everything to strings for pg8.3
    op.rval = tuple(str(value_py2db(v)) for v in tup(op.rval))


class SimpleTransactionManager(threading.local):
    _max_tries = 30

    def __init__(self):
        self._tries = 0
        self.engines = set()
        self.active = False
        threading.local.__init__(self)

    def start(self):
        """Indicate that a transaction has begun."""
        self.active = True

    def add_engine(self, engine):
        """Add a database connection to the meta-transaction if active."""
        if not self.active:
            return

        try:
            if engine not in self.engines:
                engine.begin()
                self.engines.add(engine)
                self._tries = 0
        except sqlalchemy.exc.OperationalError as e:
            if self._tries < self._max_tries:
                self._tries += 1
                LOGGER.warn('sqlalchemy queue temporary unavailable, retrying... %s' % self._tries)
                time.sleep(0.1)
                self.add_engine(engine)
            else:
                self._tries = 0
                raise e

    def commit(self):
        """Commit the meta-transaction."""
        try:
            for engine in self.engines:
                engine.commit()
        finally:
            self._clear()

    def rollback(self):
        """Roll back the meta-transaction."""
        try:
            for engine in self.engines:
                engine.rollback()
        finally:
            LOGGER.warn('transaction rollback')
            self._clear()

    def _clear(self):
        self.engines.clear()
        self.active = False


class DBCluster:
    def __init__(self):
        self.masters = {}
        self.slaves = {}

    def add_master(self, name, engine):
        self.masters[name] = engine

    def add_slave(self, name, engine):
        self.slaves[name] = engine

    def get_engines(self):
        return dict(self.masters, **self.slaves)


class WrappedResultsProxy():
    def __init__(self, sqlalchemy_results_proxy, filter_fn, do_batch=False):
        self.rp = sqlalchemy_results_proxy
        self.filter = filter_fn
        self.do_batch = do_batch

    @property
    def rowcount(self):
        return self.rp.rowcount

    def _fetch(self, res):
        if self.do_batch:
            return self.filter(res)
        else:
            return [self.filter(row) for row in res]

    def fetchall(self):
        return self._fetch(self.rp.fetchall())

    def fetchmany(self, n):
        rows = self._fetch(self.rp.fetchmany(n))
        if rows:
            return rows
        else:
            raise StopIteration

    def fetchone(self):
        row = self.rp.fetchone()
        if row:
            if self.do_batch:
                row = tup(row)
                return self.filter(row)[0]
            else:
                return self.filter(row)
        else:
            raise StopIteration


class BackendBase:
    def __init__(self, config, reset_tables=False):
        self.transactions = SimpleTransactionManager()
        self._config = None
        self._engines = {}
        self._clusters = {}
        self._unavailable = []
        self._tables = {}
        self.parse_config(config, reset_tables)

    def parse_config(self, config, reset_tables=False):
        self._config = config
        if 'engines' in self._config:
            self._engines = self._get_engines_from_config(config['engines'])
        if 'clusters' in self._config:
            self._clusters = self._get_clusters_from_config(config['clusters'], self._engines)
        self.create_tables(reset_tables)

    def create_tables(self, reset_tables=False):
        table_definitions = self._parse_tables_config(self._config['tables'], self._config.get('base_tables', {}))
        tables = {}
        for name, definition in table_definitions.items():
            cluster_name = definition['cluster']
            cluster = self._clusters[cluster_name]
            tables[name] = []
            for engine_name, engine in cluster.get_engines().items():
                # making columns
                columns = []
                for (args, kws) in definition['columns']:
                    columns.append(sqlalchemy.Column(*args, **kws))

                # making constraint
                uniqueconstraints = []
                for uc_definition in definition['uniqueconstraints']:
                    uc = explode(uc_definition)  # [s.strip() for s in uc_definition.split(',')]
                    uniqueconstraints.append(sqlalchemy.UniqueConstraint(*uc))

                # making table
                table_args = columns + uniqueconstraints
                table = self._make_table(name, engine, *table_args)
                table.cluster = cluster_name
                table.engine = engine_name
                table.is_master = True if engine in cluster.masters.values() else False

                # making index
                index_commands = []
                for index in definition['indexes']:
                    index_commands.append(self._index_str(name, **index))

                # release schema on the engine
                if reset_tables and table.bind.has_table(table.name):
                    table.drop()
                    # table.bind.execute('DROP TABLE IF EXISTS ' + table.name)
                self._create_table(table, index_commands)
                tables[name].append(table)
        self._tables = tables
        return tables

    def get_engine(self, name):
        return self._engines[name]

    def get_engines(self, names):
        engines = [self._engines[name] for name in names]
        return engines

    def get_table_with_engine(self, name, engine):
        for table in self._tables[name]:
            if table.engine == engine:
                return table
        raise EnvironmentError('No such table: name: %s, engine: %s' % (name, engine))

    @classmethod
    def _parse_tables_config(cls, tables_config, bases_config=None):
        bases = {}
        definitions = {}
        if bases_config:
            for name, definition in bases_config.items():
                bases[name] = cls._parse_table_config(bases_config[name])
            for name, definition in tables_config.items():
                definitions[name] = cls._parse_table_config(tables_config[name])
                if 'inherit' in definition:
                    definitions[name] = cls._merge_table_config(bases[definition['inherit']], definitions[name])
        return definitions

    @classmethod
    def _get_engines_from_config(cls, config, prefix='sqlalchemy.'):
        engines = {}
        for engine_name, engine_config in config.items():
            engines[engine_name] = sqlalchemy.engine_from_config(engine_config, prefix)

        return engines

    @classmethod
    def _get_clusters_from_config(cls, config, engines):
        clusters = {}
        for name, cluster_config in config.items():
            cluster = DBCluster()
            if 'masters' in cluster_config:
                for master_name in cluster_config['masters']:
                    cluster.add_master(master_name, engines[master_name])
            if 'slaves' in cluster_config:
                for slave_name in cluster_config['slaves']:
                    cluster.add_slave(slave_name, engines[slave_name])
            #if 'avoid_master_read' in cluster_config:
            #    cluster.avoid_master_read = cluster_config['avoid_master_read']
            clusters[name] = cluster
        return clusters

    @classmethod
    def _create_table(cls, table, prop_commands=None):
        t = table
        if not t.bind.has_table(t.name):
            t.create(checkfirst=False)
            if prop_commands:
                for i in prop_commands:
                    t.bind.execute(i)

    @classmethod
    def _index_str(cls, table, name, columns, where=None, unique=False):
        if unique:
            index_str = 'CREATE UNIQUE INDEX'
        else:
            index_str = 'CREATE INDEX'
        index_str += ' idx_%s_' % table
        index_str += name
        index_str += ' ON ' + table + ' (%s)' % columns
        if where:
            index_str += ' WHERE %s' % where
        return index_str

    @classmethod
    def _make_table(cls, name, engine, *args, **kwargs):
        metadata = sqlalchemy.MetaData(engine)
        metadata.bind.echo = False
        table = sqlalchemy.Table(name, metadata, *args, **kwargs)
        return table

    @classmethod
    def _parse_table_config(cls, config):
        table_config = config
        safe_kw = ('primary_key', 'nullable', 'default')
        result = {'columns': [], 'indexes': [], 'engines': [], 'uniqueconstraints': []}
        if 'columns' in table_config:
            for c in table_config['columns']:
                kws = {}
                for safe_key in safe_kw:
                    if safe_key in c.keys():
                        value = c[safe_key]
                        if safe_key == 'default' and str(value).startswith('func.'):
                            value = eval('sqlalchemy.' + value)
                        kws[safe_key] = value

                column_type = eval('sqlalchemy.' + c['type'])
                if column_type is None:
                    raise EnvironmentError('Invalid type %s defined in config file.' % c['type'])
                args = [c['name'], column_type]
                result['columns'].append((args, kws))
        if 'indexes' in table_config:
            result['indexes'] = table_config['indexes']

        if 'uniqueconstraints' in table_config:
            result['uniqueconstraints'] = table_config['uniqueconstraints']

        if 'cluster' in table_config:
            result['cluster'] = table_config['cluster']

        return result

    @classmethod
    def _merge_table_config(cls, config1, config2):
        merge_keys = list(set(list(config1.keys()) + list(config2.keys())))
        result = {}
        for key in merge_keys:
            config1_value = config1[key] if key in config1 else None
            config2_value = config2[key] if key in config2 else None
            if isinstance(config1_value, list) or isinstance(config2_value, list):
                result[key] = config1_value + config2_value
            else:
                result[key] = config2_value if config2_value is not None else config1_value
        return result


class KVSEntityTable:
    def __init__(self, body_table, prop_table):
        self.body_table = body_table
        self.prop_table = prop_table

        engine = body_table.engine
        if prop_table.engine != engine:
            raise EnvironmentError('Tables with different engines found for entity %s.' % self)
        self.engine = body_table.engine
        self.is_master = body_table.is_master

    def __repr__(self):
        mark = 'M' if self.is_master else 'S'
        return '<KVSEntityTable: %s %s@%s, %s@%s>' % (mark, self.body_table.name, self.body_table.engine,
                                                      self.prop_table.name, self.prop_table.engine)


class KVSRelationTable:
    def __init__(self, body_table, prop_table, entity1_table, entity2_table):
        self.body_table = body_table
        self.prop_table = prop_table
        self.entity1_table = entity1_table
        self.entity2_table = entity2_table

        engine = body_table.engine
        if prop_table.engine != engine or entity1_table.engine != engine or entity2_table.engine != engine:
            raise EnvironmentError('Tables with different engines found for relation %s.' % self)
        self.engine = body_table.engine
        self.is_master = body_table.is_master

    def __repr__(self):
        mark = 'M' if self.is_master else 'S'
        return '<KVSRelationTable: %s %s@%s, %s@%s>' % (mark, self.body_table.name, self.body_table.engine,
                                                        self.prop_table.name, self.prop_table.engine)


class KVSEntity(object):
    def __init__(self, name, tables=None, avoid_master_read=False):
        self.name = name
        self.tables = tables if tables else []
        self.avoid_master_read = avoid_master_read

    def __repr__(self):
        return '<KVSEntity: %s, [%s]>' % (self.name, ','.join(str(table) for table in self.tables))

    def add_table(self, kvs_table):
        self.tables.append(kvs_table)

    # todo: test master-slave availability
    def get_write_tables(self):
        tables = []
        for table in self.tables:
            if table.is_master:
                tables.append(table)
        return tables

    def get_read_tables(self):
        tables = []
        for table in self.tables:
            if not table.is_master or not self.avoid_master_read:
                tables.append(table)
        return tables


class KVSRelation(KVSEntity):
    def __init__(self, name, entity_left, entity_right, tables=None, avoid_master_read=False):
        self.entity_left = entity_left
        self.entity_right = entity_right
        KVSEntity.__init__(self, name, tables, avoid_master_read)

    def __str__(self):
        return '<KVSRelation: %s, [%s]>' % (self.name, ','.join(str(table) for table in self.tables))


class KVSBackend(BackendBase):
    MAX_ID = 9223372036854775807

    def __init__(self, config, reset_tables=False):
        self._kvs_entities = {}
        self._kvs_relations = {}
        BackendBase.__init__(self, config, reset_tables)

    def parse_config(self, config, reset_tables=False):
        BackendBase.parse_config(self, config, reset_tables)
        self._kvs_entities = self._get_kvs_entities_from_config(config['entities']) if 'entities' in config else {}
        self._kvs_relations = self._get_kvs_relations_from_config(config['relations']) if 'relations' in config else {}

    def get_kvs_entity(self, name):
        return self._kvs_entities[name]

    def get_kvs_relation(self, name):
        return self._kvs_relations[name]

    #TODO check attr exists
    def get_table(self, name, body_type='entity', table_type='body', write=False):
        method = getattr(self, 'get_kvs_%s' % body_type)
        kvs_model = method(name)
        if write:
            tables = kvs_model.get_write_tables()
        else:
            tables = kvs_model.get_read_tables()

        for table in tables:
            if table.engine in self._unavailable:
                tables.remove(table)

        if len(tables) == 0:
            raise DBUnavailableError('No db connection available.')
        if len(tables) == 1:
            chosen = tables[0]
        else:
            chosen = random.choice(list(tables))
        return getattr(chosen, '%s_table' % table_type)

    def get_body(self, body_type, body_pk, name, body_id):
        table = self.get_table(name, body_type, 'body')
        r, single = self._fetch_query_from_table(table, table.c[body_pk], body_id)

        #if single, only return one storage, otherwise make a dict
        res = {} if not single else None
        for row in r:
            storage = Storage((k, row[k]) for k in row.keys() if k != 'entity_id')
            if single:
                res = storage
                # check that we got what we asked for
                if row[body_pk] != body_id:
                    raise AttributeError(("Unexpected attributes: got %s, wanted %s" % (row[body_pk], body_id)))
            else:
                res[row[body_pk]] = storage
        return res

    def get_entity_body(self, name, entity_id):
        return self.get_body('entity', 'entity_id', name, entity_id)

    def get_relation_body(self, name, rel_id):
        return self.get_body('relation', 'rel_id', name, rel_id)

    def insert_entity_body(self, name, entity_id=None, **attrs):
        body_table = self.get_table(name, 'entity', 'body', write=True)
        if entity_id:
            attrs['entity_id'] = entity_id

        def do_insert(t):
            self.transactions.add_engine(t.bind)
            result_proxy = t.insert().execute(**attrs)
            new_id = result_proxy.inserted_primary_key[0]
            last_inserted_params = result_proxy.last_inserted_params()
            for k, v in attrs.items():
                if last_inserted_params[k] != v:
                    raise AttributeError(("Unexpected attributes: expected %s, got %s" %
                                          (attrs,  last_inserted_params)))
            return new_id

        try:
            new_pk = do_insert(body_table)
            attrs['entity_id'] = new_pk
            return new_pk
        except sqlalchemy.exc.DBAPIError as e:
            if not 'IntegrityError' in str(e):
                raise
            # wrap the error to prevent db layer bleeding out
            raise InsertDuplicateError("Entity exists (%s)" % str(attrs))

    def update_entity_body(self, name, entity_id, **attrs):
        body_table = self.get_table(name, 'entity', 'body', write=True)

        if not attrs:
            return

        #use real columns
        def do_update(t):
            self.transactions.add_engine(t.bind)
            new_props = dict((t.columns[prop], val) for prop, val in attrs.items())
            u = t.update(t.c.entity_id == entity_id, values=new_props)
            u.execute()
        do_update(body_table)

    def incr_entity_body_attr(self, name, entity_id, attr, offset):
        body_table = self.get_table(name, 'entity', 'body', write=True)

        def do_update(t):
            self.transactions.add_engine(t.bind)
            u = t.update(t.c.entity_id == entity_id,
                         values={t.c[attr]: t.c[attr] + offset})
            u.execute()
        do_update(body_table)

    def insert_relation_body(self, name, entity1_id, entity2_id, **attrs):
        body_table = self.get_table(name, 'relation', 'body', write=True)

        try:
            result_proxy = body_table.insert().execute(entity1_id=entity1_id,
                                                       entity2_id=entity2_id,
                                                       **attrs)

            return result_proxy.inserted_primary_key[0]
        except sqlalchemy.exc.DBAPIError as e:
            if not 'IntegrityError' in str(e):
                raise
            # wrap the error to prevent db layer bleeding out
            raise InsertDuplicateError("Relation exists (%s, %s, %s)" % (name, entity1_id, entity2_id))

    def update_relation_body(self, name, rel_id, **attrs):
        body_table = self.get_table(name, 'relation', 'body', write=True)

        if not attrs:
            return

        def do_update(t):
            self.transactions.add_engine(t.bind)
            new_props = dict((t.c[prop], val) for prop, val in attrs.items())
            t.update(t.c.rel_id == rel_id, values=new_props).execute()
        do_update(body_table)

    def delete_rel(self, name, rel_id):
        body_table = self.get_table(name, 'relation', 'body', write=True)
        prop_table = self.get_table(name, 'relation', 'prop', write=True)

        self.transactions.add_engine(body_table.bind)
        self.transactions.add_engine(prop_table.bind)

        body_table.delete(body_table.c.rel_id == rel_id).execute()
        prop_table.delete(prop_table.c.body_id == rel_id).execute()

    def insert_prop(self, body_type, _name, body_id, **props):
        table = self.get_table(_name, body_type, 'prop', write=True)
        self.transactions.add_engine(table.bind)

        inserts = []
        for key, val in props.items():
            val, kind = value_py2db(val, return_kind=True)
            inserts.append(dict(key=key, value=val, kind=kind))

        if inserts:
            i = table.insert(values=dict(body_id=body_id))
            i.execute(*inserts)

    def update_prop(self, body_type, _name, body_id, **props):
        table = self.get_table(_name, body_type, 'prop', write=True)
        self.transactions.add_engine(table.bind)

        command = table.update(sqlalchemy.and_(table.c.body_id == body_id,
                                               table.c.key == sqlalchemy.bindparam('_key')))

        inserts = []
        for key, val in props.items():
            val, kind = value_py2db(val, return_kind=True)

            result_proxy = command.execute(_key=key, value=val, kind=kind)
            if not result_proxy.rowcount:
                inserts.append({'key': key, 'value': val, 'kind': kind})

        if inserts:
            i = table.insert(values=dict(body_id=body_id))
            i.execute(*inserts)

    def incr_prop(self, body_type, _name, body_id, prop, offset):
        t = self.get_table(_name, body_type, 'prop', write=True)
        self.transactions.add_engine(t.bind)
        u = t.update(sqlalchemy.and_(t.c.body_id == body_id, t.c.key == prop),
                     values={t.c.value: sqlalchemy.cast(t.c.value, sqlalchemy.Float) + offset})
        u.execute()

    def get_prop(self, body_type, _name, body_id, prop=None):
        table = self.get_table(_name, body_type, 'prop')
        whereclause = table.c.key.op('=')(prop) if isinstance(prop, str) else None
        r, single = self._fetch_query_from_table(table, table.c.body_id, body_id, where=whereclause)

        #if single, only return one storage, otherwise make a dict
        res = Storage() if single else {}
        for row in r:
            val = value_db2py(row.value, row.kind)
            storage = res if single else res.setdefault(row.body_id, Storage())
            if single and row.body_id != body_id:
                raise AttributeError(("Unexpected attributes: got %s, wanted %s" % (row.body_id, body_id)))
            storage[row.key] = val

        return res

    def insert_entity_prop(self, _name, entity_id, **props):
        self.insert_prop('entity', _name, entity_id, **props)

    def insert_relation_prop(self, _name, rel_id, **props):
        self.insert_prop('relation', _name, rel_id, **props)

    def update_entity_prop(self, _name, entity_id, **props):
        self.update_prop('entity', _name, entity_id, **props)

    def update_relation_prop(self, _name, rel_id, **props):
        self.update_prop('relation', _name, rel_id, **props)

    def incr_entity_prop(self, _name, body_id, prop, offset):
        self.incr_prop('entity', _name, body_id, prop, offset)

    def incr_relation_prop(self, _name, body_id, prop, offset):
        self.incr_prop('relation', _name, body_id, prop, offset)

    def get_entity_prop(self, _name, entity_id, prop=None):
        return self.get_prop('entity', _name, entity_id, prop)

    def get_relation_prop(self, _name, entity_id, prop=None):
        return self.get_prop('relation', _name, entity_id, prop)

    @classmethod
    def add_request_info(cls, select):
        def sanitize(txt):
            return "".join(x if x.isalnum() else "."
                           for x in txt)
        from pyramid import request
        tb = simple_traceback(limit=12)

        if (hasattr(request, 'path') and
                hasattr(request, 'client_addr') and
                hasattr(request, 'user_agent')):
            comment = '/*\n%s\n%s\n%s\n*/' % (
                tb or "",
                sanitize(request.path),
                sanitize(request.client_addr))
            return select.prefix_with(comment)

        return select

    @classmethod
    def _fetch_query_from_table(cls, table, column, body_id, selects=None, where=None):
        """pull the columns from the thing/data tables for a list or single
        body_id"""
        single = False

        if not isinstance(body_id, iters):
            single = True
            body_id = (body_id,)

        if not selects:
            select_columns = [table]
        else:
            select_columns = []
            if isinstance(selects, str):
                select_columns.append(table.c[selects])
            elif isinstance(selects, list):
                for select in selects:
                    select_columns.append(table.c[select])
            else:
                select_columns = [table]

        whereclause = sqlalchemy.and_(column.in_(body_id), where) \
            if isinstance(where, sqlalchemy.sql.expression.ClauseElement) \
            else column.in_(body_id)
        s = sqlalchemy.select(columns=select_columns, whereclause=whereclause)

        try:
            r = cls.add_request_info(s).execute().fetchall()
        except Exception as e:
            # dbm.mark_dead(table.bind)
            # this thread must die so that others may live
            raise
        return r, single

    @classmethod
    def sa_op(cls, op):
        #if BooleanOp
        if isinstance(op, operators.or_):
            return sqlalchemy.or_(*[cls.sa_op(o) for o in op.ops])
        elif isinstance(op, operators.and_):
            return sqlalchemy.and_(*[cls.sa_op(o) for o in op.ops])
        elif isinstance(op, operators.not_):
            return sqlalchemy.not_(*[cls.sa_op(o) for o in op.ops])

        #else, assume op is an instance of op
        if isinstance(op, operators.eq):
            fn = lambda x, y: x == y
        elif isinstance(op, operators.ne):
            fn = lambda x, y: x != y
        elif isinstance(op, operators.gt):
            fn = lambda x, y: x > y
        elif isinstance(op, operators.lt):
            fn = lambda x, y: x < y
        elif isinstance(op, operators.gte):
            fn = lambda x, y: x >= y
        elif isinstance(op, operators.lte):
            fn = lambda x, y: x <= y
        elif isinstance(op, operators.in_):
            return sqlalchemy.or_(op.lval.in_(op.rval))
        else:
            raise TypeError('unsupported operator: %s' % op)

        rval = tup(op.rval)
        #TODO: modified from if not rval
        if rval is None:
            return '1+1=3'
        else:
            return sqlalchemy.or_(*[fn(op.lval, v) for v in rval])

    #TODO - only works with thing tables
    @classmethod
    def add_sort(cls, sort, entity_table, select):
        sort = tup(sort)
        prefixes = list(entity_table.keys()) if isinstance(entity_table, dict) else []
        if prefixes:
            #sort the prefixes so the longest come first
            prefixes.sort(key=lambda x: len(x))
        cols = []

        def make_sa_sort(_s):
            orig_col = _s.col

            col = orig_col
            if prefixes:
                table = None
                for k in prefixes:
                    if k and orig_col.startswith(k):
                        table = entity_table[k]
                        col = orig_col[len(k):]
                if table is None:
                    table = entity_table[None]
            else:
                table = entity_table
            real_col = translate_sort(table, col)

            #TODO a way to avoid overlap?
            #add column for the sort parameter using the sorted name
            select.append_column(real_col.label(orig_col))

            #avoids overlap temporarily
            select.use_labels = True

            #keep track of which columns we added so we can add joins later
            cols.append((real_col, table))

            #default to asc
            return (sqlalchemy.desc(real_col) if isinstance(_s, operators.desc)
                    else sqlalchemy.asc(real_col))

        sa_sort = [make_sa_sort(s) for s in sort]
        s = select.order_by(*sa_sort)
        return s, cols

    @classmethod
    def _add_entity_constraints(cls, query, table, constraints):
        cstr = deepcopy(constraints)

        for op in operators.op_iter(cstr):
            #assume key starts with _
            #if key.startswith('_'):
            key = op.lval_name
            op.lval = translate_sort(table, key[1:], op.lval)
            op.rval = translate_body_value(op.rval)

        for op in cstr:
            query.append_whereclause(cls.sa_op(op))

    def stat_entities(self, name, constraints, is_relation=False, stat_func=sqlalchemy.func.count):
        body_type = 'entity' if not is_relation else 'relation'
        primary_key = 'entity_id' if not is_relation else 'rel_id'
        table = self.get_table(name, body_type, 'body')
        s = sqlalchemy.select([stat_func(table.c.__getattr__(primary_key).label(primary_key))])
        self._add_entity_constraints(s, table, constraints)

        try:
            r = self.add_request_info(s).execute()
        except Exception as e:
            #todo handle dead db
            #dbm.mark_dead(table.bind)
            # this thread must die so that others may live
            raise

        return r

    def find_entities(self, name, sort, limit, constraints):
        table = self.get_table(name, 'entity', 'body')
        s = sqlalchemy.select([table.c.entity_id.label('entity_id')])
        self._add_entity_constraints(s, table, constraints)

        if sort:
            s, cols = self.add_sort(sort, {'_': table}, s)

        if limit:
            s = s.limit(limit)

        try:
            r = self.add_request_info(s).execute()
        except Exception as e:
            #todo handle dead db
            #dbm.mark_dead(table.bind)
            # this thread must die so that others may live
            raise

        fn = lambda row: row.entity_id
        return WrappedResultsProxy(r, fn)

    @classmethod
    def _add_prop_constraints(cls, query, body_table, prop_table, first_alias, constraints, append_column=True):
        cstr = deepcopy(constraints)
        have_data_rule = False
        used_first = False
        need_join = False

        for op in operators.op_iter(cstr):
            key = op.lval_name
            #vals = tup(op.rval)
            if key == '_id':
                op.lval = first_alias.c.body_id
            elif key.startswith('_'):
                need_join = True
                op.lval = translate_sort(body_table, key[1:], op.lval)
                op.rval = translate_body_value(op.rval)
            else:
                have_data_rule = True
                id_col = None
                if not used_first:
                    alias = first_alias
                    used_first = True
                else:
                    alias = prop_table.alias()
                    id_col = first_alias.c.body_id

                if id_col is not None:
                    query.append_whereclause(id_col == alias.c.body_id)

                if append_column:
                    query.append_column(alias.c.value.label(key))
                query.append_whereclause(alias.c.key == key)

                #add the substring constraint if no other functions are there
                translate_prop_value(alias, op)

        for op in cstr:
            query.append_whereclause(cls.sa_op(op))

        if not have_data_rule:
            raise Exception('Data queries must have at least one data rule.')

        return need_join

    def stat_props(self, name, constraints, is_relation=False, stat_func=sqlalchemy.func.count):
        body_type = 'entity' if not is_relation else 'relation'
        primary_key = 'body_id' if not is_relation else 'rel_id'
        body_table = self.get_table(name, body_type, 'body')
        prop_table = self.get_table(name, body_type, 'prop')

        first_alias = prop_table.alias()
        col = first_alias.c.__getattr__(primary_key).label(primary_key)
        s = sqlalchemy.select([stat_func(col)])
        need_join = self._add_prop_constraints(s, body_table, prop_table, first_alias, constraints, append_column=False)

        if need_join:
            s.append_whereclause(first_alias.c.body_id == body_table.c.entity_id)

        try:
            r = self.add_request_info(s).execute()
        except Exception as e:
            #dbm.mark_dead(t_table.bind)
            # this thread must die so that others may live
            raise
        return r

    #TODO sort by data fields
    #TODO sort by id wants body_id
    def find_props(self, name, sort, limit, constraints):
        body_table = self.get_table(name, 'entity', 'body')
        prop_table = self.get_table(name, 'entity', 'prop')

        first_alias = prop_table.alias()
        s = sqlalchemy.select([first_alias.c.body_id.label('body_id')])  # , distinct=True)
        need_join = self._add_prop_constraints(s, body_table, prop_table, first_alias, constraints)

        #TODO in order to sort by data columns, this is going to need to be smarter
        if sort:
            need_join = True
            s, cols = self.add_sort(sort, {'_': body_table}, s)

        if need_join:
            s.append_whereclause(first_alias.c.body_id == body_table.c.entity_id)

        if limit:
            s = s.limit(limit)

        if need_join:
            #print(s.compile())
            pass
        try:
            r = self.add_request_info(s).execute()
        except Exception as e:
            #dbm.mark_dead(t_table.bind)
            # this thread must die so that others may live
            raise

        return WrappedResultsProxy(r, lambda row: row.body_id)

    def find_rels(self, name, sort, limit, constraints):
        body_table = self.get_table(name, 'relation', 'body')
        prop_table = self.get_table(name, 'relation', 'prop')
        entity1_table = self.get_table(name, 'relation', 'entity1')
        entity2_table = self.get_table(name, 'relation', 'entity2')
        constraints = deepcopy(constraints)

        entity1_table, entity2_table = entity1_table.alias(), entity2_table.alias()

        s = sqlalchemy.select([body_table.c.rel_id.label('rel_id')])
        need_join1 = ('entity1_id', entity1_table)
        need_join2 = ('entity2_id', entity2_table)
        joins_needed = set()

        for op in operators.op_iter(constraints):
            #vals = con.rval
            key = op.lval_name
            prefix = key[:4]

            if prefix in ('_e1_', '_e2_'):
                #not a thing attribute
                key = key[4:]

                if prefix == '_e1_':
                    join = need_join1
                    joins_needed.add(join)
                elif prefix == '_e2_':
                    join = need_join2
                    joins_needed.add(join)
                else:
                    raise AssertionError('Unexpected prefix: ', prefix)

                table = join[1]
                op.lval = translate_sort(table, key, op.lval)
                op.rval = translate_body_value(op.rval)
                #ors = [sa_op(con, key, v) for v in vals]
                #s.append_whereclause(sa.or_(*ors))

            elif prefix.startswith('_'):
                op.lval = body_table.c[key[1:]]

            else:
                alias = prop_table.alias()
                s.append_whereclause(body_table.c.rel_id == alias.c.body_id)
                s.append_column(alias.c.value.label(key))
                s.append_whereclause(alias.c.key == key)

                translate_prop_value(alias, op)

        for op in constraints:
            s.append_whereclause(self.sa_op(op))

        if sort:
            s, cols = self.add_sort(sort,
                                    {'_': body_table,
                                     '_e1_': entity1_table,
                                     '_e2_': entity2_table},
                                    s)

            #do we need more joins?
            for (col, table) in cols:
                if table == need_join1[1]:
                    joins_needed.add(need_join1)
                elif table == need_join2[1]:
                    joins_needed.add(need_join2)

        for j in joins_needed:
            col, table = j
            s.append_whereclause(body_table.c[col] == table.c.entity_id)

        if limit:
            s = s.limit(limit)

        try:
            r = self.add_request_info(s).execute()
        except Exception as e:
            #dbm.mark_dead(body_table.bind)
            # this thread must die so that others may live
            raise
        return WrappedResultsProxy(r, lambda row: row.rel_id)

    def _get_kvs_entities_from_config(self, config):
        entities = {}
        for name, definition in config.items():
            body_table_name = definition['body']
            prop_table_name = definition['prop']
            avoid_master_read = definition['avoid_master_read'] if 'avoid_master_read' in definition else False
            body_tables = self._tables[body_table_name]

            entity = KVSEntity(name, avoid_master_read=avoid_master_read)
            for body_table in body_tables:
                prop_table = self.get_table_with_engine(prop_table_name, body_table.engine)
                entity_table = KVSEntityTable(body_table, prop_table)
                entity.tables.append(entity_table)
            entities[name] = entity
        return entities

    def _get_kvs_relations_from_config(self, config):
        relations = {}
        for name, definition in config.items():
            body_table_name = definition['body']
            prop_table_name = definition['prop']
            entity_left_name = definition['entity_left']
            entity_right_name = definition['entity_right']
            avoid_master_read = definition['avoid_master_read'] if 'avoid_master_read' in definition else False

            body_tables = self._tables[body_table_name]
            entity_left = self._kvs_entities[entity_left_name]
            entity_right = self._kvs_entities[entity_right_name]

            relation = KVSRelation(name, entity_left, entity_right, avoid_master_read=avoid_master_read)
            for body_table in body_tables:
                prop_table = self.get_table_with_engine(prop_table_name, body_table.engine)
                body_left_table = self.get_table_with_engine(entity_left.tables[0].body_table.name, body_table.engine)
                body_right_table = self.get_table_with_engine(entity_right.tables[0].body_table.name, body_table.engine)
                relation_table = KVSRelationTable(body_table, prop_table, body_left_table, body_right_table)
                relation.tables.append(relation_table)
            relations[name] = relation
        return relations
