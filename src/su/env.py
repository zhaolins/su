__author__ = 'zhaolin.su'
import yaml
import pytz
import logging
import os

PASSWORD_STRENGTH = 4

DB = yaml.load(open(os.path.dirname(os.path.abspath(__file__)) + '/config/schema.yml'))

STATSD = {
    'url': 'localhost:8125',
    'sample_rate': 1.0
}

# MEMCACHED_SERVERS = {
#     'main': ['127.0.0.1:11211'],
#     'lock': ['127.0.0.1:11211'],
# }

REDIS_SERVERS = {
    'main': {
        'host': 'localhost',
        'port': 6379,
        'db': 0
    },
    'cache': {
        'host': 'localhost',
        'port': 6379,
        'db': 1
    },
    'session': {
        'host': 'localhost',
        'port': 6379,
        'db': 2
    },
    'lock': {
        'host': 'localhost',
        'port': 6379,
        'db': 3
    },
}

MQ = {
    'connections': {
        'main': {
            'host': 'localhost',
            'port': 5672,
            'virtual_host': '/',
            'credentials': ('su_user', 'asdf')
        },
    },
    'exchanges': {
        'su.main': {
            'exchange_type': 'direct',
            'passive': False,
            'durable': True,
            'auto_delete': False
        }
    },
    'queues': {
        'su.ha.log1': {},
        'su.ha.log2': {},
        'su.log3': {},
    },
    'bindings': {
        'route1': {
            'queue': 'su.ha.log1',
            'exchange': 'su.main'
        },
        'route2': {
            'queue': 'su.ha.log2',
            'exchange': 'su.main'
        }
    },
    'encodings': {
        'su.ha.log1': 'text',
        'su.ha.log2': 'pickle'
    }
}

TIMEZONE = pytz.timezone('Asia/Tokyo')

LOGGER = logging.getLogger('su')
