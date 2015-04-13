__author__ = 'zhaolin'
import yaml
import os
import sys

if 'su.env' not in sys.modules:
    from su import env
    print("###loading test env###")
    env.DB.update(yaml.load(open(os.path.dirname(__file__) + '/test_schema.yml')))

    # env.MEMCACHED_SERVERS = {
    #     'main': ['127.0.0.1:11212'],
    #     'lock': ['127.0.0.1:11212'],
    # }

    env.REDIS_SERVERS = {
        'main': {
            'host': 'localhost',
            'port': 6379,
            'db': 8
        },
        'cache': {
            'host': 'localhost',
            'port': 6379,
            'db': 8
        },
        'session': {
            'host': 'localhost',
            'port': 6379,
            'db': 8
        },
        'lock': {
            'host': 'localhost',
            'port': 6379,
            'db': 8
        },
    }