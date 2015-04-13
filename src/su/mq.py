__author__ = 'zhaolin'

import pika
import pickle
import json
import socket
import sys
import time
from queue import Queue
from threading import Thread, local
from su.env import MQ
from su.g import stats, reset_cache_chains
from su.env import LOGGER
from copy import deepcopy


class BadMessageError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args)
        self.bug = kwargs.get('bug')


class Manager(local):
    def __init__(self):
        local.__init__(self)
        self.connection = None
        self.channel = None
        self.inited = False
        self.default_exchange = None
        self._exchanges = {}
        self._queues = {}
        self._bindings = {}

    @classmethod
    def _parse_config_connection(cls, name, config):
        parsed = deepcopy(config)
        parsed['credentials'] = pika.PlainCredentials(*parsed['credentials'])
        return parsed

    @classmethod
    def _parse_config_exchange(cls, name, config):
        parsed = deepcopy(config)
        parsed['exchange'] = name
        return parsed

    @classmethod
    def _parse_config_queue(cls, name, config):
        parsed = deepcopy(config)
        parsed['queue'] = name
        return parsed

    @classmethod
    def _parse_config_binding(cls, name, config):
        parsed = deepcopy(config)
        parsed['routing_key'] = name
        return parsed

    def get_connection(self):
        while not self.connection:
            try:
                for name, conn in MQ['connections'].items():
                    config = self._parse_config_connection(name, conn)
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(**config))
                    self.connection.channel()
                    break
            except (socket.error, IOError) as e:
                print('error connecting to mq %s:%s, %s' % (conn['host'], conn['port'], e))
                time.sleep(1)

        if not self.inited:
            self.declare()
            self.inited = True

        return self.connection

    def get_channel(self, reconnect=False):
        if self.connection and not self.connection._channels:
            LOGGER.error("Error: mq.py, connection object with no available channels.  Reconnecting...")
            self.connection = None

        if not self.connection or reconnect:
            self.connection = None
            self.channel = None
            self.get_connection()

        if not self.channel:
            self.channel = self.connection.channel()

        return self.channel

    def declare(self):
        chan = self.get_channel()
        for name, config in MQ['exchanges'].items():
            self._exchanges[name] = chan.exchange_declare(**self._parse_config_exchange(name, config))
        self.default_exchange = self._exchanges.get('su.main')

        for name, config in MQ['queues'].items():
            self._queues[name] = chan.queue_declare(**self._parse_config_queue(name, config))

        for name, config in MQ['bindings'].items():
            self._bindings[name] = chan.queue_bind(**self._parse_config_binding(name, config))

manager = Manager()


class Worker:
    def __init__(self):
        self.q = Queue()
        self.t = Thread(target=self._handle)
        self.t.setDaemon(True)
        self.t.start()

    def _handle(self):
        while True:
            # block until an item is available
            fn = self.q.get()
            try:
                fn()
                self.q.task_done()
            except:
                import traceback
                print(traceback.format_exc())

    def do(self, fn, *a, **kw):
        fn1 = lambda: fn(*a, **kw)
        self.q.put(fn1)

    def join(self):
        self.q.join()

worker = Worker()

DELIVERY_PERSISTENT = 2


def encode(msg, encoding=None):
    if encoding == 'pickle':
        return pickle.dumps(msg)
    elif encoding == 'json':
        return json.dumps(msg)
    else:
        return msg


def decode(msg, encoding=None):
    if encoding == 'text':
        return msg.decode('utf-8')
    elif encoding == 'pickle':
        return pickle.loads(msg)
    elif encoding == 'json':
        return json.loads(msg)
    else:
        return msg


def _add(routing_key, body, encoding=None,
         mandatory=False, immediate=False,
         delivery_mode=DELIVERY_PERSISTENT, headers=None, message_id=None, **kwargs):
    # if not amqp_host:
    #     log.error("Ignoring amqp message %r to %r" % (body, routing_key))
    #     return

    exchange = kwargs.pop('exchange', MQ['bindings'][routing_key]['exchange'])

    chan = manager.get_channel()
    properties = pika.BasicProperties(delivery_mode=delivery_mode,
                                      headers=headers,
                                      message_id=message_id,
                                      timestamp=int(time.time()))

    event_name = 'mq.%s' % routing_key
    queue = MQ['bindings'].get(routing_key)['queue']
    encoding = encoding or MQ['encodings'].get(queue)

    try:
        chan.basic_publish(body=encode(body, encoding), exchange=exchange, routing_key=routing_key,
                           mandatory=mandatory, immediate=immediate,
                           properties=properties)
    except Exception as e:
        stats.event_count(event_name, 'enqueue_failed')
        if isinstance(e, BrokenPipeError):
            manager.get_channel(True)
            add(routing_key, body, exchange=exchange,
                mandatory=mandatory, immediate=immediate,
                delivery_mode=delivery_mode, headers=headers, message_id=message_id)
        else:
            raise
    else:
        stats.event_count(event_name, 'enqueue')


def add(routing_key, body, **kwargs):
    worker.do(_add, routing_key, body, **kwargs)
    #_add(routing_key, body, **kwargs)


def consume(queue, callback, verbose=True, encoding=None):
    encoding = encoding or MQ['encodings'].get(queue)
    chan = manager.get_channel()

    chan.basic_qos(prefetch_size=0, prefetch_count=10, all_channels=False)

    def _callback(ch, method_frame, header_frame, msg):
        if verbose:
            print("%s: 1 item, %s" % (queue, method_frame))

        reset_cache_chains()

        try:
            ret = callback(decode(msg, encoding))
            ch.basic_ack(method_frame.delivery_tag)

            sys.stdout.flush()
            return ret
        except Exception as e:
            _log_msg(e, method_frame, header_frame, msg)
            if isinstance(e, BadMessageError):
                ch.basic_ack(method_frame.delivery_tag)
            else:
                ch.basic_reject(method_frame.delivery_tag, requeue=True)

    chan.basic_consume(consumer_callback=_callback, queue=queue)

    try:
        while chan.callbacks:
            try:
                chan.start_consuming()
            except KeyboardInterrupt:
                break
    finally:
        worker.join()
        if chan.is_open:
            chan.close()


def batch(queue, callback, encoding=None, ack=True, limit=1, min_size=0,
          drain=False, verbose=True, sleep_time=1):
    if limit < min_size:
        raise ValueError("min_size must be less than limit")

    chan = manager.get_channel()
    encoding = encoding or MQ['encodings'].get(queue)
    countdown = None

    while True:
        if countdown == 0:
            break

        method_frame, header_frame, msg = chan.basic_get(queue)
        if msg is None and drain:
            return
        elif msg is None:
            time.sleep(sleep_time)
            continue

        if countdown is None and drain and hasattr(method_frame, 'message_count'):
            countdown = 1 + method_frame.message_count

        reset_cache_chains()

        items = [(method_frame, header_frame, msg)]

        while countdown != 0:
            if countdown is not None:
                countdown -= 1
            if len(items) >= limit:
                break  # the innermost loop only
            method_frame, header_frame, msg = chan.basic_get(queue)
            if msg is None:
                if len(items) < min_size:
                    time.sleep(sleep_time)
                else:
                    break
            else:
                items.append((method_frame, header_frame, msg))

        try:
            if verbose:
                count_str = '(%d remaining)' % items[-1][0].message_count if hasattr(items[-1][0], 'message_count') \
                    else ''
                print("%s: %d items %s" % (queue, len(items), count_str))

            callback([decode(item[2], encoding) for item in items])

            if ack:
                # ack *all* outstanding messages
                chan.basic_ack(delivery_tag=0, multiple=True)

            # flush any log messages printed by the callback
            sys.stdout.flush()
        except Exception as e:
            if isinstance(e, BadMessageError):
                if e.bug is not None:
                    bug = items.pop(e.bug)
                    _log_msg(e, *bug)
                    chan.basic_ack(delivery_tag=bug[0].delivery_tag)
            for item in items:
                # explicitly reject the items that we've not processed
                chan.basic_reject(delivery_tag=item[0].delivery_tag, requeue=True)

            if not isinstance(e, BadMessageError):
                raise e


def _log_msg(error, method, header, msg):
    print("$ERROR: %s\n%s" % (msg, error))
