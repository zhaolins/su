__author__ = 'zhaolin.su'

from logging import handlers, Handler, FileHandler, getLogger
from su import mq


class MQHandler(Handler):
    def __init__(self, routing_key):
        Handler.__init__(self)
        self.routing_key = routing_key

    def send(self, record):
        mq.add(self.routing_key, record, encoding='pickle')

    def prepare(self, record):
        """
        Prepares a record for queuing. The object returned by this method is
        enqueued.

        The base implementation formats the record to merge the message
        and arguments, and removes unpickleable items from the record
        in-place.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also puts the message into
        # record.message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info attribute, as it's no longer needed and, if not None,
        # will typically not be pickleable.
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        return record

    def emit(self, record):
        try:
            self.send(self.prepare(record))
        except (KeyboardInterrupt, SystemExit):  # pragma: no cover
            raise
        except:
            self.handleError(record)
