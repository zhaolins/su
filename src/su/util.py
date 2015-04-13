__author__ = 'zhaolin.su'
import os
import traceback
import re
import datetime
import time
from itertools import islice
from collections import OrderedDict
from su.env import LOGGER

iters = (list, tuple, set)


def simple_traceback(limit):
    """Generate a pared-down traceback that's human readable but small.

    `limit` is how many frames of the stack to put in the traceback.

    """

    stack_trace = traceback.extract_stack(limit=limit)[:-2]
    return "\n".join(":".join((os.path.basename(filename),
                               function_name,
                               str(line_number),))
                     for filename, line_number, function_name, text
                     in stack_trace)


def split_list(it, size=25):
    chunk = []
    it = iter(it)
    try:
        while True:
            chunk.append(next(it))
            if len(chunk) >= size:
                yield chunk
                chunk = []
    except StopIteration:
        if chunk:
            yield chunk


class Storage(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __repr__(self):
        return '<Storage ' + dict.__repr__(self) + '>'


class Timer(object):
    def __init__(self, name=None, verbose=True):
        self.name = name
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print('#%s# elapsed time: %f ms' % (self.name, self.msecs))


def constant_time_compare(actual, expected):
    actual_len = len(actual)
    expected_len = len(expected)
    result = actual_len ^ expected_len
    if result != 0:
        return False

    if expected_len > 0:
        for i in range(actual_len):
            result |= ord(actual[i]) ^ ord(expected[i % expected_len])
            if result != 0:
                return False
    return True


def num2base(num, base):
    if num < 0:
        raise ValueError("must supply a positive integer")
    l = len(base)
    converted = []
    while num != 0:
        num, r = divmod(num, l)
        converted.insert(0, base[r])
    return "".join(converted) or '0'


def alnum(num):
    # num to alphabetic and numeric characters
    base = '0123456789abcdefghijklmnopqrstuvwxyz'
    return num2base(num, base)


def tup(item, ret_is_single=False):
    if not isinstance(item, str) and hasattr(item, '__iter__'):
        return (item, False) if ret_is_single else item
    elif item is not None:
        return ((item,), True) if ret_is_single else (item,)
    else:
        return ((), True) if ret_is_single else ()


def map_dict_keys(keys, prefix):
    mapped_dict = {}
    for key in keys:
        mapped_dict[prefix + str(key)] = key
    return mapped_dict


def unmap_dict_keys(mapped_dict, key_mapping):
    origin_dict = {}
    for k, v in mapped_dict.items():
        origin_dict[key_mapping[k]] = v
    return origin_dict


def call_with_prefix_keys(items, fn, prefix=''):
    if not len(prefix):
        return fn(items)

    if hasattr(items, 'keys'):
        orig_keys = items.keys()
        key_mapping = map_dict_keys(orig_keys, prefix)
        prefixed_items = {prefix + str(k): v for k, v in items.items()}
    elif hasattr(items, '__iter__'):
        orig_keys = items
        key_mapping = map_dict_keys(orig_keys, prefix)
        prefixed_items = key_mapping.keys()
    else:
        raise ValueError('cannot prefix items %s' % items)

    resource = fn(prefixed_items)
    result = unmap_dict_keys(resource, key_mapping)
    return result


def general_retriever(get_func, set_func, keys, key_filter=None, miss_fn=None, found_fn=None, is_update=False):
    result = {}

    if not key_filter:
        key_filter = str
    cache_keys = {key_filter(k): k for k in keys}

    if not is_update:
        cached = get_func(cache_keys.keys())
        if cached:
            for k, v in cached.items():
                result[cache_keys[k]] = v

    missed = set(cache_keys.values()) - set(result.keys())

    if found_fn:
        found_fn(result, missed)

    if miss_fn and missed:
        complementary = miss_fn(missed)
        result.update(complementary)
        set_cache = {key_filter(k): v for k, v in complementary.items()}
        set_func(set_cache)
        LOGGER.debug('general_retriever: missing: %s, complementary found: %s' % (str(missed), str(len(complementary))))

    return result


def cache_retriever(cache, keys, miss_fn=None, prefix='', found_fn=None, is_update=False):
    get_func = cache.get_multi
    set_func = cache.set_multi
    key_filter = lambda x: prefix + str(x).replace(' ', '')

    return general_retriever(get_func, set_func, keys,
                             key_filter=key_filter, miss_fn=miss_fn, found_fn=found_fn, is_update=is_update)


def flatten(lists, unique=False, compare_fn=None):
    result = []
    if not unique:
        for lst in lists:
            result.extend(lst)
    else:
        exists = set()
        for lst in lists:
            for item in lst:
                i = item if not compare_fn else compare_fn(item)
                if i not in exists and not exists.add(i):
                    result.append(item)

    return result


def explode(string, token=',', trim=True):
    if trim:
        return [s.strip() for s in string.split(token)]
    else:
        return [s for s in string.split(token)]


def _strips(direction, text, remove):
    if direction == 'l':
        if text.startswith(remove):
            return text[len(remove):]
    elif direction == 'r':
        if text.endswith(remove):
            return text[:-len(remove)]
    else:
        raise ValueError("Direction needs to be r or l.")
    return text


def rstrips(text, remove):
    """
    removes the string `remove` from the right of `text`

        >>> rstrips("foobar", "bar")
        'foo'

    """
    return _strips('r', text, remove)


def lstrips(text, remove):
    """
    removes the string `remove` from the left of `text`

        >>> lstrips("foobar", "foo")
        'bar'

    """
    return _strips('l', text, remove)


def strips(text, remove):
    """removes the string `remove` from the both sides of `text`

        >>> strips("foobarfoo", "foo")
        'bar'

    """
    return rstrips(lstrips(text, remove), remove)

ESCAPE = re.compile(r'[\x00-\x19\\"\b\f\n\r\t]')
ESCAPE_ASCII = re.compile(r'([\\"/]|[^\ -~])')
ESCAPE_DCT = {
    # escape all forward slashes to prevent </script> attack
    '/': '\\/',
    '\\': '\\\\',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}


def _string2js_replace(match):
    return ESCAPE_DCT[match.group(0)]


def string2js(s):
    """adapted from http://svn.red-bean.com/bob/simplejson/trunk/simplejson/encoder.py"""
    for i in range(20):
        ESCAPE_DCT.setdefault(chr(i), '\\u%04x' % (i,))
    return '"' + ESCAPE.sub(_string2js_replace, s) + '"'


def bisect_right(a, x, lo=0, hi=None, lt_fn=None):
    """Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e <= x, and all e in
    a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
    insert just after the rightmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """
    if not lt_fn:
        lt_fn = lambda l, r: l < r

    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo+hi)//2
        if lt_fn(x, a[mid]):
            hi = mid
        else:
            lo = mid+1
    return lo


def bisect_left(a, x, lo=0, hi=None, lt_fn=None):
    """Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
    insert just before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """
    if not lt_fn:
        lt_fn = lambda l, r: l < r

    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo+hi)//2
        if lt_fn(a[mid], x):
            lo = mid+1
        else:
            hi = mid
    return lo


def _slice(seq, start=None, end=None):
    if isinstance(seq, list):
        return seq[start:end]
    elif isinstance(seq, OrderedDict):
        return OrderedDict(islice(seq.items(), start, end))
    else:
        raise TypeError("type %s not supported yet" % type(seq))


def slice_seq(seq, anchor, limit=0, direction='after', lt_fn=None, anchor_in_list=True):
    if not anchor:
        return _slice(seq, end=limit) if limit > 0 else seq

    if not lt_fn:
        lt_fn = lambda x, y: x < y

    lst = list(seq.items()) if hasattr(seq, 'items') else seq
    i = bisect_left(lst, anchor, lt_fn=lt_fn)

    if direction == 'after':
        base = i+1 if anchor_in_list else i
        return _slice(seq, base, base+limit) if limit > 0 else _slice(seq, base)
    elif direction == 'before':
        base = i
        start = None if limit <= 0 or base <= limit else base-limit
        end = base if base > 0 else 0
        return _slice(seq, start, end)
    else:
        raise TypeError("direction %s not supported yet" % direction)

_epoch = datetime.datetime(1970, 1, 1)


def epoch_seconds(date=None):
    date = date.replace(tzinfo=None) if date is not None else datetime.datetime.now()
    td = date - _epoch
    return td.days * 86400 + td.seconds + (float(td.microseconds) / 1000000)


def diff_entities(list1, list2):
    if list1 == list2:
        return False
    elif not list1 or not list2:
        return True

    if len(list1) != len(list2):
        return True
    for i, e in enumerate(list1):
        if e._id != list2[i]._id:
            return True

    return False