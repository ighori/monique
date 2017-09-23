from __future__ import division

import random

import datetime
import time
import re
from collections import OrderedDict
import uuid
from functools import wraps
import calendar
import importlib
import sys
import logging

import pytz


class Undefined(object):
    def __repr__(self):
        return '<undefined>'
    __str__ = __repr__
undefined = Undefined()


class cached_property(property):

    def __init__(self, func):
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self.func = func

    def __set__(self, obj, value):
        obj.__dict__[self.__name__] = value

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, undefined)
        if value is undefined:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value

    def __delete__(self, obj):
        if obj is None:
            return
        value = obj.__dict__.get(self.__name__, undefined)
        if value is not undefined:
            del obj.__dict__[self.__name__]


class DefaultFrozenDict(dict):
    def __init__(self, default):
        self._dfd_default = default
    def __missing__(self, key):
        return self._dfd_default()
    def __setitem__(self, item, value):
        raise ValueError('DefaultFrozenDict does not support __setitem__')


### datetime

MIN_DATETIME = datetime.datetime.utcfromtimestamp(0)
MAX_DATETIME = datetime.datetime.utcfromtimestamp(0x7FFFFFFF)

def datetime_to_timestamp(dt):
    if dt.tzinfo is not None:
        dt = make_tz_naive(dt)
    return (dt - MIN_DATETIME).total_seconds()

def datetime_from_timestamp(ts):
    return MIN_DATETIME + datetime.timedelta(seconds=ts)

def current_timestamp_millis():
    return int(round(time.time() * 1000))

def datetime_from_date(d):
    return datetime.datetime.combine(d, datetime.datetime.min.time())

def make_tz_naive(dt):
    return dt.astimezone(pytz.utc).replace(tzinfo=None)

def dt_to_unix_microseconds(dt):
    seconds = int(calendar.timegm(dt.utctimetuple()))
    return int((seconds * 1e6) + dt.time().microsecond)

def prev_dt(dt):
    return dt - datetime.timedelta(microseconds=1)

def next_dt(dt):
    return dt + datetime.timedelta(microseconds=1)



### seq & dict

def flatten(seq):
    return [el for subseq in seq for el in subseq]

def uniq_sameorder(lst, key=lambda x:x):
    seen = set() 
    return [seen.add(key(obj)) or obj for obj in lst if key(obj) not in seen]

def without_idx(lst, del_idx):
    return lst[:del_idx] + lst[del_idx+1:]

def iter_prefixes(seq, max_len=None, include_empty=False):
    if max_len is None:
        max_len = len(seq)+1
    for i in xrange(0 if include_empty else 1, max_len):
        yield seq[:i]

def chunks(l, n, fill_gen=None):
    for i in xrange(0, len(l), n):
        chunk = l[i:i+n]
        if fill_gen is not None:
            fill_to_len(chunk, n, fill_gen)
        yield chunk

def first(iterable, default=None, key=None):
    if key is None:
        for el in iterable:
            if el:
                return el
    else:
        for el in iterable:
            if key(el):
                return el
    return default

def nestedget(o, *keys):
    for k in keys:
        try:
            o = o[k]
        except (IndexError, KeyError, TypeError):
            return None
    return o

def cyclicget(ls, i):
    return ls[i % len(ls)]

def safeget(ls, i):
    if not ls:
        return None
    if i < 0 or i >= len(ls):
        return None
    return ls[i]

def powerset(lst):
    result = [[]]
    for x in lst:
        result.extend([subset + [x] for subset in result])
    return result

def all_equal(seq):
    fst = first(seq, default=undefined)
    if fst is undefined:
        return True
    for el in seq:
        if el != fst:
            return False
    return True

def dictproject(d, *keys):
    return {k: d[k] for k in keys if k in d}

def dictwithout(d, *keys):
    keys_set = set(keys)
    return {k: d[k] for k in d if k not in keys_set}

def sort_dict_by_key(d):
    res = OrderedDict()
    for k in sorted(d.keys()):
        res[k] = d[k]
    return res

def contains_dict(subdict, d):
    for k in subdict:
        if k not in d:
            return False
        if subdict[k] != d[k]:
            return False
    return True

def fill_to_len(lst, wanted_len, fill_gen):
    for i in range(len(lst), wanted_len):
        lst.append(fill_gen())

def valid_index(lst_or_len, idx):
    if not isinstance(lst_or_len, int):
        if lst_or_len is None:
            lst_or_len = 0
        else:
            lst_or_len = len(lst_or_len)
    return 0 <= idx < lst_or_len

def avg(seq):
    s = 0
    l = 0
    for x in seq:
        s += x
        l += 1
    return s / l

class CommonValue(object):

    def __init__(self):
        self._value = undefined

    def present(self, v):
        if v is None:
            return
        if self._value is None:
            return
        if self._value is undefined:
            self._value = v
        elif self._value != v:
            self._value = None
    @property
    def value(self):
        if self._value is undefined:
            return None
        return self._value

def common_value(seq):
    cv = CommonValue()
    for val in seq:
        cv.present(val)
        if cv._value is None:
            break
    return cv.value


### string

def simplify_whitespace(s):
    return ' '.join(s.split())

_re_word_split = re.compile(r'[^a-zA-Z_]+')
def find_nonnumeric_words(s):
    words = _re_word_split.split(s)
    return [w for w in words if w]


### number

def rectangles_intersect(d1, d2):
    return not (d1['x']+d1['width']<=d2['x'] or \
                d2['x']+d2['width']<=d1['x'] or \
                d1['y']+d1['height']<=d2['y'] or \
                d2['y']+d2['height']<=d1['y'])


def is_number_or_bool(x):
    return isinstance(x, (int, float))



### UUIDs

def gen_timetoken():
    return uuid.uuid1().hex

def uuid_lt(u1, u2):
    if u1.time < u2.time:
        return True
    if u1.time == u2.time and u1.bytes < u2.bytes:
        return True
    return False

def uuid_for_string(s):
    return uuid.uuid3(uuid.NAMESPACE_OID, s)

def timestamp_from_uuid1(u):
    return (u.time - 0x01B21DD213814000) / 1e7

def datetime_from_uuid1(u):
    return datetime.datetime.utcfromtimestamp(timestamp_from_uuid1(u))

def uuid_with_timestamp(microseconds, lowest_val=False, randomize=False):
    ts = int(microseconds * 10) + long(0x01b21dd213814000)
    time_low = ts & long(0xffffffff)
    time_mid = (ts >> 32) & long(0xffff)
    time_hi_version = (ts >> long(48)) & long(0x0fff)
    if randomize:
        cs = random.randrange(1 << long(14))
        clock_seq_low = cs & long(0xff)
        clock_seq_hi_variant = (cs >> long(8)) & long(0x3f)
        node = uuid.getnode()
    else:
        if lowest_val: # uuid with lowest possible clock value
            clock_seq_low = 0 & long(0xff)
            clock_seq_hi_variant = 0 & long(0x3f)
            node = 0 & long(0xffffffffffff) # 48 bits
        else: # UUID with highest possible clock value
            clock_seq_low = long(0xff)
            clock_seq_hi_variant = long(0x3f)
            node = long(0xffffffffffff) # 48 bits
    return uuid.UUID(
        fields=(time_low, time_mid, time_hi_version,
                clock_seq_hi_variant, clock_seq_low, node),
        version=1
    )

def min_uuid_with_dt(dt):
    return uuid_with_timestamp(dt_to_unix_microseconds(dt), True)

def max_uuid_with_dt(dt):
    return uuid_with_timestamp(dt_to_unix_microseconds(dt), False)

def uuid_with_dt(dt):
    return uuid_with_timestamp(dt_to_unix_microseconds(dt), False, True)

def time60_from_uuid(u):
    return u.get_time()

def time60_now():
    return time60_from_uuid(uuid.uuid1())

def uuid_from_time60(time60, node=None, clock_seq=None):
    intervals = time60

    time_low = intervals & 0xffffffff
    time_mid = (intervals >> 32) & 0xffff
    time_hi_version = (intervals >> 48) & 0x0fff

    if clock_seq is None:
        clock_seq = random.getrandbits(14)
    else:
        if clock_seq > 0x3fff:
            raise ValueError('clock_seq is out of range (need a 14-bit value)')

    clock_seq_low = clock_seq & 0xff
    clock_seq_hi_variant = 0x80 | ((clock_seq >> 8) & 0x3f)

    if node is None:
        node = random.getrandbits(48)

    return uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                             clock_seq_hi_variant, clock_seq_low, node), version=1)

def min_uuid_from_time60(time60):
    return uuid_from_time60(time60, 0x808080808080, 0x80)

def max_uuid_from_time60(time60):
    return uuid_from_time60(time60, 0x7f7f7f7f7f7f, 0x3f7f)

def uuid_for_prev_dt(u):
    """Return the maximal time-UUID for the timestamp immediately preceding the timestamp of the time-UUID ``u``"""
    t60 = time60_from_uuid(u)
    return max_uuid_from_time60(t60 - 1)

def uuid_for_next_dt(u):
    """Return the minimal time-UUID for the timestamp immediately following the timestamp of the time-UUID ``u``"""
    t60 = time60_from_uuid(u)
    return min_uuid_from_time60(t60 + 1)


MIN_UUID = min_uuid_with_dt(MIN_DATETIME)
MAX_UUID = max_uuid_with_dt(MAX_DATETIME)


### other

def cache():
    def decorator(f):
        f._cache = {}
        @wraps(f)
        def decorated(*args, **kwargs):
            key = (tuple(args), frozenset(kwargs.items()))
            if key in f._cache:
                return f._cache[key]
            f._cache[key] = f(*args, **kwargs)
            return f._cache[key]
        return decorated
    return decorator

def clear_cache(f):
    if hasattr(f, '_cache'):
        f._cache.clear()

def run_once(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if wrapped._did_run:
            return
        try:
            f(*args, **kwargs)
        finally:
            wrapped._did_run = True
    wrapped._did_run = False
    return wrapped

class NotCompleted(Exception):
    pass

def try_complete(tries, f, after_fail=lambda try_no: None, cond=lambda res: res):
    """Runs the ``f`` function until it returns a ``cond``-satisfying result and returns the
    result. Raises :class:`NotCompleted` when couldn't do that in up to ``tries`` tries.
    Runs ``after_fail(try_no) after each unsuccessful attempt."""
    for i in xrange(tries):
        res = f()
        if cond(res):
            return res
        after_fail(i)
    raise NotCompleted()

def import_module_var(var_path):
    module_path = var_path[:var_path.rfind('.')]
    module = sys.modules.get(module_path) or importlib.import_module(module_path)
    return getattr(module, var_path)

def setup_logging(level_name='INFO', debug_queries=None, stream=sys.stdout,
                  format='%(asctime)s %(levelname)s %(message)s',
                  loggers=['mqe', 'mqeweb', 'mqeapi', 'mqetables']):
    """Setup logging to a stream for the given loggers. The call ``setup_logging()``
    will configure logging to stdout with level INFO for the library's loggers."""
    if debug_queries is not None:
        from mqe import mqeconfig
        mqeconfig.DEBUG_QUERIES = debug_queries
    hdlr = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(format)
    hdlr.setFormatter(formatter)
    for name in loggers:
        logging.getLogger(name).addHandler(hdlr)
        logging.getLogger(name).setLevel(logging.getLevelName(level_name))




