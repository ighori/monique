from __future__ import division

import logging
import datetime
import re
from collections import OrderedDict
import string
from types import NoneType

import dateutil.parser
import pytimeparse

from mqe import util
from mqe.util import cached_property
from mqe.pars import parsing
from mqe import serialize


log = logging.getLogger('mqe.pars.enrichment')


FRAME_CHARS = {'-', '=', '+', '|', '_'}
FIELDSEP_CHARS = {',', '\t', ';'}
MAX_HEADER_ROWS = 5

null = object()


class Time(object):
    """A representation of the minutes and the seconds part of a full datetime"""

    def __init__(self, seconds):
        #: the total number of seconds contained in the hour and and the minute
        self.seconds = seconds

    def for_json(self):
        return {'__type__': 'Time', 'arg': self.seconds}

    def for_external_json(self):
        parts = []
        hours, rest = divmod(self.seconds, 3600)
        if hours:
            parts.append(str(hours))
        minutes, seconds = divmod(rest, 60)
        if parts:
            parts.append('%02d' % minutes)
        else:
            parts.append(str(minutes))
        parts.append('%02d' % seconds)
        return ':'.join(parts)

    def __unicode__(self):
        return u'Time(seconds=%s)' % self.seconds
    __repr__ = __unicode__
    def __hash__(self):
        return self.seconds
    def __cmp__(self, other):
        return cmp(self.seconds, other.seconds)


class EnrichedValue(object):
    """A class that wraps a raw value and allows converting it to other types / formats called
    *kinds*.
    The conversion methods are properties called ``as_<kind>`` which return the converted value if the conversion is possible, ``None`` otherwise. The returned value can be also equal to the raw
    value (no conversion taking place), making reading the property a boolean check.

    :param raw: the raw value, which can have any type, but the conversions are supported for
        :class:`NoneType` :class:`str`
        :class:`unicode` :class:`datetime.date` :class:`datetime.datetime` :class:`list`
        :class:`tuple` :class:`dict` :class:`int` :class:`float` :class:`bool`
    """

    def __init__(self, raw):
        self.raw = raw
        self._kinds = None

    @cached_property
    def raw_chars(self):
        if self.raw_is_str:
            return set(self.raw)
        return None

    @cached_property
    def raw_is_str(self):
        return isinstance(self.raw, basestring)

    def _raw_stripped(self):
        return self.raw.strip(',. :;')

    @cached_property
    def as_int(self):
        """The raw value as an :class:`int`"""
        try:
            return int(self.raw)
        except (ValueError, TypeError):
            return None

    @cached_property
    def as_float(self):
        """The raw value as a :class:`float`"""
        try:
            return float(self.raw)
        except (ValueError, TypeError):
            return None

    @cached_property
    def as_bool(self):
        """The raw value as a :class:`bool`. The property handles text strings
        like ``'success'`` or ``'no'``."""
        if isinstance(self.raw, bool):
            return self.raw
        if not self.raw_is_str:
            return None
        raw_lower = self.raw.strip().lower()
        if raw_lower in ('true', 'ok', 'success', 'yes'):
            return True
        if raw_lower in ('false', 'fail', 'failure', 'no', 'not', 'notok'):
            return False
        return None

    @cached_property
    def as_percent(self):
        """The raw value converted to a percent :class:`float` value. The
        property handles text strings like ``'23.5%'``"""
        if not self.raw_is_str:
            return None
        if not '%' in self.raw:
            return None
        try:
            num = float(self.raw.replace('%', ''))
        except ValueError:
            return None
        return num / 100

    
    _unit_to_divider = {
        'b': 1, 'byte': 1, 'bytes': 1,
        'k': 1024, 'kb': 1024, 'kib': 1024,
        'm': 1024**2, 'mb': 1024**2, 'mib': 1024**2,
        'g': 1024**3, 'gb': 1024**3, 'gib': 1024**3,
        't': 1024**4, 'tb': 1024**4, 'tib': 1024**4,
    }
    @cached_property
    def as_humansize(self):
        """The raw value as a number of bytes. The property
        handles text strings like ``'13MB'``, ``'2.5kB'``"""
        if not self.raw_is_str:
            return None
        num, rest = util.partition_numstr(self.raw)
        if num is None:
            return None
        rest = rest.strip().lower()
        if not rest:
            return None
        divider = self._unit_to_divider.get(rest)
        if divider is None:
            return None
        return num * divider

    @cached_property
    def as_number(self):
        """The raw value as a number - either a :class:`float`, an :class:`int`,
        the percent value or a number of bytes"""
        if self.as_int is not None:
            return self.as_int
        if self.as_float is not None:
            return self.as_float
        if self.as_percent is not None:
            return self.as_percent
        if self.as_humansize is not None:
            return self.as_humansize
        return None

    @cached_property
    def as_datetime(self):
        """The raw value as a :class:`datetime.datetime`. The property parses
        multiple datetime formats."""
        if isinstance(self.raw, datetime.date):
            return datetime.datetime.combine(self.raw, datetime.datetime.min.time())
        if isinstance(self.raw, datetime.datetime):
            return self.raw
        # Don't accidentally parse numbers as dates, as dateutil does that
        if self.as_number is not None or self.as_percent is not None:
            return None
        if self.as_time is not None:
            return None
        if not self.raw_is_str:
            return None
        # Empty strings are also parsed
        if not self.raw.strip():
            return None
        if len(self.raw) <= 3:
            return None
        if len(self.raw) > 30:
            return None
        if not any(c.isalnum() for c in self.raw):
            return None

        try:
            res = dateutil.parser.parse(self._raw_stripped())
            return res
        except:
            pass

        return None

    @property
    def optimistic_as_datetime(self):
        """A more optimistic version of :attr:`as_datetime`, which parses
        more datetime formats"""
        if isinstance(self.raw, datetime.date):
            return datetime.datetime.combine(self.raw, datetime.datetime.min.time())
        if isinstance(self.raw, datetime.datetime):
            return self.raw
        if not self.raw_is_str:
            return None
        try:
            res = dateutil.parser.parse(self._raw_stripped())
            return res
        except:
            pass

        try:
            res = util.parsedatetime_parse(self.raw)
            return res
        except:
            pass

    _re_time = re.compile(r'(\d+:)?(\d+):(\d+)(\.\d+)?')
    @cached_property
    def as_time(self):
        """The raw value as :class:`Time`"""
        if isinstance(self.raw, datetime.time):
            return self.raw
        if not self.raw_is_str:
            return None
        m = self._re_time.match(self.raw)
        if m is None:
            return None

        secs = pytimeparse.parse(self._raw_stripped())
        if secs is None:
            return None
        return Time(secs)

    @cached_property
    def as_datelike(self):
        """The raw value as either a :class:`~datetime.datetime` or :class:`Time`"""
        if self.as_datetime is not None:
            return self.as_datetime
        if self.as_time is not None:
            return self.as_time
        return None

    @cached_property
    def as_frame(self):
        """The raw value as an ASCII frame character, like ``|``"""
        if not self.raw_is_str:
            return None
        if self.raw and self.raw_chars.issubset(FRAME_CHARS):
            return self.raw
        return None

    @cached_property
    def as_fieldsep(self):
        """The raw value as a field separator, like a TAB character or ``;``"""
        if not self.raw_is_str:
            return None
        if self.raw and self.raw_chars.issubset(FIELDSEP_CHARS):
            return self.raw
        return None

    @cached_property
    def as_seplike(self):
        """The raw value as either a field separator or an ASCII frame"""
        if self.as_frame is not None:
            return self.as_frame
        if self.as_fieldsep is not None:
            return self.as_fieldsep
        if self.as_punct is not None:
            return self.as_punct
        return None

    @cached_property
    def as_punct(self):
        """The string value as a punctuation character"""
        if not self.raw_is_str:
            return None
        puncts = 0
        for c in self.raw:
            if c in string.punctuation:
                puncts += 1
            elif not c.isspace():
                return None
        if puncts >= 1:
            return self.raw
        return None

    _re_singleword = re.compile(r'^[0-9a-zA-Z_:,.()"\'\-]+$')
    @cached_property
    def as_singleword(self):
        """The raw value as a single word"""
        if not self.raw_is_str:
            return None
        if self._re_singleword.match(self.raw):
            return self.raw
        return None

    @cached_property
    def as_upper(self):
        """The raw value as an uppercased text"""
        if not self.raw_is_str:
            return None
        if self.raw.isupper():
            return self.raw
        return None

    @cached_property
    def as_lower(self):
        """The raw value as an lowercased text"""
        if not self.raw_is_str:
            return None
        if self.raw.islower():
            return self.raw
        return None

    @cached_property
    def as_capital(self):
        """The raw value as a capitalized text"""
        if not self.raw_is_str:
            return None
        if self.as_upper is None and len(self.raw) >= 2 and self.raw[0].isupper():
            return self.raw
        return None

    @cached_property
    def as_multifield(self):
        """The raw value as a multi-field text, containing newlines or TABs"""
        if not self.raw_is_str:
            return None
        if '\t' in self.raw or '\n' in self.raw:
            return self.raw
        return None

    @cached_property
    def as_empty(self):
        """The raw value as an empty value"""
        if not self.raw_is_str:
            return None
        if self.raw == '' or self.raw.isspace():
            return self.raw
        return None

    @cached_property
    def as_numberandword(self):
        """The raw value as a text containing a single word ana a single number"""
        if not self.raw_is_str:
            return None
        tokens = self.raw.split()
        if len(tokens) == 2:
            if (tokens[0].isdigit() and not tokens[1].isdigit()) or \
               (tokens[1].isdigit() and not tokens[0].isdigit()):
                return self.raw
        return None

    @cached_property
    def as_numericdata(self):
        """The raw value as numeric data - numbers or hex strings"""
        if not self.raw_is_str:
            return None
        if len(self.raw) > 200:
            return None
        num_digits = 0
        for c in self.raw:
            if c.isdigit():
                 num_digits += 1
            elif c in string.punctuation:
                pass
            elif c in string.hexdigits:
                pass
            else:
                return None
        if num_digits == 0:
            return None
        return self.raw

    @cached_property
    def as_null(self):
        if isinstance(self.raw, NoneType):
            return null
        return None


    def get_kind(self, kind):
        return EnrichedValue.kind_properties[kind].__get__(self)

    @property
    def rich(self):
        """Convert the raw value to the best matching Python type: a
        :class:`~datetime.datetime`, a :class:`float`, an :class:`int`"""
        if self.as_datetime is not None:
            return self.as_datetime
        if self.as_time is not None:
            return self.as_time
        if not self.raw_is_str:
            return self.raw

        if self.as_number is not None:
            return self.as_number
        if self.as_percent is not None:
            return self.as_percent
        return self.raw

    @property
    def optimistic_as_number(self):
        """A more optimistic version of :attr:`as_number`, which searches for
        a number within the raw value"""
        if util.is_number(self.raw):
            return self.raw
        converted = self.as_number
        if converted is not None:
            return converted
        if self.raw_is_str:
            converted = util.find_number(self.raw)
            if converted is not None:
                return converted
        converted = self.as_bool
        if converted is not None:
            return int(converted)
        if isinstance(self.raw, (list, dict, tuple)):
            converted = len(self.raw)
        if converted is not None:
            return converted
        return None

    def to_string_key(self):
        """Formats the raw value to a string:

        * if it's a string, return the value itself
        * it it's a :class:`datetime.datetime` or :class:`datetime.date`, return :meth:`datetime.datetime.isoformat`
        * otherwise, serialize the value to a JSON in a compact format (no unneeded spaces and
          newlines)

        """
        if self.raw is None:
            return ''
        if self.raw_is_str:
            return self.raw
        if isinstance(self.raw, datetime.date):
            return self.raw.isoformat()
        return serialize.mjson_external(self.raw)

    @cached_property
    def kinds(self):
        if self._kinds is not None:
            return self._kinds
        self._kinds = [name for name, prop in EnrichedValue.kind_properties.iteritems() if prop.__get__(self) is not None]
        return self._kinds

    @cached_property
    def items(self):
        return [(kind, EnrichedValue.kind_properties[kind].__get__(self)) for kind in self.kinds]

    def is_hashable(self):
        return not isinstance(self.raw, (list, dict, set))

    def __hash__(self):
        return hash(self.raw)
    def __cmp__(self, other):
        if not isinstance(other, EnrichedValue):
            return -1
        return cmp(self.raw, other.raw)
    def __repr__(self):
        return 'EV(%r)' % self.raw
    __unicode__ = __repr__


EnrichedValue.kind_properties = {name[3:]: getattr(EnrichedValue, name)
                                 for name in vars(EnrichedValue)
                                 if name.startswith('as_')}

@serialize.json_type('ET')
class EnrichedTable(parsing.Table):
    """

    A :class:`~mqe.pars.parsing.Table` which cells are :class:`EnrichedValue` objects. It adds
    support for getting table headers.

    Base class: :class:`~mqe.pars.parsing.Table`.

    :param ~mqe.pars.parsing.Table table: the table based on which to create the
        :class:`EnrichedTable`

    """

    def __init__(self, table):
        if table is None:
            super(EnrichedTable, self).__init__()
            return

        assert not isinstance(table, EnrichedTable), 'The table parameter must be an instance of ' \
                                                     'the base Table class'

        super(EnrichedTable, self).__init__(
            rows=[[EnrichedValue(raw) for raw in row] for row in table.rows],
            header_idxs=table.header_idxs,
            ignored_idxs=table.ignored_idxs,
            header_idxs_source=table.header_idxs_source,
        )
        if self.header_idxs is None:
            self.header_idxs = detect_header_idxs_avg(self)
            self.header_idxs_source = parsing.HEADER_IDXS_SOURCE_DETECTION


    def for_json(self):
        res = OrderedDict()

        # rows
        res['r'] = [[ev.raw for ev in row] for row in self.rows]

        # header_idxs
        if self.header_idxs is not None:
            res['hi'] = self.header_idxs

        # ignored_idxs
        if self.ignored_idxs is not None:
            res['ii'] = self.ignored_idxs

        # header_idxs_source
        if self.header_idxs_source is not None:
            res['hs'] = self.header_idxs_source

        return res

    @classmethod
    def from_rawjson(cls, raw):
        res = EnrichedTable(None)

        res.rows = []
        for row in raw['r']:
            res.rows.append([EnrichedValue(el) for el in row])

        hi = raw.get('hi')
        if hi is not None:
            res.header_idxs = hi

        ii = raw.get('ii')
        if ii is not None:
            res.ignored_idxs = ii

        hs = raw.get('hs')
        if hs is not None:
            res.header_idxs_source = raw.get('hs')

        return res

    @cached_property
    def all_headers(self):
        """A list of string headers, one for each column"""
        header_rows = self.header_rows
        if not header_rows:
            return [None] * self.num_columns
        res = []
        for i in range(self.num_columns):
            header = ' '.join(r[i].to_string_key() for r in header_rows)
            res.append(header)
        return res

    def header(self, idx):
        """Get the string header of the specified column"""
        return self.all_headers[idx]

    @cached_property
    def header_to_idx(self):
        """A dictionary mapping a header name to a column index. Contains only headers which
        uniquely map to a column."""
        if not self.has_header:
            return {}
        res = {}
        duplicated_headers = set()
        for i, h in enumerate(self.all_headers):
            if h in res:
                duplicated_headers.add(h)
            else:
                res[h] = i
        for dh in duplicated_headers:
            del res[dh]
        return res

    def column_spec(self, idx):
        """A specification of a column ``idx`` as a dictionary containing two keys:

        * ``idx`` - the passed ``idx``
        * ``header`` - a header of a column. The header is a non-``None`` value only if
           the parsers were sure it's a header, or the header was set manually"""
        if idx < 0:
            return {'idx': idx, 'header': None}

        # if header_idxs not set by parsers or API, don't use it in spec as it's too risky
        if self.header_idxs_source == parsing.HEADER_IDXS_SOURCE_DETECTION:
            header = None
        else:
            header = self.header(idx)
            # is it unique?
            if header not in self.header_to_idx:
                header = None
        return {'idx': idx, 'header': header}

    @classmethod
    def one_cell_table(cls, value):
        return EnrichedTable(parsing.Table.one_cell_table(value))

    def __unicode__(self):
        lines = []
        for row in self.rows:
            cells = ', '.join(repr(cell.raw) for cell in row)
            lines.append('    [%s],' % cells)
        return u'Table(header_idxs={self.header_idxs}, rows=[\n{rows}\n])\n'.format(
            self=self, rows=u'\n'.join(lines))

    __repr__ = __unicode__


_SCORE_FOR_VALUE_EV_CACHE = {}
def score_for_enriched_value(ev):
    global _SCORE_FOR_VALUE_EV_CACHE
    if ev.is_hashable():
        score = _SCORE_FOR_VALUE_EV_CACHE.get(ev)
        if score is not None:
            return score
        score = _compute_score_for_enriched_value(ev)
        _SCORE_FOR_VALUE_EV_CACHE[ev] = score
        return score
    else:
        return _compute_score_for_enriched_value(ev)

def _compute_score_for_enriched_value(ev):
    if ev.as_empty is not None:
        return 0
    if ev.as_seplike is not None or ev.as_multifield is not None:
        return -1000

    if ev.as_number is not None or ev.as_datelike is not None or ev.as_bool is not None or ev.as_null is not None:
        return 1000
    if ev.as_numericdata is not None:
        return 400
    if ev.as_numberandword is not None:
        return 150

    if ev.as_singleword is None:
        return 10

    if ev.as_upper is not None:
        return 50
    if ev.as_capital is not None:
        return 10
    if ev.as_lower is not None:
        return 125

    return 100

def _row_score(row):
    if not row:
        return 0
    s = 0
    for ev in row:
        ev_score = score_for_enriched_value(ev)
        s += ev_score
    s = max(s, 0)
    avg = s / len(row)
    return avg

def detect_header_idxs_avg(epr):
    rows = epr.nonignored_rows[:MAX_HEADER_ROWS*2]

    if not rows or len(rows) == 1:
        return []

    value_scores = []
    for row in rows:
        s = _row_score(row)
        log.debug('detect_header row score: %s %s', row, s)
        value_scores.append(_row_score(row))

    if len(value_scores) == 2:
        if value_scores[1] - value_scores[0] > 200:
            return [0]
        return []

    if len(value_scores) < MAX_HEADER_ROWS*2:
        filler = util.avg(value_scores[1:])
        value_scores.extend([filler] * (MAX_HEADER_ROWS*2 - len(value_scores)))

    avg_score = util.avg(value_scores)

    header_idxs = []
    for i in range(MAX_HEADER_ROWS):
        if not value_scores[i] < avg_score/2:
            break
        header_idxs.append(i)

    if log.isEnabledFor(logging.DEBUG):
        log.debug('outlier_summary %s %s %s', avg_score, header_idxs, value_scores)

    return header_idxs

