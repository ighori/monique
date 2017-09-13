from __future__ import division

import copy
import json
import logging
import re
from collections import OrderedDict, defaultdict, Counter

from mqe import util
from mqe.ext import csv
from mqe.pars import parsing
from mqe.util import dict_copy_with_item, cached_property


log = logging.getLogger('mqe.pars.basicparsing')


MAX_LINE_LEN = 4000


class FilteredUserInput(parsing.UserInput):
    """A :class:`~mqe.pars.parsing.UserInput` that deletes lines from the text input known to be
    comments / unwanted labels.
    """

    res_last_line = [
        # psql
        re.compile(r'\(\d+ rows?\)'),
        # sqlplus
        re.compile(r'\d+ rows selected.'),
        # mysql
        re.compile(r'\d+ rows in set.*'),
    ]

    def __init__(self, orig):
        self.orig = orig

    @cached_property
    def input_string(self):
        input_string = self.orig.input_string.rstrip('\n\r')
        log.debug('input_string filtered: %r', input_string)
        last_line_start = input_string.rfind('\n')
        if last_line_start == -1:
            last_line_start = 0
        last_line_len = len(input_string) - last_line_start
        if last_line_len > MAX_LINE_LEN:
            last_line = None
        else:
            last_line = input_string[last_line_start:].strip()

        if last_line is not None:
            for r in self.res_last_line:
                if r.match(last_line):
                    input_string = input_string[:last_line_start]
                    break

        return input_string


class CSVParser(parsing.InputParser):
    """CSV parser, auto-detecting a delimiter and quoting. The delimiter can be also passed inside ``ip_options``."""

    multiplier = 2.0

    def _detect_dialect(self, input_string):
        try:
            dialect = csv.Sniffer().sniff(input_string)
            return dialect
        except csv.Error:
            return None

    def _create_reader(self, user_input):
        if self.ip_options.get('delimiter'):
            if len(self.ip_options['delimiter']) > 1:
                raise parsing.NotParsable()
            self._has_header = csv.Sniffer().has_header(user_input.input_string)
            self._reader = csv.reader(user_input.lines, delimiter=self.ip_options['delimiter'])
            return

        dialect = self._detect_dialect(user_input.input_string)
        if dialect is None:
            raise parsing.NotParsable('No CSV dialect')
        if log.isEnabledFor(logging.DEBUG):
            log.debug('Detected CSV dialect: %s', dialect.__dict__)

        if dialect.delimiter in {'.'}:
            raise parsing.NotParsable('Invalid delimiter')
        if dialect.delimiter not in {',', '\t', ';', ':', ','}:
            log.debug('Lowering multiplier due to nonstandard delimiter')
            self.multiplier = 0.5

        self._has_header = csv.Sniffer().has_header(user_input.input_string, _sniff_result=dialect)
        self._reader = csv.reader(user_input.lines, dialect=dialect)

    def parse(self, user_input):
        self._create_reader(user_input)
        rows = list(self._reader)
        rows = [r for r in rows if r]
        for r in rows:
            for i in xrange(len(r)):
                r[i] = r[i].strip()

        if self._has_header and len(rows) > 1:
            header_idxs = [0]
        else:
            header_idxs = []

        return parsing.Table(rows, header_idxs=header_idxs,
                             header_idxs_source=parsing.HEADER_IDXS_SOURCE_DETECTION)

def _2cell_row(row):
    if len(row) == 1:
        row.append('')
    return [row[0].strip(), row[1].strip()]


class PropertiesParser(parsing.InputParser):
    """A parser of "properties" files containing ``key <delimiter> value`` lines.
    The delimiter is auto-detected. The resulting table will be composed of two-cell rows."""

    freeform = True

    #: possible delimiters
    splitstrings = ('=', ':', ';', '-')

    _splitstring_to_rank = {ss: idx for idx, ss in enumerate(splitstrings)}

    def parse(self, user_input):
        candidates = []
        bonuses = []
        for line in user_input.lines:
            line = line.strip()
            ranges = util.chars_ranges(self.splitstrings, line)
            #log.debug('ranges: %s', ranges)
            for c in ranges:
                valid_ranges = [r for r in ranges[c] if r[0] != 0 and r[-1] != len(line)]

                ranges_by_len = defaultdict(list)
                for r in valid_ranges:
                    ranges_by_len[r[1]-r[0]].append(r)

                for range_len, range_list in ranges_by_len.items():
                    candidates.append((c, range_len))
                    # add a bonus point if a split char is separated by spaces
                    for sr in range_list:
                        if sr[0] > 0 and sr[1] < len(line) and line[sr[0]-1] == ' ' and line[sr[1]] == ' ':
                            bonuses.append((c, range_len))
                            # only one bonus point
                            break
        if not candidates:
            raise parsing.NotParsable('No candidates')

        counter_candidates = Counter(candidates)
        counter_bonuses =  Counter(bonuses)
        counter = counter_candidates + counter_bonuses
        log.debug('counter_candidates, counter_bonuses, counter:\n%s\n%s\n%s', counter_candidates, counter_bonuses, counter)
        best_score = counter.most_common(1)[0][1]
        if best_score == 0:
            raise parsing.NotParsable('best_score: %s' % best_score)
        if best_score == 1:
            self.multiplier = 0.5
        # Assure we select the first item with the best score (to prevent randomness
        # caused by dict (Counter) ordering)
        with_best_score = [candidate for candidate in counter if counter[candidate] == best_score]
        best = min(with_best_score, key=lambda candidate: self._splitstring_to_rank[candidate[0]])

        split_str = best[0] * best[1]
        rows = []
        for line in user_input.lines:
            row = line.split(split_str, 1)
            rows.append(_2cell_row(row))

        return parsing.Table(rows)


class KeyValueParser(parsing.InputParser):
    """A parser of ``key value`` lines where ``key`` is the first word and ``value`` is the rest
    of the line. The resulting table will be composed of two-cell rows."""

    freeform = True

    multiplier = 0.5

    def parse(self, user_input):
        rows = []
        for line in user_input.lines:
            row = line.split(None, 1)
            if row:
                rows.append(_2cell_row(row))

        log.debug('Raw rows: %s', rows)
        return parsing.Table(rows, header_idxs=[])


class MultipleValuesParser(parsing.InputParser):
    """A parser that puts each word into a table cell. Supports the ``ip_options['delimiter']``
    setting."""

    multiplier = 0.5
    freeform = True

    def parse(self, user_input):
        if self.ip_options.get('delimiter'):
            split_str = self.ip_options['delimiter']
        else:
            split_str = None

        rows = []
        for line in user_input.lines:
            parts = line.split(split_str)
            parts = [p.strip() for p in parts]
            rows.append(parts)

        return parsing.Table(rows)


class JsonParser(parsing.InputParser):
    """A parser of JSON documents.

    There are two normalized document types:

    * an array of arrays of cells will be directly mapped to the resulting :class:`Table`
    * an array of objects, where each object represents a row by mapping a column name to a cell
      value. The format will be mapped to a :class:`Table` having a header row collected from the
      objects' keys.

    The parser handles formats not described above. For example, for mixed arrays and objects it
    will still try to create proper rows, and fill missing cells with ``null`` values. It also
    maps a single object or a number into a single-row table.

    """

    multiplier = parsing.MULTIPLIER_SURE

    def _row_list_from_doc(self, doc):
        if isinstance(doc, list):
            return [ [(i, None, el) for i, el in enumerate(doc)] ]
        if isinstance(doc, dict):
            return [ [(None, k, v) for k, v in doc.iteritems()] ]
        return [ [(0, None, doc)] ]

    def _do_parse(self, doc, original_doc):
        if isinstance(doc, list):
            rows = util.flatten(self._row_list_from_doc(el) for el in doc)
        elif isinstance(doc, dict):
            rows = self._row_list_from_doc(doc)
        else:
            rows = [[(0, None, doc)]]
        log.debug('Json rows: %s', rows)
        converted = parsing.from_colno_colname_values(rows)
        log.debug('Json converted: %s', converted)
        return parsing.Table(**converted)

    def parse(self, user_input):
        try:
            doc = json.loads(user_input.input_string, object_pairs_hook=OrderedDict)
        except ValueError:
            raise parsing.NotParsable('Not a JSON document')
        return self._do_parse(doc, copy.deepcopy(doc))


MAX_JSONDEEP_EXPANDED = 10000

class JsonDeepParser(JsonParser):
    """An extension of a :class:`JsonParser` that applies additional processing allowing putting
    nested values into individual cells:

    * nested objects are flattened, with keys joined with a . (dot) character. For example, ``{"x": {"y": 8, "z": true}}`` is converted to ``{"x.y": 8, "x.z": true}``.
    * arrays of objects contained in outer objects are unnested. For example, ``{"x": [{"y": 1}, {"y": 2}]}`` is converted to ``[{"x.y": 1}, {"x.y": 2}]``.
    """

    def _do_parse(self, doc, original_doc):

        def rewrite_one_to_many(xdoc):
            if isinstance(xdoc, dict):
                for k, v in xdoc.items():
                    xdoc[k], is_rewritten = rewrite_one_to_many(v)

                to_rewrite = [xdoc]

                def rewrite_one_key(start_from):
                    for i in xrange(start_from, len(to_rewrite)):
                        curr_doc = to_rewrite[i]
                        for k, v in curr_doc.iteritems():
                            if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                                return i, [dict_copy_with_item(curr_doc, k, x) for x in v]
                    return -1, []

                rewritten_idx = 0
                while True:
                    rewritten_idx, new_docs = rewrite_one_key(rewritten_idx)
                    if rewritten_idx == -1 or not new_docs:
                        break
                    to_rewrite[rewritten_idx:rewritten_idx+1] = new_docs
                    log.debug('to_rewrite: %d', len(to_rewrite))
                    if len(to_rewrite) > MAX_JSONDEEP_EXPANDED:
                        raise parsing.NotParsable('Too many expanded')

                if len(to_rewrite) == 1 and to_rewrite[0] is xdoc:
                    return (xdoc, False)
                return (to_rewrite, True)

            if isinstance(xdoc, list):
                res = []
                for x in xdoc:
                    rdoc, is_rewritten = rewrite_one_to_many(x)
                    if is_rewritten and isinstance(rdoc, list):
                        res.extend(rdoc)
                    else:
                        res.append(rdoc)
                return (res, False)

            return (xdoc, False)

        def to_items(key, value):
            if not isinstance(value, dict) or (not value and key):
                return [(key, value)]
            res = []
            for inner_key, inner_value in value.items():
                if key:
                    if inner_key:
                        k = '%s.%s' % (key, inner_key)
                    else:
                        k = key
                else:
                    k = inner_key
                res.extend(to_items(k, inner_value))
            #log.debug('to_items(%s, %s)=%s', key, value, res)
            return res

        def walk_to_items(xdoc):
            if isinstance(xdoc, list):
                return [walk_to_items(x) for x in xdoc]
            if isinstance(xdoc, dict):
                return OrderedDict(to_items(None, xdoc))
            return xdoc

        rewritten_doc, is_rewritten = rewrite_one_to_many(doc)
        #if log.isEnabledFor(logging.DEBUG):
        #    log.debug('rewritten_doc:\n%s', pformat(rewritten_doc))
        walked_doc = walk_to_items(rewritten_doc)
        #if log.isEnabledFor(logging.DEBUG):
        #    log.debug('walked_doc:\n%s', pformat(walked_doc))
        return JsonParser._do_parse(self, walked_doc, original_doc)


class SingleValueParser(parsing.InputParser):
    """Parses the single-word input into a single-cell table."""

    multiplier = 10.0

    _re_word = re.compile(r'^[a-zA-Z0-9_]+$')
    def parse(self, user_input):
        if re.match(self._re_word, user_input.stripped):
            return parsing.Table.one_cell_table(user_input.stripped)
        raise parsing.NotParsable()


class OneCellParser(parsing.InputParser):
    """Treats the whole input as a single-cell text."""

    multiplier = 1.0

    def parse(self, user_input):
        return parsing.Table.one_cell_table(user_input.input_string)

