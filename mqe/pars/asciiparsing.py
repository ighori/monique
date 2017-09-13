from __future__ import division

from collections import defaultdict, namedtuple
import pprint
import logging
import re

from mqe import util
from mqe.pars import parsing
from mqe.pars import tokens as tokensmod


log = logging.getLogger('mqe.pars.asciiparsing')


CALIGN_LEFT = 0
CALIGN_RIGHT = 1
CALIGN_CENTER = 2

LINES_FOR_COLUMNSPEC_DISCOVERY = 1000
MAX_CHARS_OFF_A_SPEC = 1

FRAMECHARS = ['|', '+', '=', '-']
FRAMECHARS_HORIZONTAL = ['=', '-', '+']
FRAMECHARS_VERTICAL = ['|']

TABSTOP = 8

RE_ANY_FRAMECHAR = re.compile('[%s]' % ''.join(FRAMECHARS))

MAX_HORIZONTAL_LINENO_AS_BOTTOM_HEADER_BORDER = 4


class ColumnSpec(object):

    def __init__(self, col_start, col_end, calign):
        self.col_start = col_start
        self.col_end = col_end
        self.calign = calign

    def align_column(self):
        if self.calign == CALIGN_LEFT:
            return self.col_start
        if self.calign == CALIGN_RIGHT:
            return self.col_end
        if self.calign == CALIGN_CENTER:
            return int((self.col_start + self.col_end) / 2)
        assert False, 'Unknown alignment %r' % self.calign

    def with_offset(self, off):
        return ColumnSpec(self.col_start + off, self.col_end + off, self.calign)

    def __hash__(self):
        return hash((self.col_start, self.col_end, self.calign))
    def __cmp__(self, other):
        return cmp((self.col_start, self.col_end, self.calign),
                   (other.col_start, other.col_end, other.calign))
    def __unicode__(self):
        return u'CS(%s, %s, %s)' % (self.col_start, self.col_end,
            'L' if self.calign == CALIGN_LEFT else
            'R' if self.calign == CALIGN_RIGHT else
            'C' if self.calign == CALIGN_CENTER else
            self.calign)
    __repr__ = __unicode__


def parsing_result_from_token_rows(rows, header_idxs):
    value_rows = []
    for row in rows:
        new_row = []
        for token_list in row:
            new_row.append(' '.join(token.value for token in token_list))
        value_rows.append(new_row)
    return parsing.Table(value_rows, header_idxs=header_idxs)


class AsciiSurface(parsing.InputParser):

    freeform = True

    def isspace_for_tokenization(self, i, x):
        return x.isspace()

    def force_clear_awaiting_for_tokenization(self, i, x):
        return False

    def before_tokenization(self):
        pass

    def _init(self, user_input):
        # skip empty/whitespace lines
        self.lines = []
        for l in user_input.lines:
            #log.debug('checking line %r', l)
            l = util.expand_tabs(l, TABSTOP)
            l = l.replace('\r', '')
            to_tokenize = self.line_for_tokenization(l)
            #log.debug('Checking to_tokenize %r', to_tokenize)
            if not to_tokenize:
                continue
            self.lines.append(l)

        def skip_line(l):
            if not l:
                return True
            if l.isspace():
                return True
            return False
        self.lines = util.strip(self.lines, skip_line, from_start=True, from_end=False)

        if not self.lines:
            raise parsing.NotParsable('No nonempty lines')
        
        if log.isEnabledFor(logging.DEBUG):
            log.debug('original lines:\n%s', '\n'.join(self.lines))

        self.rewrite_lines()

        if log.isEnabledFor(logging.DEBUG):
            log.debug('ascii lines:\n%s', '\n'.join(self.lines))

        self.max_line_len = max(len(line) for line in self.lines)
        self.header_linenos = None

        self.before_tokenization()

        self.skipped_linenos = set()
        self.tokens_by_lineno = []
        for lineno, line in enumerate(self.lines):
            to_tokenize = self.line_for_tokenization(line)
            tokens = list(tokensmod.tokenize(lineno,
                                             to_tokenize,
                                             f_isspace=self.isspace_for_tokenization,
                                             handle_matches=False,
                                             f_force_clear_awaiting=self.force_clear_awaiting_for_tokenization))
            self.tokens_by_lineno.append(tokens)
            if not tokens:
                self.skipped_linenos.add(lineno)
        #if log.isEnabledFor(logging.DEBUG):
        #    log.debug('tokens_by_lineno:\n%s', pprint.pformat(self.tokens_by_lineno))

        self.header_specs = None

    def rewrite_lines(self):
        pass

    def line_for_tokenization(self, line):
        return line

    def parse(self, user_input):
        self._init(user_input)
        self.header_specs = self.compute_header_specs()
        #return self._off_match()
        return self._optimistic_match()

    def compute_header_specs(self):
        raise NotImplementedError()

    def _optimistic_match(self):
        rows = []
        def match_score(token, spec):
            if token.start > spec.col_start:
                count_from = token.start
            else:
                count_from = spec.col_start
            if token.end < spec.col_end:
                count_to = token.end
            else:
                count_to = spec.col_end
            common = count_to - count_from
            if common < 0:
                common = 0
            common_ratio = common / (token.end-token.start)
            distance = abs( ((token.start + token.end) / 2) - ((spec.col_end + spec.col_start) / 2) )
            return common_ratio, -distance
        for lineno, line_tokens in enumerate(self.tokens_by_lineno):
            if lineno in self.skipped_linenos:
                continue
            row = [[] for _ in xrange(len(self.header_specs))]
            for token in line_tokens:
                spec_scores = [match_score(token, spec) for spec in self.header_specs]
                best_spec_idx = max(xrange(len(spec_scores)), key=lambda i: spec_scores[i])
                #log.debug('token %s best %s spec_scores %s', token, best_spec_idx, zip(self.header_specs, spec_scores))
                row[best_spec_idx].append(token)
            rows.append(row)
        #log.debug('Token rows:\n%s', pprint.pformat(rows))
        if self.header_linenos is not None:
            header_idxs = util.translate_indexes_after_deletion(self.header_linenos, self.skipped_linenos)
        else:
            header_idxs = None
        return parsing_result_from_token_rows(rows, header_idxs)

    def _select_specs_by_column(self, ranked_specs):
        by_column = [None] * self.max_line_len
        for spec in ranked_specs:
            if any(by_column[pos] is not None for pos in xrange(spec.col_start, spec.col_end)):
                continue
            for pos in xrange(spec.col_start, spec.col_end):
                by_column[pos] = spec
        uniq = util.uniq_sameorder(by_column)
        uniq = [x for x in uniq if x is not None]
        return uniq


class SpaceAlignedTableParser(AsciiSurface):
    """A parser of space-aligned text tables, often printed by Linux utilities, like ``df``.
    """

    def _init(self, user_input):
        AsciiSurface._init(self, user_input)
        if '  ' in user_input.input_string:
            self.multiplier = 1.5

    def line_for_tokenization(self, line):
        if all(c.isspace() or c in FRAMECHARS for c in line):
            return None
        return line

    def _discover_header_spec_candidates(self):
        specs_by_lineno = []
        for lineno, line_tokens in enumerate(self.tokens_by_lineno[:LINES_FOR_COLUMNSPEC_DISCOVERY]):
            specs_for_line = []

            def extended_column_specs(left_token_no, right_token_no):
                assert left_token_no >= 0

                if left_token_no == 0:
                    left_limit = 0
                else:
                    left_limit = line_tokens[left_token_no-1].end + 1

                if right_token_no == len(line_tokens) - 1:
                    right_limit = len(self.lines[lineno])
                else:
                    right_limit = line_tokens[right_token_no+1].start - 1

                ret = []
                for start in xrange(left_limit, line_tokens[left_token_no].start+1):
                    for end in xrange(line_tokens[right_token_no].end, right_limit+1):
                        if start == line_tokens[left_token_no].start:
                            ret.append(ColumnSpec(start, end, CALIGN_LEFT))
                        if end == line_tokens[right_token_no].end:
                            ret.append(ColumnSpec(start, end, CALIGN_RIGHT))
                return ret

            def spaces_between_tokens(fst_token_no, snd_token_no):
                return all(c == ' ' for c in self.lines[lineno][line_tokens[fst_token_no].end : \
                                                                line_tokens[snd_token_no].start])

            def distance_between_tokens(fst_token_no, snd_token_no):
                return line_tokens[snd_token_no].start - line_tokens[fst_token_no].end

            for i, token in enumerate(line_tokens):
                specs_for_token = []

                specs_for_token.extend(extended_column_specs(i, i))

                # specs coming from joining tokens divided by a single space
                for left_token_no in reversed(xrange(0, i)):
                    if spaces_between_tokens(left_token_no, left_token_no+1) and distance_between_tokens(left_token_no, left_token_no+1) == 1:
                        specs_for_token.extend(extended_column_specs(left_token_no, i))
                    else:
                        break

                specs_for_line.append(specs_for_token)

            specs_by_lineno.append(specs_for_line)

        #log.debug('Specs by lineno:\n%s', pprint.pformat(list(enumerate(specs_by_lineno))))
        return specs_by_lineno

    def _rewrite_last_column(self, hs_candidates):
        for lineno, specs_for_line in enumerate(hs_candidates):
            if specs_for_line:
                for spec in specs_for_line[-1]:
                    if spec.col_end == len(self.lines[lineno]):
                        spec.col_end = self.max_line_len

    def compute_header_specs(self):
        hs_candidates = self._discover_header_spec_candidates()
        self._rewrite_last_column(hs_candidates)
        hs_counts = defaultdict(int)
        for specs_for_line in hs_candidates:
            for specs_for_token in specs_for_line:
                for spec in specs_for_token:
                    hs_counts[spec] += 1
        hs_ranking = sorted(hs_counts.items(), key=\
                            lambda (hs, count): (-count,
                                                 hs.col_end - hs.col_start,
                                                 hs.calign,
                                                 hs.align_column()))
        #if log.isEnabledFor(logging.DEBUG):
        #    log.debug('hs_ranking:\n%s', pprint.pformat(hs_ranking))

        header_specs = self._select_specs_by_column(x[0] for x in hs_ranking)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('header_specs:\n%s', pprint.pformat(header_specs))
        if len(header_specs) <= 1:
            raise parsing.NotParsable('Not enough header specs')
        return header_specs


class OracleSqlplusRewriter(object):

    def __init__(self, lines):
        self.lines = lines[:]

    def _divider_idxs(self, l):
        return [i for i in xrange(len(l)) if l[i] == ' ' and i < len(l)-1]

    def _is_header_line(self, idx):
        if idx >= len(self.lines) - 1:
            return False
        below_line = self.lines[idx+1]
        if len(below_line) < len(self.lines[idx]):
            return False
        if '|' in self.lines[idx] or '|' in below_line:
            return False
        below_words = below_line.split(' ')
        for w in below_words:
            if not all(c == '-' for c in w):
                return False
            if len(w) < 4:
                return False
        return True

    def rewrite(self):
        starting_header_lines_idxs = []
        i = 0
        while True:
            if not self._is_header_line(i):
                break
            starting_header_lines_idxs.append(i)
            i += 2

        if not starting_header_lines_idxs:
            return self.lines

        header_lens = [len(self.lines[i+1]) for i in starting_header_lines_idxs]

        def prepare_to_extend(i):
            self.lines[i] += ' ' * (len(self.lines[i+1])-len(self.lines[i]))
            self.lines[i] += ' '
            self.lines[i+1] += ' '

        prepare_to_extend(0)
        while True:
            if len(starting_header_lines_idxs) == 1:
                break
            prepare_to_extend(2)
            self.lines[0] += self.lines[2]
            self.lines[1] += self.lines[3]
            del self.lines[2]
            del self.lines[2]
            starting_header_lines_idxs.pop()

        to_remove = []
        for i in xrange(2, len(self.lines)):
            if self._is_header_line(i):
                to_remove.append(i)
                to_remove.append(i+1)
        self.lines = util.remove_indexes(self.lines, to_remove)

        joined = self.lines[:2]
        for chunk in util.chunks(self.lines[2:], len(header_lens), lambda: ''):
            log.debug('chunk: %r', chunk)
            chunk = [ch.ljust(header_lens[i]) for i, ch in enumerate(chunk)]
            joined.append(' '.join(chunk))
        self.lines = joined

        if log.isEnabledFor(logging.DEBUG):
            log.debug('before divider:\n%s', '\n'.join(self.lines))
        divider_idxs = self._divider_idxs(self.lines[1])
        for i in xrange(len(self.lines)):
            c = 0
            for didx in divider_idxs:
                didx += c*2
                self.lines[i] = util.change_at(self.lines[i], didx, ' | ')
                c += 1

        return self.lines


class AsciiTableParser(AsciiSurface):
    """A parser of ASCII tables drawn with the ``| + = -`` characters, printed by command line programs like ``psql``."""

    multiplier = 2.0

    def isspace_for_tokenization(self, i, x):
        if x.isspace():
            return True
        if x in FRAMECHARS and i in self.original_vertical:
            return True
        return False

    def force_clear_awaiting_for_tokenization(self, i, x):
        if i in self.original_vertical:
            return True
        return False

    def before_tokenization(self):
        self._compute_vertical()
        self._compute_horizontal()
        log.debug('horizontal: %s', self.horizontal)
        log.debug('vertical: %s', self.vertical)


    def rewrite_lines(self):
        try:
            self.lines = OracleSqlplusRewriter(self.lines).rewrite()
        except:
            log.exception('While rewrite_lines(), ignoring')

    #def line_for_tokenization(self, line):
    #    return RE_ANY_FRAMECHAR.sub(' ', line)

    def _compute_horizontal(self):
        self.horizontal = []
        for lineno, line in enumerate(self.lines):
            stripped = line.strip()
            if len(stripped) in (0, 1):
                continue
            if not all(c in FRAMECHARS or c.isspace() for c in stripped):
                continue
            count = util.count(c for c in stripped if c in FRAMECHARS)
            count_ratio = count / len(stripped)
            if count_ratio >= 0.5:
                self.horizontal.append(lineno)

        horizontal_after_content = None
        if self.horizontal:
            content_seen = False
            for i in xrange(MAX_HORIZONTAL_LINENO_AS_BOTTOM_HEADER_BORDER + 1):
                if content_seen and i in self.horizontal:
                    horizontal_after_content = i
                    break
                if i not in self.horizontal:
                    content_seen = True
                    continue

        log.debug('horizontal_after_content %s', horizontal_after_content)

        # check for header
        if horizontal_after_content is not None:
            linenos_with_content = [lineno for lineno, line in enumerate(self.lines[:horizontal_after_content])
                                    if util.has_content(line)]
            if linenos_with_content:
                self.header_linenos = linenos_with_content

    class Linerange(namedtuple('Linerange', 'columnno len')):
        pass

    def _compute_vertical(self):
        lineranges = []
        columnnos_with_content = []
        for columnno in xrange(0, self.max_line_len+1):
            column = [line[columnno] if columnno < len(line) else None for line in self.lines]
            if any(c is not None and c.isalnum() for c in column):
                columnnos_with_content.append(columnno)
            without_ends = util.strip(column, lambda c: c not in FRAMECHARS)
            if len(without_ends) in (0, 1):
                continue
            if any(c not in FRAMECHARS for c in without_ends):
                continue

            vertical_framechars_count = util.count(c for c in without_ends if c in FRAMECHARS_VERTICAL)
            if len(without_ends) == 2 and vertical_framechars_count == 1:
                pass
            elif vertical_framechars_count in (0, 1):
                continue

            lineranges.append(self.Linerange(columnno, len(without_ends)))
        if not lineranges:
            self.vertical = []
            self.original_vertical = []
            self.multiplier = 1.0
            return
        log.debug('lineranges %s', lineranges)
        min_valid_vertical = columnnos_with_content[0] + 1 if columnnos_with_content else 0
        max_valid_vertical = columnnos_with_content[-1] if columnnos_with_content else self.max_line_len

        max_linerange_len = max(x.len for x in lineranges)
        def linerange_len_valid(l):
            if l <= 3:
                return l >= max_linerange_len
            return l+2 >= max_linerange_len
        long_lineranges = [lr for lr in lineranges if linerange_len_valid(lr.len)]

        # for use in _filter_tokens
        self.original_vertical = [lr.columnno for lr in long_lineranges]
        log.debug('original_vertical: %s', self.original_vertical)

        selected_lineranges = [lr for lr in long_lineranges
                               if linerange_len_valid(lr.len) \
                               and min_valid_vertical <= lr.columnno <= max_valid_vertical]
        self.vertical = [x.columnno for x in selected_lineranges]

        # Remove starting and ending columnnos if they don't divide any content
        if self.vertical and columnnos_with_content:
            if self.vertical[0] < columnnos_with_content[0]:
                del self.vertical[0]

        # Multiplier is based on how long the found verticals are
        if not self.vertical:
            self.multiplier = 1.0
        else:
            avg_fill_factor = util.avg(x.len / len(self.lines) for x in selected_lineranges)
            fill_factor_without_one = (len(self.lines)-1) / len(self.lines)
            if avg_fill_factor >= fill_factor_without_one:
                self.multiplier = 2.0
            else:
                self.multiplier = 1.5 * avg_fill_factor

    def _filter_tokens(self):
        if log.isEnabledFor(logging.DEBUG):
            log.debug('tokens before filtering: %s', pprint.pformat(self.tokens_by_lineno))
        def is_token_valid(token):
            if token.lineno in self.horizontal:
                return False
            if any(token.contains_idx(v) for v in self.original_vertical):
                return False
            return True

        self.tokens_by_lineno = [[t for t in tokens if is_token_valid(t)]
                                 for tokens in self.tokens_by_lineno]
        for lineno, line_tokens in enumerate(self.tokens_by_lineno):
            if not line_tokens:
                self.skipped_linenos.add(lineno)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('tokens after filtering: %s', pprint.pformat(self.tokens_by_lineno))

    def compute_header_specs(self):
        if not self.vertical and not self.horizontal:
            raise parsing.NotParsable('No frames detected')
        if not self.vertical:
            self.vertical = [self.max_line_len-1]

        self._filter_tokens()

        header_specs = []
        if self.vertical:
            prev = -1
            for columnno in self.vertical:
                header_specs.append(ColumnSpec(prev+1, columnno, CALIGN_CENTER))
                prev = columnno

        if any(self.lines[lineno][self.vertical[-1]:].strip().strip(''.join(FRAMECHARS)) \
               for lineno in xrange(len(self.lines)) if lineno not in self.horizontal):
            header_specs.append(ColumnSpec(self.vertical[-1], self.max_line_len, CALIGN_CENTER))

        if log.isEnabledFor(logging.DEBUG):
            log.debug('header_specs:\n%s', pprint.pformat(header_specs))
        return header_specs

