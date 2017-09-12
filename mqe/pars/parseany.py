from __future__ import division

import logging
from collections import OrderedDict
import time

from mqe.pars import parsing
from mqe.pars import asciiparsing
from mqe.pars import basicparsing
from mqe.pars import enrichment
from mqe import util


log = logging.getLogger('mqe.pars.parseany')


INPUT_PARSERS = OrderedDict()

def register_input_parsers(input_type, *input_parser_cls_list):
    """Register the given input parser classes as implementing parsing the given input type.

    :param str input_type: an input type
    :param input_parser_cls_list: :class:`~mqe.pars.parsing.InputParser` subclasses that
        implement parsing the input type
    """
    global INPUT_PARSERS
    if input_type not in INPUT_PARSERS:
        INPUT_PARSERS[input_type] = []
    INPUT_PARSERS[input_type].extend(input_parser_cls_list)


register_input_parsers('any',
    basicparsing.JsonDeepParser,
    basicparsing.JsonParser,
    basicparsing.SingleValueParser,
    basicparsing.CSVParser,
    asciiparsing.SpaceAlignedTableParser,
    asciiparsing.AsciiTableParser,
    basicparsing.PropertiesParser,
    basicparsing.KeyValueParser,
    basicparsing.MultipleValuesParser,
)

register_input_parsers('json',
    basicparsing.JsonDeepParser,
    basicparsing.JsonParser,
)

register_input_parsers('jsonraw',
    basicparsing.JsonParser,
)

register_input_parsers('csv',
   basicparsing.CSVParser,
)

register_input_parsers('ascii',
    basicparsing.SingleValueParser,
    asciiparsing.SpaceAlignedTableParser,
    asciiparsing.AsciiTableParser,
)

register_input_parsers('asciitable',
    basicparsing.SingleValueParser,
    asciiparsing.AsciiTableParser,
)

register_input_parsers('asciispace',
    basicparsing.SingleValueParser,
    asciiparsing.SpaceAlignedTableParser,
)

register_input_parsers('props',
    basicparsing.PropertiesParser,
    basicparsing.KeyValueParser,
)

register_input_parsers('tokens',
    basicparsing.MultipleValuesParser,
)

register_input_parsers('single',
   basicparsing.OneCellParser,
)

register_input_parsers('_supporting_delimiter',
    basicparsing.CSVParser,
    basicparsing.MultipleValuesParser,
)


class ParsingResult(object):
    """A result returned by the :func:`parse_input` function.

    .. attribute:: table

        the final resulting :class:`~mqe.pars.parsing.Table`, selected from
        multiple candidates if more than one parser was used to parse an input. ``None`` if
        no parser was able to parse the input.

    .. attribute:: result_by_input_parser

        a dict mapping an :class:`~mqe.pars.parsing.InputParser` object to the resulting
        :class:`~mqe.pars.parsing.Table`

    .. attribute:: best_input_parser

        an :class:`~mqe.pars.parsing.InputParser` object whose result was selected as
        the final result

    In addition, the arguments to the :func:`parse_input` function ``input_string``, ``input_type`` and
    ``ip_options`` are also available as instance variables.
    """

    def __init__(self):
        self.table = None

        self.result_by_input_parser = OrderedDict()
        self.status_by_input_parser = OrderedDict()
        self.score_by_input_parser = OrderedDict()
        self.best_input_parser = None

        self.input_string = None
        self.input_type = None
        self.ip_options = None

        self._started = time.time()
        self._finished = None

        self._result = None
        self.info = {}

    def finish(self):
        self._finished = time.time()

    @property
    def took(self):
        if self._started is None or self._finished is None:
            return None
        return self._finished - self._started

    @property
    def elapsed(self):
        if self._started is None:
            return None
        return time.time() - self._started


def table_score(table):
    s = 0
    # relying on header computation
    rows_to_check = table.value_rows or table.header_rows or []
    num_cells = table.num_columns * len(rows_to_check)
    if num_cells == 0:
        return 0
    for row in rows_to_check:
        for ev in row:
            ev_score = enrichment.score_for_enriched_value(ev)
            #log.debug('ev_score %s for %s', ev_score, ev)
            s += ev_score
    s = max(s, 0)
    avg = s / num_cells
    #res = math.sqrt(num_cells) * avg
    return avg


def choose_best_result(parsing_result):
    if not parsing_result.result_by_input_parser:
        return

    score_by_ip = OrderedDict()
    first_ip = util.first(parsing_result.result_by_input_parser.iterkeys())

    # Special case for MULTIPLIER_SURE
    if len(parsing_result.result_by_input_parser) == 1 and first_ip.multiplier == parsing.MULTIPLIER_SURE:
        score_by_ip[first_ip] = (parsing.MULTIPLIER_SURE, parsing.MULTIPLIER_SURE, parsing.MULTIPLIER_SURE)
    else:
        # Default case when computing scores is needed
        for ip, enriched_table in parsing_result.result_by_input_parser.items():
            log.debug('Computing score for %r', ip)
            try:
                s = table_score(enriched_table)
            except:
                log.exception('While computing score for %r', enriched_table)
                s = -1
            score_by_ip[ip] = (s, ip.multiplier, s * ip.multiplier)
    log.debug('All scores: %s', score_by_ip)

    best_ip = max(score_by_ip, key=lambda ip: score_by_ip[ip][2])
    log.debug('Best input parser: %s', best_ip)

    parsing_result.table = parsing_result.result_by_input_parser[best_ip]
    parsing_result.score_by_input_parser = score_by_ip
    parsing_result.best_input_parser = best_ip


def _postprocess_ip_options(ip_options):
    if ip_options.get('delimiter'):
        if isinstance(ip_options['delimiter'], unicode):
            try:
                ip_options['delimiter'] = ip_options['delimiter'].encode('utf-8')
            except:
                raise parsing.InvalidInput()


def parse_input(input_string, input_type='any', ip_options={}):
    """The high-level entry function for parsing a string input ``input_string`` into a
    :class:`~mqe.pars.parsing.Table`.

    The ``input_type`` defines an input type:

    * ``any`` - the input is of unspecified type
    * ``json`` - a JSON document that should be possibly unnested
    * ``jsonraw`` - a JSON document (with no unnesting)
    * ``csv`` - a CSV file
    * ``ascii`` - a space- or ascii-characters-aligned table
    * ``asciitable`` - an ascii-characters-aligned table
    * ``asciispace`` - a space-aligned table
    * ``props`` - each line defines a property name and a property value
    * ``tokens`` - each token (a word) is treated as a table cell
    * ``markdown`` - the input is in Markdown format and will be converted to a single-cell table
    * ``single`` - the whole input will be converted to a single-cell table

    Additional values of ``input_type`` are supported if parsers were registered using :func:`register_input_parsers`.

    :param str|unicode input_string: input data
    :param str input_type: input type (see above)
    :param dict ip_options: Extra options for the parsers. See
        :class:`~mqe.pars.parsing.InputParser`.
    :return: a :class:`ParsingResult` containing the parsed table
    """
    res = ParsingResult()
    res.input_string = input_string
    res.input_type = input_type
    res.ip_options = ip_options

    try:
        _postprocess_ip_options(ip_options)
        user_input = parsing.UserInput(input_string)
        filtered_user_input = basicparsing.FilteredUserInput(user_input)
        log.debug('filtered input_string=%r', filtered_user_input.input_string)
        log.debug('filtered lines=%r', filtered_user_input.lines)
    except parsing.InvalidInput:
        log.warn('parse_input got input non-encodable as UTF-8')
        return res

    input_parser_list = INPUT_PARSERS[input_type]
    if ip_options.get('delimiter'):
        input_parser_list = [ip for ip in input_parser_list if ip in INPUT_PARSERS['_supporting_delimiter']]

    enrichment._SCORE_FOR_VALUE_EV_CACHE.clear()

    for input_parser_cls in input_parser_list:
        try:
            input_parser = input_parser_cls(ip_options)
            log.debug('Starting parsing using %r', input_parser)
            if input_parser.freeform:
                input_object = filtered_user_input
            else:
                input_object = user_input

            table = input_parser.parse(input_object)

            if input_parser.multiplier != parsing.MULTIPLIER_SURE:
                if not table or not table.rows or (len(table.rows) == 1 and not table.rows[0]):
                    raise parsing.NotParsable('Invalid table: %r' % table)

            enriched_table = enrichment.EnrichedTable(table)

            res.status_by_input_parser[input_parser] = 'success'
            res.result_by_input_parser[input_parser] = enriched_table
            log.debug('Resulting table:\n%s', enriched_table)

            if input_parser.multiplier == parsing.MULTIPLIER_SURE:
                break
        except parsing.NotParsable as e:
            res.status_by_input_parser[input_parser] = 'notparsed'
            log.debug('Not parsable by %r: %s', input_parser, e)
        except:
            res.status_by_input_parser[input_parser] = 'error'
            log.exception('When parsing using %r, skipping this parser', input_parser)
        log.debug('Finished parsing using %r', input_parser)

    choose_best_result(res)

    res.finish()

    if not res.table:
        log.debug('Not parsable by any parser')

    return res


if __name__ == '__main__':
    import sys
    from mqe import serialize

    input_string = sys.stdin.read()
    parsing_result = parse_input(input_string)
    if not parsing_result.table:
        sys.exit(1)
    d = {
        'header': parsing_result.table.header_idxs,
        'rows': [[cell.to_string_key() for cell in row] for row in parsing_result.table.rows],
    }
    print serialize.json_dumps_external(d)

