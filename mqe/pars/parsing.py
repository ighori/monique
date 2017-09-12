import logging

from mqe import util
from mqe.util import cached_property
from mqe import mqeconfig


log = logging.getLogger('mqe.pars.parsingbase')


#: Indicates that a header of table was detected automatically
HEADER_IDXS_SOURCE_DETECTION = 1
#: Indicates that a header of a table was set explicitly by a user
HEADER_IDXS_SOURCE_USER = 2

#: A :attr:`InputParser.multiplier` indicating that a parser is sure that it correctly
#: detected and parsed an input.
MULTIPLIER_SURE = 1000000


class NotParsable(Exception):
    """An exception raised by a parser when it's unable to parse the given input.
    """

class InvalidInput(Exception):
    """An exception raised when the input to parse is not UTF-8-encodable"""


class Table(object):
    """A table with an optional header and ignored rows.
      Returned by parsers as a parsing result.
    """

    def __init__(self, rows=[], header_idxs=None, ignored_idxs=None, header_idxs_source=None):
        #: a list of rows. Each row is a list of cells (of any type).
        #: If the number of cells in each row is not the same, the shorter rows
        #: are filled with an empty string.
        self.rows = rows
        #: a list of indexes of rows that form the table's header
        self.header_idxs = header_idxs
        #: a list of indexes of rows that should be ignored
        self.ignored_idxs = ignored_idxs
        #: tells how the header was set. If specified, must be either
        #: :data:`HEADER_IDXS_SOURCE_DETECTION` or :data:`HEADER_IDXS_SOURCE_USER`
        self.header_idxs_source = header_idxs_source

        self._assure_same_col_numbers()

    def _assure_same_col_numbers(self):
        if not self.rows:
            return
        cols = max(len(row) for row in self.rows)
        for row in self.rows:
            to_fill = cols - len(row)
            if to_fill > 0:
                row.extend([''] * to_fill)

    @property
    def num_rows(self):
        """The number of rows"""
        return len(self.rows)

    @property
    def value_idxs(self):
        """Indexes of value rows - non-ignored and not forming a header."""
        return [i for i in xrange(len(self.rows)) \
                if i not in (self.header_idxs or []) \
                and i not in (self.ignored_idxs or [])]

    @property
    def value_or_other_idxs(self):
        """Indexes of preferably value rows. If they are not available, indexes of header rows. If they are not available, indexes of the whole table."""
        value_idxs = self.value_idxs
        if value_idxs:
            return value_idxs
        header_idxs = self.header_idxs
        if header_idxs:
            return header_idxs
        return range(self.num_rows)

    @property
    def nonignored_idxs(self):
        """Indexes of non-ignored rows"""
        return [i for i in xrange(len(self.rows)) \
                if i not in (self.ignored_idxs or [])]

    @property
    def header_rows(self):
        """The rows forming the header"""
        return [self.rows[i] for i in self.header_idxs or []]

    @property
    def has_header(self):
        """A bool telling if the table has a header"""
        return bool(self.header_idxs)

    @property
    def nonignored_rows(self):
        """Non-ignored rows"""
        return [self.rows[i] for i in self.nonignored_idxs or []]

    @property
    def value_rows(self):
        """Value rows (see :attr:`value_idxs`)"""
        return [self.rows[i] for i in self.value_idxs or []]

    @property
    def num_columns(self):
        """The number of columns of this table"""
        if not self.rows:
            return None
        return len(self.rows[0])

    def value_column(self, colno):
        """The column of the table with the index ``colno`` formed from :attr:`value_rows`.

        :return: a list of tuples ``(rowno, value)``.
        """
        res = []
        for i in self.value_idxs:
            res.append((i, self.rows[i][colno]))
        return res

    @classmethod
    def one_cell_table(cls, value):
        """A shortcut for creating a table with a single cell ``value`` and no header.

        :return: a :class:`Table` instance
        """
        return Table(rows=[[value]], header_idxs=[])

    def __unicode__(self):
        return u'%s(%s, header_idxs=%s, ignored_idxs=%s)' % (self.__class__.__name__, util.format_rows(self.rows[:10]) if self.rows else None, self.header_idxs, self.ignored_idxs)
    __repr__ = __unicode__
    def __cmp__(self, other):
        if not isinstance(other, Table):
            return -1

        return cmp((self.rows, self.header_idxs, self.ignored_idxs), (other.rows, other.header_idxs, other.ignored_idxs))



def from_colno_colname_values(rows):
    colno_by_colname = {}
    next_colno = 0
    for row in rows:
        for colno, colname, value in row:
            if colname is not None:
                saved_colno = colno_by_colname.get(colname)
                if saved_colno is None:
                    saved_colno = next_colno
                    next_colno += 1
                    colno_by_colname[colname] = saved_colno
    max_colno = max([next_colno-1] + [colno or 0 for row in rows for colno, colname, value in row])
    if max_colno+1 > mqeconfig.MAX_REPORT_COLUMNS:
        raise NotParsable('Too many columns')

    res_rows = []

    if colno_by_colname:
        header_row = [None] * (max_colno+1)
        for header_colname, header_colno in colno_by_colname.items():
            header_row[header_colno] = header_colname
        res_rows.append(header_row)
        header_idxs = [0]
    else:
        header_idxs = None

    for row in rows:
        res_row = [None] * (max_colno+1)
        for colno, colname, value in row:
            if colname is not None:
                actual_colno = colno_by_colname[colname]
            else:
                actual_colno = colno
            res_row[actual_colno] = value
        res_rows.append(res_row)

    return {'rows': res_rows, 'header_idxs': header_idxs}


class UserInput(object):
    """A representation of an input string from which parsers can extract a table.

    :param in_content: an input string. It must be an UTF8-encoded :class:`str` or a
        :class:`unicode` object that can be encoded as UTF8.

    .. attribute:: input_string

        the ``in_content`` as an UTF8-encoded :class:`str`
    """

    def __init__(self, in_content):
        if isinstance(in_content, unicode):
            try:
                self.input_string = in_content.encode('utf8')
            except:
                raise InvalidInput()
        else:
            try:
                in_unicode = in_content.decode('utf8')
            except:
                raise InvalidInput()
            else:
                self.input_string = in_content

    @cached_property
    def lines(self):
        """A list of lines forming the ``input_string``"""
        return self.input_string.splitlines()

    @cached_property
    def stripped(self):
        """The ``input_string`` with leading and trailing whitespace removed"""
        try:
            return self.input_string.strip()
        except:
            return self.input_string


class InputParser(object):
    """A parser that can extract a :class:`Table` from an input string.

    :param ip_options: a dictionary containing common options for the parser. The following keys can appear there:

        * ``delimiter`` - a string that was specified by a user as a field delimiter

    """

    #: A score computed for a :class:`Table` returned by the parser will be multiplied by that value. If a parser "thinks" it parsed the input correctly, the value should be increased. It can be set to :data:`MULTIPLIER_SURE`.
    #:
    #: The ``multiplier`` attribute is read as an instance attribute, allowing setting the value for specific inputs.
    multiplier = 1.0

    #: The value should be set to ``True`` for free-form text inputs like text files or ASCII tables. If it's ``True``, the default implementation of :class:`UserInput` is replaced with :class:`~mqe.pars.basicparsing.FilteredUserInput`.
    freeform = False

    def __init__(self, ip_options):
        self.ip_options = ip_options

    @property
    def name(self):
        """A name of the parser which is it's class' name"""
        return self.__class__.__name__

    def parse(self, user_input):
        """Parse the given input into a :class:`Table`.

        :param UserInput user_input: input data
        :return: a :class:`Table`
        :raises NotParsable: when a parser is not able to parse the given input
        """
        raise NotImplementedError()

    def __hash__(self):
        return hash(self.name)

    def __cmp__(self, other):
        if not isinstance(other, InputParser):
            return -1
        return cmp(self.name, other.name)

    def __repr__(self):
        return 'InputParser:%s' % self.name
    __str__ = __repr__

