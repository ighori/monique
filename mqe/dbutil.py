import uuid

from mqe import util, serialize



def gen_uuid():
    """Generate a random :class:`uuid.UUID` (version 4)"""
    return uuid.uuid4()


def gen_timeuuid():
    """Generate a time-:class:`uuid.UUID` (version 1)"""
    return uuid.uuid1()



class Row(object):
    """A class that wraps a database row (a dictionary). It's used to abstract the access
    to rows returned by the DAO classes.

    The class acts like a dictionary allowing accessing the row's values with ``__getitem__``,
    ``__contains__`` and ``get()``. The original row is available as the ``row`` attribute.

    The public attributes of the dictionaries should be defined as the class:`Row` subclass'
    class attributes of type :class:`Column`.

    :param dict row: the database row to wrap

    """

    #: A tuple of the row's keys to skip when calling ``str(row)`` or ``repr(row)``
    skip_printing = tuple()

    def __init__(self, row):
        self.row = row

    def __getitem__(self, attr):
        return self.row[attr]

    def __setitem__(self, attr, val):
        self.row[attr] = val

    def __contains__(self, item):
        return self.row.__contains__(item)

    def get(self, *args, **kwargs):
        return self.row.get(*args, **kwargs)

    def key(self):
        """Return the key of this row (must be a hashable object) that should be used for the ``__hash__`` and ``__cmp__`` methods. The method can be left not implemented when hashing and comparisons are not used."""
        raise NotImplementedError()

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, util.dictwithout(self.row, *self.skip_printing))
    __repr__ = __str__

    def __hash__(self):
        return hash(self.key())

    def __cmp__(self, other):
        if not isinstance(other, Row):
            return -1
        return cmp(self.key(), other.key())


class Column(object):
    """A property that can be set as a :class:`Row` subclass attribute that gets the
    given column from the wrapped dictionary (database row).

    :param str column: the name of a dictionary key holding the column's value
    :param default: the zero-argument function returning a value used
        when a dictionary doesn't have the key
    """

    def __init__(self, column, default=lambda: None):
        self.column = column
        self.default = default

    def __get__(self, obj, objtype):
        if self.column not in obj.row:
            return self.default()
        return obj.row[self.column]

    def __set__(self, obj, value):
        raise AttributeError('Readonly')


class TextColumn(Column):
    """A column which type is :class:`str` or :class:`unicode`"""
    pass


class ListColumn(Column):
    """A column which type is :class:`list`"""
    pass


class UUIDColumn(Column):
    """A column which type is a random :class:`~uuid.UUID`"""
    pass


class TimeUUIDColumn(Column):
    """A column which type is a time :class:`~uuid.UUID`"""
    pass


class JsonColumn(Column):
    """A column which type is a JSON-serializable value (a list, a dict, a number).
    The class loads JSON content from a dictionary on first use.
    The ``del`` statement can be used to clear the loaded content.

    """

    def __init__(self, column, default=lambda: None):
        self.column = column
        self.json_value_prop = '_json_column_%s' % column
        self.default = default

    def __get__(self, obj, objtype):
        if not hasattr(obj, self.json_value_prop):
            raw_val = obj[self.column]
            if raw_val is None:
                setattr(obj, self.json_value_prop, self.default())
            else:
                setattr(obj, self.json_value_prop, serialize.json_loads(raw_val))
        return getattr(obj, self.json_value_prop)

    def __set__(self, obj, value):
        raise AttributeError('Readonly')

    def __delete__(self, obj):
        if hasattr(obj, self.json_value_prop):
            delattr(obj, self.json_value_prop)