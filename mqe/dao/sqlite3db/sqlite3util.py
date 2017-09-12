import sqlite3
import _sqlite3
import uuid
import logging
import time

from mqe import mqeconfig
from mqe import c


log = logging.getLogger('mqe.dao.sqlite3')


TIMEOUT = 5


def adapt_uuid(u):
    if u.version == 1:
        return '%s;%s' % (u.time, u.hex)
    else:
        return u.hex

def convert_uuid(s):
    if ';' in s:
        return uuid.UUID(s[s.rfind(';')+1:])
    return uuid.UUID(s)

def adapt_strset(lst):
    if not lst:
        return ''
    return ','.join(sorted(lst))

def convert_strset(s):
    if not s:
        return []
    return sorted(s.split(','))


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def connect():
    sqlite3.register_adapter(uuid.UUID, adapt_uuid)
    sqlite3.register_converter('uuid', convert_uuid)
    sqlite3.register_converter('timeuuid', convert_uuid)

    sqlite3.register_adapter(list, adapt_strset)
    sqlite3.register_converter('strset', convert_strset)

    c.sqlite3 = sqlite3.connect(mqeconfig.SQLITE3_DATABASE,
                                timeout=TIMEOUT,
                                detect_types=sqlite3.PARSE_DECLTYPES)
    c.sqlite3.row_factory = dict_factory

    with closing_cursor() as cur:
        cur.execute("""PRAGMA case_sensitive_like = true;""")


class LoggingCursorWrapper(object):

    def __init__(self, cursor):
        self.cursor = cursor

    def _simplify_sql(self, sql):
        return ' '.join(sql.split())

    def execute(self, sql, params=[]):
        if not mqeconfig.DEBUG_QUERIES:
            return self.cursor.execute(sql, params)

        start = time.time()
        log.info('execute SQL %r %r', self._simplify_sql(sql), params)
        res = self.cursor.execute(sql, params)
        log.info('Finished (%.1f)', (time.time() - start) * 1000)
        return res

    def executemany(self, sql, many_params):
        if not mqeconfig.DEBUG_QUERIES:
            return self.cursor.executemany(sql, many_params)

        log.info('executemany SQL %r %r', self._simplify_sql(sql), many_params)
        start = time.time()
        res = self.cursor.executemany(sql, many_params)
        log.debug('Finished (%.1f)', (time.time() - start) * 1000)
        return res

    def __getattr__(self, name):
        return getattr(self.cursor, name)


class closing_cursor(object):
    def __init__(self, commit=True):
        self.commit = commit
        self._real_cursor = c.sqlite3.cursor()
        self.cursor = LoggingCursorWrapper(self._real_cursor)
    def __enter__(self):
        return self.cursor
    def __exit__(self, *exc_info):
        self._real_cursor.close()
        if self.commit:
            c.sqlite3.commit()

def _into_statement(op, table, columns_values):
    """A shortcut for creating an ``INSERT`` statement.

    :param str table: a table name to insert into
    :param dict columns_values: a dict mapping the table's column names to values to insert
    :return: the ``INSERT`` statement
    """
    items = sorted(columns_values.items(), key=lambda item: item[0])
    qs = """{op} INTO {table} ({columns}) VALUES ({qmarks})""".format(
        op=op,
        table=table,
        columns=', '.join(item[0] for item in items),
        qmarks=', '.join('?' * len(items)))
    return qs, [item[1] for item in items]

def insert(table, columns_values):
    return _into_statement('INSERT', table, columns_values)


def replace(table, columns_values):
    return _into_statement('REPLACE', table, columns_values)

def in_params(lst):
    return '(%s)' % ', '.join('?' * len(lst))
