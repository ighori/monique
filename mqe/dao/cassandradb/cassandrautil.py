import logging
import time
import uuid

import cassandra
import cassandra.cluster
import cassandra.policies
import cassandra.query
import cassandra.query
import cassandra.util
from cassandra import ConsistencyLevel
import datetime

from mqe import mqeconfig
from mqe import util
from mqe.dbutil import Row
from mqe.util import cached_property


log = logging.getLogger('mqe.cassutil')


DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM
FETCH_SIZE = 10000
PARALLEL_QUERIES_CHUNK_SIZE = 1000

NONEXISTING = util.DefaultFrozenDict(lambda: None)


def day_text(dt_or_uuid):
    if isinstance(dt_or_uuid, uuid.UUID):
        dt = util.datetime_from_uuid1(dt_or_uuid)
    else:
        dt = dt_or_uuid

    if dt.year < 2000:
        return '000000'
    return dt.strftime('%y%m%d')

def day_text_from_rid(rid):
    return day_text(cassandra.util.datetime_from_uuid1(rid))

def dt_from_day_text(day):
    """Convert the content of the ``mqe.report_instance.day`` column to a :class:`datetime.datetime`"""
    return datetime.datetime.strptime(day, '%y%m%d')

def bind(qs, vals, consistency_level=DEFAULT_CONSISTENCY_LEVEL):
    """Binds the CQL query ``qs`` (a string) to a list of values ``vals`` using the given consistency level and returns a :class:`cassandra.query.BoundStatement`. The ``qs`` must use the ``?`` character for parameter placeholders."""
    from mqe import c

    q = c.cass._prepare(qs, consistency_level)
    return q.bind(vals)

def insert(table, columns_values, column_renames={}, if_not_exists=False):
    """A shortcut for creating an ``INSERT`` statement.

    :param str table: a table name to insert into
    :param dict columns_values: a dict mapping the table's column names to values to insert
    :param dict column_renames: a dict defining renames of the keys of ``columns_values``
    :param bool if_not_exists: whether to append ``IF NOT EXISTS`` to the statement
    :return: the ``INSERT`` statement bound to the values
    :rtype: cassandra.query.BoundStatement
    """
    cv = columns_values.copy()

    for old_col, new_col in column_renames.items():
        if old_col in cv:
            cv[new_col] = cv[old_col]
            del cv[old_col]

    items = sorted(cv.items(), key=lambda item: item[0])
    qs = """INSERT INTO {table} ({columns}) VALUES ({qmarks}){postfix}""".format(
        table=table,
        columns=', '.join(item[0] for item in items),
        qmarks=', '.join('?' * len(items)),
        postfix=' IF NOT EXISTS' if if_not_exists else '',
    )
    return bind(qs, [item[1] for item in items])

def batch(*queries):
    """A shortcut for creating a :class:`cassandra.query.BatchStatement` from a list of :class:`cassandra.query.BoundStatement` ``queries``"""
    b = cassandra.query.BatchStatement()
    b._mqe_raw_values_list = []
    for q in queries:
        b.add(q)
        if isinstance(q, cassandra.query.BoundStatement):
            b._mqe_raw_values_list.append(q.raw_values)
    return b

def is_nonexisting(row):
    """Check if the row returned by :meth:`Cassandra.execute_fst` is a sentinel *non-existing* row. The ``row`` can be also a :class:`Row` instance created from the sentinel raw."""
    return row is NONEXISTING or (isinstance(row, Row) and row.row is NONEXISTING)

def firstrow(result):
    """Return the first row from the rows returned by :meth:`Cassandra.execute`, ``None`` if there are no rows"""
    if not result:
        return None
    if hasattr(result, 'next'):
        try:
            return result.next()
        except StopIteration:
            return None
    return result[0]

def is_transaction_applied(update_rows):
    """Tells whether a Cassandra lightweight transaction was applied, based on the returned rows"""
    if not update_rows:
        return False
    return update_rows[0].get('[applied]')

def execute_lwt(fun):
    """Execute the function ``fun`` that makes a Cassandra lightweight transaction and returns the result rows and tell whether the transaction was successful. The :func:`execute_lwt` function handles the case when Cassandra doesn't know if the transaction was applied.

    :return: ``True`` if applied, ``False`` if not applied, ``None`` if it's unknown if applied."""
    try:
        update_rows = fun()
        res = is_transaction_applied(update_rows)
        if not res:
            log.debug('LWT not applied: %s', fun)
        return res
    except cassandra.WriteTimeout:
        log.debug('LWT gives WriteTimeout: %s', fun)
        return None


def dict_factory(colnames, rows):
    res = []
    for row in rows:
        d = util.create_small_dict()
        for i, colname in enumerate(colnames):
            d[colname] = row[i]
        res.append(d)
    return res


class Cassandra(object):
    """A class that wraps a connection to the Cassandra database and offers helper methods for executing queries.

    The instance of this class is available as ``mqe.c.cass`` (see :class:`~mqe.context.Context`).
    """

    def __init__(self):
        self.qs_to_prepared = {}
        self.query_id_to_qs = {}

    @cached_property
    def cluster(self):
        """The connection to the database - a :class:`cassandra.cluster.Cluster` instance, initialized from the :attr:`mqeconfig.CASSANDRA_CLUSTER` on the first usage"""
        kwargs = mqeconfig.CASSANDRA_CLUSTER
        if 'reconnection_policy' not in kwargs:
            kwargs['reconnection_policy'] = cassandra.policies.ConstantReconnectionPolicy(delay=5, max_attempts=100000000)
        log.info('Connecting to Cassandra')
        cluster = cassandra.cluster.Cluster(**kwargs)
        #!!! not supported in pypy
        #cluster.connection_class = LibevConnection
        return cluster

    @cached_property
    def session(self):
        """The :class:`cassandra.cluster.Session` object created from the :attr:`cluster` that allows executing queries. The :attr:`session` returns rows as dictionaries (instead of the default namedtuples)."""
        try:
            csession = self.cluster.connect()
        except Exception as e:
            if e.message == 'Cluster is already shut down':
                log.warn('Recreating cluster due to <Cluster is already shut down> exception')
                del self.__dict__['cluster']
                csession = self.cluster.connect()
            else:
                raise
        csession.default_fetch_size = FETCH_SIZE
        csession.row_factory = dict_factory
        return csession

    def _assure_is_prepared(self, query, consistency_level):
        if isinstance(query, basestring):
            return self._prepare(query, consistency_level)
        return query

    def _log_start(self, query, parameters):
        try:
            log.info('CQL_START %r %r', util.simplify_whitespace(self._get_query_string(query)),
                     parameters or self._get_parameters(query))
        except:
            log.exception('When _log_start, ignoring')

    def _log_end(self):
        log.info('CQL_END (%.1f)', (time.time()-self._start)*1000)

    def _postprocess_result(self, result):
        if not result.has_more_pages:
            return result.current_rows
        return iter(result)

    def execute(self, query, parameters=None, consistency_level=DEFAULT_CONSISTENCY_LEVEL):
        """Execute the given query and return a list of rows or an iterator over the rows.

        :param query: an already bound query or a string with ``?`` as a placeholder for parameter values. The query is automatically prepared (it's done once for each unique ``query``)
        :param parameters: a list of values to bind to the query
        :param consistency_level: the :class:`cassandra.ConsistencyLevel` to use
        :return: a list of rows if the number of resulting rows doesn't exceed :attr:`mqeconfig.FETCH_SIZE`, an iterator over the rows otherwise
        """
        query = self._assure_is_prepared(query, consistency_level)
        def do():
            return self._postprocess_result(self.session.execute(query, parameters))
        if not mqeconfig.DEBUG_QUERIES:
            return do()
        self._start = time.time()
        self._log_start(query, parameters)
        try:
            return do()
        finally:
            self._log_end()

    def _execute_parallel_list(self, queries):
        if isinstance(queries, (list, tuple)):
            queries = iter(queries)

        def do():
            res = []
            for chunk in util.chunks_it(queries, PARALLEL_QUERIES_CHUNK_SIZE):
                chunk = [self._assure_is_prepared(query, DEFAULT_CONSISTENCY_LEVEL) for query in chunk]
                if mqeconfig.DEBUG_QUERIES:
                    for query in chunk:
                        self._log_start(query, None)

                futures = [self.session.execute_async(query) for query in chunk]
                res.extend([self._postprocess_result(f.result()) for f in futures])
            return res
        if not mqeconfig.DEBUG_QUERIES:
            return do()
        log.info('Starting parallel execution of queries')
        self._start = time.time()
        try:
            return do()
        finally:
            self._log_end()

    def _execute_parallel_dict(self, name_to_query):
        items = name_to_query.items()
        names = [it[0] for it in items]
        queries = [it[1] for it in items]

        results = self._execute_parallel_list(queries)

        return {name: results[i] for i, name in enumerate(names)}

    def execute_parallel(self, queries):
        """Execute queries in parallel.

         If ``queries`` is a list, return a list of results for each query (the i-th result is for the i-th query).

         If ``queries`` is a dict mapping a query name to a query, return a dict mapping a query name to results.

         The type of each result is the same as for :meth:`execute`. The method executes up to :attr:`mqeconfig.PARALLEL_QUERIES_CHUNK_SIZE` simultaneous queries."""
        if isinstance(queries, dict):
            return self._execute_parallel_dict(queries)
        return self._execute_parallel_list(queries)

    def execute_fst(self, *args, **kwargs):
        """Execute the query by passing arguments to :meth:`execute` and return the first row from the result. If the result is empty, return a sentinel *non-existing* row which behaves like a dict which has ``None`` values for all keys. The presence of the sentinel row can be also checked with :func:`is_nonexisting`.

        Using the function allows writing concise code, for example::

          dashboard_id = c.cass.execute_fst('SELECT * FROM mqe.dashboard')['dashboard_id']
          if not dashboard_id:
              error('No dashboards')
        """
        rows = self.execute(*args, **kwargs)
        if not rows:
            return NONEXISTING
        return firstrow(rows)

    def _prepare(self, qs, consistency_level=DEFAULT_CONSISTENCY_LEVEL):
        prepared = self.qs_to_prepared.get(qs)
        if prepared is not None:
            return prepared
        if mqeconfig.DEBUG_QUERIES:
            log.info('Preparing %r', qs)
        prepared = self.session.prepare(qs)
        prepared.consistency_level = consistency_level
        self.qs_to_prepared[qs] = prepared
        if getattr(prepared, 'query_id', None):
            self.query_id_to_qs[prepared.query_id] = qs
        return prepared

    def _get_parameters(self, q):
        if hasattr(q, '_mqe_raw_values_list'):
            return q._mqe_raw_values_list
        if isinstance(q, cassandra.query.BoundStatement):
            return q.raw_values
        return None

    def _get_query_string(self, q):
        if isinstance(q, basestring):
            return q
        if isinstance(q, cassandra.query.BoundStatement):
            return q.prepared_statement.query_string
        if isinstance(q, cassandra.query.BatchStatement):
            qss = []
            for x in q._statements_and_parameters:
                if x[1] in self.query_id_to_qs:
                    qss.append(self.query_id_to_qs[x[1]])
                else:
                    qss.append(x[1])
            return '; '.join(qss)
        return q.query_string


