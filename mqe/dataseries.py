from __future__ import division

import copy
import logging
from collections import OrderedDict, namedtuple, defaultdict

from mqetables import util as tabutil

from mqe import c
from mqe import mqeconfig
from mqe import serialize
from mqe import util
from mqe.dbutil import Row, TextColumn, ListColumn, TimeUUIDColumn, JsonColumn
from mqe.util import prev_dt


log = logging.getLogger('mqe.dataseries')


@serialize.json_type('SeriesSpec')
class SeriesSpec(object):
    """A description of data series - a list of values extracted from a range of report instances.
    Defines how to extract a single cell from a report instance.

    The definition can be represented using the following pseudo-SQL:

        select column ``data_colno`` where column ``filtering_colno`` *equals|contains*
        ``filtering_expr['args']``

    The `data_colno` defines a column number containing the wanted value, and the
    `filtering_colno` and :data:`filtering_expr` specify how to find the wanted row: the content of
    `filtering_colno` column must match :data:`filtering_expr`.

    For example, having the following report instance:

    .. list-table::
       :header-rows: 1

       * - user_name
         - is_active
         - points
       * - john
         - true
         - 128
       * - monique
         - true
         - 210

    the following :class:`SeriesSpec` object specifies that we want to graph monique's points::

        ss = SeriesSpec(data_colno=2,
                        filtering_colno=0,
                        filtering_expr={'op': 'eq', args: ['monique'])

    When the ``filtering_colno`` is equal to ``-1``, it references a virtual column
    containing row indexes and the :data:`filtering_expr` specifies the wanted row index (as a string).

    All non-temporary instance data is stored inside the :attr:`SeriesSpec.params` dictionary,
    which can be serialized directly and deserialized using :meth:`from_params`. The class supports also the built-in :ref:`guide_serialization`.

    """

    def __init__(self, data_colno, filtering_colno, filtering_expr):
        self._validate_filtering_expr(filtering_expr)

        self.params = OrderedDict()
        self.params['data_colno'] = data_colno
        self.params['filtering_colno'] = filtering_colno
        self.params['filtering_expr'] = filtering_expr
        self._fill_params()

    def _validate_filtering_expr(self, filtering_expr):
        assert isinstance(filtering_expr, dict)
        assert filtering_expr['op'] in ('eq', 'contains')
        assert filtering_expr['args']
        for arg in filtering_expr['args']:
            assert isinstance(arg, basestring)

    def _fill_params(self):
        # This one is used for selecting data
        self.params.setdefault('data_column_header', None)
        # This one is used just for guessing series spec name
        self.params.setdefault('data_column_header_for_name', None)

        self.params.setdefault('filtering_column_header', None)

        self.params.setdefault('static_name', None)

    def get_cell(self, report_instance):
        """Get a :class:`Cell` from a report instance specified by this :class:`SeriesSpec`.
        If the :class:`Cell` couldn't be extracted, return ``None``.
        """
        actual_data_colno = self.actual_data_colno(report_instance)
        if actual_data_colno is None:
            return None
        actual_filtering_colno = self.actual_filtering_colno(report_instance)
        if actual_filtering_colno is None:
            return None
        filtering_expr = self.params['filtering_expr']

        matching_row_idx = None
        if actual_filtering_colno == -1 and filtering_expr['op'] == 'eq':
            try:
                matching_row_idx = int(filtering_expr['args'][0])
            except ValueError:
                return None
            if not 0 <= matching_row_idx < report_instance.table.num_rows:
                return None
        else:
            for row_idx in xrange(report_instance.table.num_rows):
                if self._matches_filtering_expr(filtering_expr, report_instance.table.rows[row_idx][actual_filtering_colno]):
                    matching_row_idx = row_idx
                    break
        if matching_row_idx is None:
            return None

        res_ev = report_instance.table.rows[matching_row_idx][actual_data_colno]
        return Cell(matching_row_idx, actual_data_colno, res_ev.raw)

    def _matches_filtering_expr(self, filtering_expr, ev):
        if filtering_expr['op'] == 'eq':
            args = filtering_expr['args']
            for arg in args:
                if arg == '':
                    return True
                if arg == ev.raw or (not ev.raw_is_str and arg == ev.to_string_key()):
                    return True
            return False

        if filtering_expr['op'] == 'contains':
            args = filtering_expr['args']
            for arg in args:
                if arg == '':
                    return True
                if arg in ev.to_string_key():
                    return True
            return False

        assert False, 'Unknown filtering_expr %s' % filtering_expr

    def _colno_if_valid(self, colno, report_instance, virtual=False):
        if 0 <= colno < report_instance.table.num_columns:
            return colno
        if virtual and colno == -1:
            return colno
        return None

    def name(self, force_nonempty=False):
        """Get a series name, which is either set explicitly or computed automatically. The name
        is meant for displaying in the UI.

           The order of getting the name is the following:

           * the name set explicitly with :meth:`set_name` (which stores it as ``self.params[
             'name']``). This value is usually set by a user in the UI.
           * the value of ``self.params['static_name']``. This value can be set by application
             code to override automatic computation.
           * the value computed automatically from the params ``self.params[filtering_*]`` and
             ``self.data_col*``
        """
        if 'name' in self.params:
            if not force_nonempty or self.params['name']:
                return self.params['name']
        manual_name = self.params.get('name')
        if manual_name:
            return manual_name

        if self.params.get('static_name'):
            return self.params['static_name']

        filtering_param = util.nestedget(self.params, 'filtering_expr', 'args', 0)
        if filtering_param is not None:
            if self.params.get('filtering_colno') != -1:
                return filtering_param


            data_column_header = self.params.get('data_column_header_for_name')
            # filtering_param is row idx

            idx_label = ''
            if data_column_header:
                col_label = data_column_header
                #if filtering_param != 1:
                idx_label = '(%s)' % filtering_param
            else:
                col_label = 'col. %s' % self.params['data_colno']
                #if filtering_param != 0:
                idx_label = '(%s)' % filtering_param

            return u' '.join([col_label, idx_label]).strip()

        return '%s' % self.params['data_colno']

    def set_name(self, name):
        """Set an explicit series name.

        :param name: a string
        """
        self.params['name'] = name


    def promote_colnos_to_headers(self, report_instance):
        """Use column headers instead of column numbers for selecting values
        and computing the default series name. The headers are taken from the table of the
        ``report_instance`` and are put into ``params['data_column_header']`` and
        ``params[ 'filtering_column_header']``, which replace ``params['data_colno']``
        and ``params['filtering_colno']``.

        :param report_instance: a report instance from which to take column headers
        :type report_instance: :class:`~mqe.reports.ReportInstance`

        """
        if self.params['data_colno'] is not None:
            spec = report_instance.table.column_spec(self.params['data_colno'])
            if spec['header'] is not None:
                self.params['data_column_header'] = spec['header']

        if self.params['filtering_colno'] is not None:
            spec = report_instance.table.column_spec(self.params['filtering_colno'])
            if spec['header'] is not None:
                self.params['filtering_column_header'] = spec['header']

        if self.params['data_colno'] is not None and self.params['data_colno'] >= 0 \
                and report_instance.table.has_header:
            self.params['data_column_header_for_name'] = report_instance.table.header(
                self.params['data_colno'])


    def tweak_computed_name(self, report_instance):
        """Tweak the computed :meth:`name` based on a sample report instance for
        which the :class:`SeriesSpec` is defined. The method looks for default series
        names available for the report and uses headers from the report instance
        to produce nicer labels."""

        def tweak():
            if report_instance.table.num_rows == 1 and report_instance.table.num_columns == 1:
                self.params['static_name'] = 'value'
                return

            # The rest of customization is for series specs that use the virtual column
            if self.params.get('filtering_colno') != -1:
                return

            cell = self.get_cell(report_instance)
            if not cell:
                return

            # For single-column tables use just the row number
            if report_instance.table.num_columns == 1:
                self.params['static_name'] = str(cell.rowno)
                return

            # The series spec must point to the single value row
            if report_instance.table.header_idxs:
                first_value_idx = report_instance.table.header_idxs[-1] + 1
            else:
                first_value_idx = 0
            if first_value_idx != report_instance.table.num_rows - 1:
                return

            if cell.rowno == first_value_idx:
                self.params['static_name'] = self.params.get('data_column_header_for_name') or \
                                             'col. %s' % self.params['data_colno']

        # Make sure the auto-tweaks are applied first, before default options are fetched
        tweak()

        default_options = select_default_series_spec_options(report_instance.report_id, [self])[0]
        if default_options.get('name'):
            self.params['static_name'] = default_options['name']
            return



    def actual_data_colno(self, report_instance):
        """Returns an actual column index that acts as the `data_colno`. This can be different
        than the `data_colno` set explicitly if :meth:`promote_colnos_to_headers`
        was called. Returns ``None`` if the series spec can't return a valid data column for the
        report instance.
        """
        data_column_header = self.params.get('data_column_header')
        if data_column_header is not None:
            if data_column_header in report_instance.table.header_to_idx:
                return report_instance.table.header_to_idx[data_column_header]
            return None

        return self._colno_if_valid(self.params['data_colno'], report_instance)

    def actual_filtering_colno(self, report_instance):
        """Returns an actual column index that acts as the `filtering_colno`. This can be
        different
        than the `filtering_colno` set explicitly if :meth:`promote_colnos_to_headers`
        was called. Returns ``None`` if the series spec can't return a valid data column for the
        report instance.
        """
        filtering_column_header = self.params.get('filtering_column_header')
        if filtering_column_header is not None:
            if filtering_column_header in report_instance.table.header_to_idx:
                return report_instance.table.header_to_idx[filtering_column_header]
            return None

        return self._colno_if_valid(self.params.get('filtering_colno'), report_instance, True)

    def copy(self, without_params=[]):
        res = SeriesSpec.__new__(SeriesSpec)
        res.params = copy.deepcopy(self.params)
        for p in without_params:
            res.params.pop(p, None)
        return res

    def for_json(self):
        return util.sort_dict_by_key(self.params)

    @classmethod
    def from_params(cls, params):
        """Create a :class:`SeriesSpec` from the :attr:`SeriesSpec.params` dictionary"""
        res = SeriesSpec.__new__(SeriesSpec)
        res.params = OrderedDict()
        res.params.update(params)
        res._fill_params()
        return res

    @classmethod
    def from_rawjson(cls, obj):
        return cls.from_params(obj)

    def __repr__(self):
        return '<select %s where %s %s %s as %s>' % (
            self.params['data_column_header'] or self.params['data_colno'],
            self.params['filtering_column_header'] or self.params['filtering_colno'],
            '=' if self.params['filtering_expr']['op'] == 'eq' else
                   self.params['filtering_expr']['op'],
            self.params['filtering_expr']['args'][0]
                if len(self.params['filtering_expr']['args']) == 1 else
                self.params['filtering_expr']['args'],
            self.name(),
        )

    def __cmp__(self, other):
        if not isinstance(other, SeriesSpec):
            return -1
        return cmp(dict(self.params), dict(other.params))

    def __hash__(self):
        return hash(serialize.mjson(self))


class Cell(namedtuple('Cell', ('rowno', 'colno', 'value'))):
    """A namedtuple representing a single cell extracted from a report instance.

    .. attribute:: rowno

        a row number from which the value was extracted

    .. attribute:: colno

        a column number from which the value was extracted

    .. attribute:: value

        the extracted value

    """


def _label_score(ev):
    if ev.as_datetime is not None:
        return 0.5
    # if couldn't enrich to a number/date, it's a good label
    is_good_candidate = isinstance(ev.rich, basestring) and ev.rich and not ev.rich.isspace()
    if not is_good_candidate:
        return 0

    score = 1.0

    # lower score if numbers are present within string
    nums = tabutil.find_all_number_strs(ev.rich)
    nums_lowering = len(nums) if len(nums) < 5 else 5
    if nums_lowering:
        score -= nums_lowering * 0.1

    # lower score if "humansize" postfixes are present
    words = util.find_nonnumeric_words(ev.rich)
    words_lowering = 0
    for w in words:
        if w and w.lower() in ('gb', 'mb', 'tb', 'bytes'):
            words_lowering += 1
    if words_lowering:
        score -= words_lowering * 0.3

    return max(score, 0.1)

def guess_series_spec(report, report_instance, sample_rowno, sample_colno):
    """Guess a data series specification based on a sample cell that should be
    graphed.

    The function uses heuristics to guess which values should be put into ``filtering_*``
    parameters of the returned :class:`SeriesSpec`.
    """
    row = report_instance.table.rows[sample_rowno]
    filtering_candidate_cols = [colno for colno in xrange(report_instance.table.num_columns) \
                                if colno != sample_colno]
    def colno_score(colno):
        if _label_score(row[colno]) == 0:
            return 0
        vals = [report_instance.table.rows[i][colno] for i in
                report_instance.table.value_or_other_idxs]
        label_vals = [v for v in vals if _label_score(v) > 0]
        label_vals_factor = len(label_vals) / len(vals)
        if label_vals_factor < 0.5:
            return 0
        label_string_keys = [ev.to_string_key() for ev in label_vals]

        row_occurrences = label_string_keys.count(row[colno].to_string_key())
        if row_occurrences != 1:
            return 0

        uniq_vals = util.uniq_sameorder(label_string_keys)
        if report_instance.table.num_rows > 1 and len(uniq_vals) == 1:
            return 0
        return len(uniq_vals) * label_vals_factor * util.avg(_label_score(ev) for ev in label_vals)

    if filtering_candidate_cols:
        scores_colnos = [(colno_score(colno), colno) for colno in filtering_candidate_cols]
        max_score, filtering_colno_candidate = max(scores_colnos)
        if max_score > 0:
            filtering_colno = filtering_colno_candidate
            filtering_expr = {'op': 'eq', 'args': [row[filtering_colno].to_string_key()]}
        else:
            filtering_colno = -1
            filtering_expr = {'op': 'eq', 'args': [str(sample_rowno)]}
    else:
        filtering_colno = -1
        filtering_expr = {'op': 'eq', 'args': [str(sample_rowno)]}
    res = SeriesSpec(data_colno=sample_colno, filtering_colno=filtering_colno, filtering_expr=filtering_expr)

    res.promote_colnos_to_headers(report_instance)

    res.tweak_computed_name(report_instance)

    return res



### series defs


class SeriesDef(Row):
    """Series definition - a :class:`SeriesSpec` defined for a report and tags, with information
    for which report instances the data series values are available.

    Note that usually you will not need to use the
    class directly, as tiles and tilewidgets automatically create necessary series definitions.

    """

    #: the :class:`SeriesSpec` object
    series_spec = JsonColumn('series_spec')

    #: The ID of the series definition
    series_id = TimeUUIDColumn('series_id')

    #:
    report_id = TimeUUIDColumn('report_id')

    #:
    tags = ListColumn('tags')

    #: the minimal |rid| for which series values are available
    from_rid = TimeUUIDColumn('from_rid')

    #: the maximal |rid| for which series values are available
    to_rid = TimeUUIDColumn('to_rid')


    @property
    def from_dt(self):
        """A :class:`~datetime.datetime` from which this data series has data"""
        if self.from_rid is None:
            return None
        return util.datetime_from_uuid1(self.from_rid)

    @property
    def to_dt(self):
        """A :class:`~datetime.datetime` up to which this data series has data"""
        if self.to_rid is None:
            return None
        return util.datetime_from_uuid1(self.to_rid)

    @staticmethod
    def select(report_id, tags, series_id):
        """Select and return an existing :class:`SeriesDef` with the specified ID"""
        return SeriesDef.select_multi(report_id, [(tags, series_id)])[0]

    @staticmethod
    def select_multi(report_id, tags_series_id_list):
        rows = c.dao.SeriesDefDAO.select_multi(report_id, tags_series_id_list)
        return [SeriesDef(row) if row else None for row in rows]

    @staticmethod
    def insert(report_id, tags, series_spec):
        """Insert and return an ID of a new :class:`SeriesDef`"""
        return SeriesDef.insert_multi(report_id, [(tags, series_spec)])[0]

    @staticmethod
    def insert_multi(report_id, tags_series_spec_list):
        return c.dao.SeriesDefDAO.insert_multi(report_id, tags_series_spec_list)

    @staticmethod
    def select_id_or_insert(report_id, tags, series_spec):
        """Returns :attr:`series_id` of the existing or a newly created :class:`SeriesDef`
        matching the parameters"""
        return SeriesDef.select_id_or_insert_multi(report_id, [(tags, series_spec)])[0]

    @staticmethod
    def select_id_or_insert_multi(report_id, tags_series_spec_list):
        return c.dao.SeriesDefDAO.select_id_or_insert_multi(report_id, tags_series_spec_list)

    def update_from_rid(self, from_rid):
        c.dao.SeriesDefDAO.update_from_rid_to_rid(self.report_id, self.series_id, self.tags,
                                                from_rid=from_rid)
        self['from_rid'] = from_rid

    def update_to_rid(self, to_rid):
        c.dao.SeriesDefDAO.update_from_rid_to_rid(self.report_id, self.series_id, self.tags,
                                                to_rid=to_rid)
        self['to_rid'] = to_rid

    def key(self):
        return (self['report_id'], self.tags, self['series_spec'])



def clear_series_defs(report_id, tags_powerset):
    c.dao.SeriesDefDAO.clear_all_series_defs(report_id, tags_powerset)



class SeriesValue(Row):
    """A single data series value extracted from a given :class:`.ReportInstance`.
    The :attr:`value` can be any JSON-serializable value
    """

    #: the :attr:`SeriesDef.series_id`
    series_id = TimeUUIDColumn('series_id')

    #: the ID of the report instance from which the value was extracted
    report_instance_id = TimeUUIDColumn('report_instance_id')

    #: the value deserialized from JSON
    value = JsonColumn('json_value')

    #: an optional header of the value
    header = TextColumn('header')


def insert_series_values(series_def, report, from_dt, to_dt, after=None, limit=None):
    assert after or (from_dt is not None and to_dt is not None)

    log.debug('insert_series_values report_id=%s sd.from_dt=%s sd.to_dt=%s from_dt=%s'
              'to_dt=%s after=%s limit=%s', report.report_id, series_def.from_dt,
              series_def.to_dt, from_dt, to_dt, after, limit)

    instances = report.fetch_instances(after=after,
                                       from_dt=from_dt if not after else None,
                                       to_dt=to_dt if not after else None,
                                       limit=limit or mqeconfig.MAX_SERIES_POINTS,
                                       tags=series_def.tags,
                                       columns=['report_instance_id', 'ri_data'])
    if not instances:
        return
    data = []
    for ri in instances:
        cell = series_def.series_spec.get_cell(ri)
        if cell:
            row = dict(report_instance_id=ri.report_instance_id,
                       json_value=serialize.mjson(cell.value))
            header = ri.table.header(cell.colno)
            if header:
                row['header'] = header
            data.append(row)
    log.info('Inserting %d series values from %d instances report_name=%r series_id=%s',
             len(data), len(instances), report.report_name, series_def.series_id)
    c.dao.SeriesValueDAO.insert_multi(series_def.series_id, data)

    oldest_rid_fetched = instances[0].report_instance_id
    newest_rid_fetched = instances[-1].report_instance_id

    # from_rid stores minimal uuid from dt for which we fetched instances,
    # while to_rid stores an actual latest report_instance_id in the series.
    # However, generally it's not expected to_rid can always be a real report_instance_id
    if from_dt is not None:
        oldest_rid_stored = util.min_uuid_with_dt(from_dt)
    else:
        oldest_rid_stored = oldest_rid_fetched

    if series_def.from_rid is None or \
            util.uuid_lt(oldest_rid_stored, series_def.from_rid):
        log.debug('Updating series_def_id=%s from_rid_dt=%s', series_def.series_id,
                  util.datetime_from_uuid1(oldest_rid_stored))
        series_def.update_from_rid(oldest_rid_stored)

    if series_def.to_rid is None or \
            util.uuid_lt(series_def.to_rid, newest_rid_fetched):
        log.debug('Updating series_def_id=%s to_rid_dt=%s', series_def.series_id,
                  util.datetime_from_uuid1(newest_rid_fetched))
        series_def.update_to_rid(newest_rid_fetched)


def get_series_values(series_def, report, from_dt, to_dt,
                      limit=mqeconfig.MAX_SERIES_POINTS_IN_TILE, latest_instance_id=None):
    """Retrieves a list of :class:`SeriesValue` objects for a given time range.
    The function inserts new data series values if they haven't been already created
    for the requested time period.

    :param SeriesDef series_def: a series definition for which to get data
    :param ~mqe.report.Report report: a report for which to get data
    :param ~datetime.datetime from_dt: starting datetime
    :param ~datetime.datetime to_dt: ending datetime
    :param int limit: the limit of the series values to fetch/create
    :param latest_instance_id: (optional) a latest report instance ID of the report and tags
        (if not passed, the value will be fetched)
    :return: a list of :class:`SeriesValue` objects in the order of creation time of the corresponding report instances
    """
    assert from_dt is not None and to_dt is not None
    if series_def.from_dt is None or series_def.to_dt is None:
        insert_series_values(series_def, report, from_dt, to_dt, limit=limit)
    else:
        if from_dt < series_def.from_dt:
            insert_series_values(series_def, report, from_dt, prev_dt(series_def.from_dt), limit=limit)

        if not latest_instance_id:
            latest_instance_id = report.fetch_latest_instance_id(series_def.tags)
        if latest_instance_id is not None \
                and util.uuid_lt(series_def['to_rid'], latest_instance_id) \
                and to_dt >= series_def.to_dt:
            insert_series_values(series_def, report, None, None, after=series_def['to_rid'], limit=limit)


    min_report_instance_id = util.uuid_for_prev_dt(util.uuid_with_dt(from_dt))
    max_report_instance_id = util.uuid_for_next_dt(util.uuid_with_dt(to_dt))
    rows = c.dao.SeriesValueDAO.select_multi(series_def.series_id, min_report_instance_id,
                                             max_report_instance_id, limit)

    log.debug('Selected %d series_values by dates series_id=%s report_name=%r',
              len(rows), series_def.series_id, report.report_name)
    return list(reversed([SeriesValue(row) for row in rows]))


def get_series_values_after(series_def, report, after,
                            limit=mqeconfig.MAX_SERIES_POINTS_IN_TILE):
    """Retrieves a list of :class:`SeriesValue` created after the specified report instance ID
    (``after``). The function inserts new data series values if they haven't been already created.

    :param SeriesDef series_def: a series definition for which to get data
    :param ~mqe.report.Report report: a report for which to get data
    :param ~uuid.UUID after: a ``report_instance_id`` that specifies the starting point to fetch
        (and possibly create) data series values.
    :param int limit: the limit of the series values to fetch/create
    :return: a list of :class:`SeriesValue` objects in the order of creation time of the corresponding report instances
    """
    if series_def['from_rid'] is None or series_def['to_rid'] is None:
        insert_after = after
    elif util.uuid_lt(after, series_def['from_rid']):
        insert_after = after
    else:
        insert_after = series_def['to_rid']
    insert_series_values(series_def, report, None, None, after=insert_after)

    rows = c.dao.SeriesValueDAO.select_multi(series_def.series_id, after, None, limit)
    log.debug('Selected %d series_values after series_id=%s report_name=%r',
              len(rows), series_def.series_id, report.report_name)
    return list(reversed([SeriesValue(row) for row in rows]))



### default options

def series_spec_for_default_options(series_spec):
    return series_spec.copy(without_params=['name', 'data_column_header_for_name'])

def update_default_options(tile):
    """Include the tile's :data:`tile_options` in a pool of default options belonging
    to the owner of the tile"""
    series_specs = tile.series_specs()
    if not series_specs:
        return

    old_options_list = c.dao.OptionsDAO.select_multi(tile.report_id, 'SeriesSpec',
        [serialize.mjson(series_spec_for_default_options(ss)) for ss in series_specs])

    default_options_by_ss = {}
    for ss_do_raw, options_raw in old_options_list:
        ss_do = series_spec_for_default_options(serialize.json_loads(ss_do_raw))
        default_options_by_ss[ss_do] = serialize.json_loads(options_raw)

    colors = tile.tile_options.get('colors')
    to_set = []
    for i, ss in enumerate(series_specs):
        ss_do = series_spec_for_default_options(ss)
        old_options = default_options_by_ss.get(ss_do, {})
        new_options = {}

        name = ss.params.get('name')
        if name:
            new_options['name'] = name

        color = util.safeget(colors, i)
        if color:
            new_options['color'] = color

        if old_options.get('color') and not new_options.get('color'):
            new_options['color'] = old_options['color']

        if old_options != new_options:
            to_set.append((serialize.mjson(ss_do), serialize.mjson(new_options)))

    if to_set:
        c.dao.OptionsDAO.set_multi(tile.report_id, 'SeriesSpec', to_set)
        log.debug('Updated default options from tile %s', tile)


def select_default_series_spec_options(report_id, series_spec_list):
    """Return default options for a list of :class:`SeriesSpec` objects
    as a list of dictionaries with the keys:

    * ``name`` - the suggested name of the series
    * ``color`` - the suggested color of the series

    """
    assert isinstance(series_spec_list, list)

    series_spec_vals = [serialize.mjson(series_spec_for_default_options(ss))
                        for ss in series_spec_list]
    series_spec_val_to_idxs = defaultdict(list)
    for i, ssv in enumerate(series_spec_vals):
        series_spec_val_to_idxs[ssv].append(i)

    res = [{} for _ in xrange(len(series_spec_list))]
    options_list = c.dao.OptionsDAO.select_multi(report_id, 'SeriesSpec', series_spec_vals)
    for series_spec_raw, default_options_raw in options_list:
        for idx in series_spec_val_to_idxs[series_spec_raw]:
            if default_options_raw:
                res[idx] = serialize.json_loads(default_options_raw)
    return res
