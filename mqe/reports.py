import logging
import sys
from collections import OrderedDict, namedtuple

import datetime

from mqe import c
from mqe import mqeconfig
from mqe import serialize
from mqe import util
from mqe.dbutil import Row, gen_timeuuid, TextColumn, ListColumn, TimeUUIDColumn, JsonColumn
from mqetables import basicparsing
from mqetables import parseany
from mqetables import parsing
from mqe.signals import fire_signal, new_report

log = logging.getLogger('mqe.reports')



class ReportInstance(Row):
    """A representation of a report instance - tabular data associated with a
    :class:`Report`"""

    #: a report ID to which this report instance belongs
    report_id = TimeUUIDColumn('report_id')

    #: a time-UUID identifying the report instance
    report_instance_id = TimeUUIDColumn('report_instance_id')

    #: an input string from which the :attr:`table` was created
    input_string = TextColumn('input_string')

    ri_data = JsonColumn('ri_data') # type: dict

    #: a list of all tags attached to the report instance at the creation time.
    all_tags = ListColumn('all_tags') # type: list

    @property
    def table(self):
        """The data of this report instance as an :class:`~mqetables.enrichment.EnrichedTable`"""
        return self.ri_data.get('table')

    @property
    def created(self):
        """Creation datetime"""
        return util.datetime_from_uuid1(self.report_instance_id)

    @property
    def parsing_result_desc(self):
        """Extra description of the parsed table - a dictionary possibly containing the keys:

        * ``input_is_json`` - set to ``True`` if the input string was a JSON document"""
        return self.ri_data.get('result_desc', {})

    def _rows_result(self, rows):
        return [[ev.rich for ev in row] for row in rows]

    def _raw_rows_result(self, rows):
        return [[ev.raw for ev in row] for row in rows]

    def fetch_extra_ri_data(self):
        """Fetch the ``extra_ri_data`` passed to :meth:`.process_input` - a custom JSON document"""
        raw_res = c.dao.ReportInstanceDAO.select_extra_ri_data(self.report_id, self.report_instance_id)
        return serialize.json_loads(raw_res) if raw_res is not None else None

    def desc(self, expand_content, expand_input=False):
        """Returns a dictionary describing the instance, containing at least ``id``, ``tags`` and
        ``created`` keys.

        :param bool expand_content: whether to include ``rows`` and ``header`` in the result
        :param bool expand_input: whether to include user input string under ``input`` key
        """
        d = OrderedDict()
        d['id'] = self.report_instance_id.hex
        d['tags'] = self.all_tags
        d['created'] = self.created.isoformat()
        if expand_content:
            if self.table is not None:
                d['rows'] = self._raw_rows_result(self.table.rows)
                d['header'] = self.table.header_idxs
            else:
                d['rows'] = []
                d['header'] = []
        if expand_input:
            if self.parsing_result_desc.get('input_is_json'):
                d['input'] = serialize.json_loads(self['input_string'])
            else:
                d['input'] = self['input_string']
        return d

    skip_printing = ('ri_data', 'input_string', 'day', 'tags')

    def key(self):
        return (self.report_instance_id, tuple(self.all_tags))



class Report(Row):
    """A representation of a report - a list of report instances grouped under a name,
     belonging to the same owner.
    """

    #: the ID of the report
    report_id = TimeUUIDColumn('report_id')

    #: the owner of the report
    owner_id = TimeUUIDColumn('owner_id')

    #: the name of the report
    report_name = TextColumn('report_name')
    
    @staticmethod
    def select(report_id):
        """Select and return an existing report with the given ID, ``None`` if it doesn't exist"""
        row = c.dao.ReportDAO.select(report_id)
        return Report(row) if row else None

    @staticmethod
    def select_multi(owner_id, report_id_list):
        """Returns an ordered dictionary mapping the report ID present on the ``report_id_list`` to a :class:`Report`, in the order of the ``report_id_list``. Non-existing reports are not present in the result."""
        report_id_list = util.uniq_sameorder(report_id_list)
        rows = c.dao.ReportDAO.select_multi(owner_id, report_id_list)
        row_by_id = {row['report_id']: row for row in rows}
        res = OrderedDict()
        for id in report_id_list:
            if id in row_by_id:
                res[id] = Report(row_by_id[id])
        return res

    @staticmethod
    def select_or_insert(owner_id, report_name):
        """Returns a :class:`Report` having the passed name - either an existing row or a newly
        created"""
        inserted, row = c.dao.ReportDAO.select_or_insert(owner_id, report_name)
        if not row:
            return
        report = Report(row)

        if inserted:
            fire_signal(new_report, report=report)
        return report


    @staticmethod
    def select_by_name(owner_id, report_name):
        """Returns an existing :class:`Report` having the passed name,
        ``None`` if it doesn't exist"""
        row = c.dao.ReportDAO.select_by_name(owner_id, report_name)
        return Report(row) if row else None

    @staticmethod
    def insert(owner_id, report_name):
        """Insert and return a new :class:`Report` having the passed name. Returns ``None``
        if it already exists."""
        row = c.dao.ReportDAO.insert(owner_id, report_name)
        if not row:
            return None

        report = Report(row)

        fire_signal(new_report, report=report)

        return report


    def process_input(self, input_string, tags=None, created=None, input_type='any',
                      ip_options={}, force_header=None, extra_ri_data=None,
                      handle_tpcreator=True, handle_sscreator=True):
        """Process an input string - parse it into a table and create a report instance belonging
        to the report.

        :param str|unicode input_string: the input string
        :param list tags: a list of string tags attached to the report instance
        :param ~datetime.datetime created: an explicit creation datetime of the report instance (
            default: the current datetime)
        :param str input_type: input type (see :func:`mqetables.parseany.parse_input`)
        :param dict ip_options: extra parser options (see :func:`mqetables.parsing.InputParser`)
        :param force_header: a list of header rows indexes to set as a header (defaults to
            auto-detection)
        :param extra_ri_data: a custom JSON-serializable document attached to the report instance
        :param handle_tpcreator: whether to handle TPCreator for the created report instance
            by calling :func:`~mqe.tpcreator.handle_tpcreator`
        :param handle_sscreator: whether to handle SSCS by calling :func:`~mqe.sscreator.handle_sscreator`
        :return: an :class:`InputProcessingResult`
        """
        assert isinstance(input_string, (str, unicode))

        # disallow 'created' in the future
        now = datetime.datetime.utcnow()
        if created is not None and created.tzinfo:
            created = util.make_tz_naive(created)

        if created is not None and created.year < 2000:
                raise ValueError('created cannot be before the year 2000')

        if created is not None and created < now:
            report_instance_id = util.uuid_with_dt(created)
            custom_created = True
        else:
            custom_created = False
            report_instance_id = gen_timeuuid()
            created = util.datetime_from_uuid1(report_instance_id)

        if tags is None:
            tags = []

        parsing_result = parseany.parse_input(input_string, input_type, ip_options)
        table = mqeconfig.get_table_from_parsing_result(parsing_result)
        if table is None:
            return InputProcessingResult(None, parsing_result)

        if force_header is not None:
            log.debug('Overwriting header detection due to force_header')
            table.header_idxs = [i for i in force_header if util.valid_index(table.num_rows, i)]
            table.header_idxs_source = parsing.HEADER_IDXS_SOURCE_USER

        ri_data_dict = {
            'table': table,
        }
        result_desc = self._get_result_desc(parsing_result)
        if result_desc:
            ri_data_dict['result_desc'] = result_desc

        report_instance_row = c.dao.ReportInstanceDAO.insert(
            owner_id=self.owner_id, report_id=self.report_id, report_instance_id=report_instance_id,
            tags=tags, ri_data=serialize.mjson(ri_data_dict),
            input_string=parsing_result.input_string,
            extra_ri_data=serialize.mjson(extra_ri_data) if extra_ri_data else None,
            custom_created=custom_created)

        report_instance = ReportInstance(report_instance_row)

        log.info('Created new report instance report_id=%s report_name=%r tags=%s '
                 'report_instance_id=%s created=%s', self.report_id, self.report_name, tags,
                 report_instance_id, report_instance.created)

        if tags and handle_tpcreator:
            from mqe import tpcreator
            tpcreator.handle_tpcreator(self.owner_id, self.report_id, report_instance)

        if handle_sscreator:
            from mqe import sscreator
            sscreator.handle_sscreator(self.owner_id, self.report_id, report_instance)

        if custom_created:
            from mqe import dataseries
            dataseries.clear_series_defs(self.report_id, util.powerset(tags))

        return InputProcessingResult(report_instance, parsing_result)

    def _get_result_desc(self, parsing_result):
        res = {}
        if parsing_result.best_input_parser:
            if isinstance(parsing_result.best_input_parser, (basicparsing.JsonParser,
                                                             basicparsing.JsonDeepParser)):
                res['input_is_json'] = True
        return res

    def _min_max_uuid_from_args(self, from_dt, to_dt, before, after):
        if after is not None or before is not None:
            min_uuid = after or util.MIN_UUID
            max_uuid = before or util.MAX_UUID
        else:
            min_uuid = util.uuid_for_prev_dt(util.uuid_with_dt(from_dt)) \
                if from_dt is not None else util.MIN_UUID
            max_uuid = util.uuid_for_next_dt(util.uuid_with_dt(to_dt)) \
                if to_dt is not None else util.MAX_UUID
        return min_uuid, max_uuid

    def fetch_instances(self, from_dt=None, to_dt=None, before=None, after=None, tags=None, columns=None,  order='asc', limit=100):
        """Fetch a list of report instances. The time range can be specified as either
        datetimes (``from_dt``, ``to_dt``) or report instance IDs (``before``, ``after``).

        :param ~datetime.datetime|None from_dt: fetch report instances created on the datetime or
            later
        :param ~datetime.datetime|None to_dt: fetch report instances created on the datetime or
            earlier
        :param ~uuid.UUID|None before: fetch instances created before the given report instance ID
        :param ~uuid.UUID|None after: fetch instances created after the given report instance ID
        :param list tags: a list of tags the returned instances must have attached
        :param str order: ``asc`` (ascending) or ``desc`` (descending) order wrt. creation datetime
        :param list columns: a list of :class:`ReportInstance` attributes to select
        :param int limit: the limit of the number of report instances to fetch
            fetch
        :return: a list of :class:`ReportInstance` objects
        """
        min_uuid, max_uuid = self._min_max_uuid_from_args(from_dt, to_dt, before, after)

        rows = c.dao.ReportInstanceDAO.select_multi(report_id=self.report_id, tags=tags,
                min_report_instance_id=min_uuid, max_report_instance_id=max_uuid,
                columns=columns, order=order, limit=limit)
        return [ReportInstance(row) for row in rows]


    def fetch_single_instance(self, report_instance_id, tags=None):
        """Fetch a single report instance with the given ID and tags (returns ``None`` if such
        instance doesn't exist)"""
        row = c.dao.ReportInstanceDAO.select(self.report_id, report_instance_id, tags)
        return ReportInstance(row) if row else None

    def fetch_latest_instance_id(self, tags=None):
        """Returns the report instance ID with the latest creation datetime"""
        return c.dao.ReportInstanceDAO.select_latest_id(self.report_id, tags)

    def fetch_prev_instance(self, report_instance_id, tags=None):
        """Fetch the previous report instance - the latest created before the given report
        instance ID and having the specified tags"""
        ri_list = self.fetch_instances(before=report_instance_id, limit=1, tags=tags, order='desc')
        return ri_list[0] if ri_list else None

    def fetch_next_instance(self, report_instance_id, tags=None):
        """Fetch the next report instance - the oldest created after the given report instance ID (``rid``) and having the specified tags"""
        ri_list = self.fetch_instances(after=report_instance_id, limit=1, tags=tags)
        return ri_list[0] if ri_list else None

    def find_report_instance_by_dt(self, dt, tags=None):
        """Find a report instance with a creation datetime matching ``dt`` datetime as close as
        possible."""
        ri_prev = self.fetch_prev_instance(util.max_uuid_with_dt(dt), tags)
        ri_next = self.fetch_next_instance(util.min_uuid_with_dt(dt), tags)
        return min([ri_prev, ri_next], key=lambda ri: abs((ri.created - dt).total_seconds())
                                                      if ri else sys.maxint)

    def report_instance_count(self):
        """The number of report instances belonging to the report."""
        return c.dao.ReportDAO.select_report_instance_count(self.owner_id, self.report_id)

    def report_instance_diskspace(self):
        """The sum of input sizes consumed by the report instances of this report"""
        return c.dao.ReportDAO.select_report_instance_diskspace(self.owner_id, self.report_id)

    def delete_single_instance(self, report_instance_id, update_counters=True):
        """Delete the given report instance and return a :class:`bool` telling if the
        operation was successful. If ``update_counters`` is ``False``, report instance
        and disk space counters won't be updated."""

        from mqe import dataseries

        num, all_tags_subsets = c.dao.ReportInstanceDAO.delete(self.owner_id, self.report_id,
                                               report_instance_id, update_counters=update_counters)
        dataseries.clear_series_defs(self.report_id, all_tags_subsets)
        return num > 0

    def delete_multiple_instances(self, tags=[], from_dt=None, to_dt=None,
                                  before=None, after=None, limit=1000, update_counters=True):
        """Delete a range of report instances specified by the arguments described
        for the :meth:`fetch_instances` method. If ``update_counters`` is ``False``, report instance
        and disk space counters won't be updated. Returns the number of deleted instances."""
        from mqe import dataseries

        min_uuid, max_uuid = self._min_max_uuid_from_args(from_dt, to_dt, before, after)

        num, all_tags_subsets = c.dao.ReportInstanceDAO.delete_multi(self.owner_id,
                                         self.report_id, tags, min_uuid, max_uuid, limit,
                                         update_counters=update_counters)

        dataseries.clear_series_defs(self.report_id, all_tags_subsets)

        return num

    def delete(self):
        """Delete the report. The method detaches and deletes tiles that display the report.
        Report instances are NOT deleted by the method - the method
        :meth:`delete_multiple_instances` must be called before :meth:`delete` to achieve it.
        """
        from mqe import dashboards
        from mqe import layouts

        owner_dashboards = dashboards.OwnerDashboards(self.owner_id)
        for dashboard in owner_dashboards.dashboards:
            layout = layouts.Layout.select(self.owner_id, dashboard.dashboard_id)
            if not layout:
                continue
            tiles_to_detach = [tile for tile in layout.tile_dict
                                    if tile.report_id == self.report_id]
            if tiles_to_detach:
                res = layouts.replace_tiles({tile: None for tile in tiles_to_detach}, None)
                if not res:
                    return False

        c.dao.ReportDAO.delete(self.owner_id, self.report_id)

        return True

    def fetch_days(self, tags=None):
        """Fetch a list of days on which report instances with the specified tags were created as :class:`~datetime.datetime` objects"""
        return c.dao.ReportDAO.select_report_instance_days(self.report_id, tags or [])

    def fetch_tags_sample(self, tag_prefix='', limit=10):
        """Fetch sample tags attached to the report instances, having the given ``prefix``,
        returning up to the ``limit`` values"""
        return c.dao.ReportDAO.select_tags_sample(self.report_id, tag_prefix, limit)

    def has_tags(self):
        """Tells if any tags are used for the report's instances"""
        return bool(self.fetch_tags_sample(limit=1))

    def key(self):
        return self.report_id



class InputProcessingResult(namedtuple('InputProcessingResult',
                                       ('report_instance', 'parsing_result'))):
    """A result of the :meth:`Report.process_input` method.

    :ivar ReportInstance report_instance: the created report instance. ``None`` if no report
        instance was created
    :ivar ~mqetables.parseany.ParsingResult: parsing_result: the detailed result of running the
        parsers
    """


def fetch_reports_by_name(owner_id, name_prefix=None, after_name=None, limit=100):
    """Fetch report IDs with a name having the given prefix, placed lexicographically after the
    given report name.

    :param ~uuid.UUID owner_id: the owner of the reports
    :param str name_prefix: a prefix of report names
    :param str|None after_name: if not ``None``, search for report names coming after the
        parameter
    :param int limit: the limit of results to return
    :return: a list of report IDs (UUIDs)
    """
    ids = c.dao.ReportDAO.select_ids_by_name_prefix_multi(owner_id, name_prefix, after_name, limit)
    return Report.select_multi(owner_id, ids).values()

def owner_has_reports(owner_id):
    """Returns a boolean telling if the owner has created any reports"""
    ids = c.dao.ReportDAO.select_ids_by_name_prefix_multi(owner_id, '', None, 1)
    return bool(ids)

def report_instance_count_for_owner(owner_id):
    """The number of report instances created by the given owner"""
    return c.dao.ReportInstanceDAO.select_report_instance_count_for_owner(owner_id)

def report_instance_diskspace_for_owner(owner_id):
    """The sum of input sizes consumed by the report instances created by the owner"""
    return c.dao.ReportInstanceDAO.select_report_instance_diskspace_for_owner(owner_id)


