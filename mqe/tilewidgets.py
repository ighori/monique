import logging
from collections import OrderedDict, namedtuple
import datetime
import colorsys

from mqetables import enrichment
from mqe import dataseries
from mqe import util
from mqe.util import safeget, cyclicget, nestedget, CommonValue
from mqe import mqeconfig
from mqe import serialize
from mqe.serialize import mjson
from mqe import c


log = logging.getLogger('mqe.tilewidgets')


DEFAULT_SECONDS_BACK = 7 * 86400


class DataPoint(namedtuple('DataPoint', ('rid', 'dt', 'value'))):
    """A namedtuple representing a single data series point.

    .. attribute:: rid

         a report instance ID from which the value was extracted (a :class:`~uuid.UUID`)

    .. attribute:: dt

        a creation :class:`~datetime.datetime` of the report instance

    .. attribute:: value

        the value extracted from a report instance's cell
    """


    @staticmethod
    def from_series_value(series_value):
        return DataPoint(series_value['report_instance_id'],
                         util.datetime_from_uuid1(series_value['report_instance_id']),
                         series_value.value)


def is_value_charts_compatible(val):
    if enrichment.EnrichedValue(val).optimistic_as_number is None:
        return False
    return True


def data_points_by_dt(data_points):
    dts = [p.dt for p in data_points]
    res = OrderedDict.fromkeys(sorted(dts))
    for p in data_points:
        if not res[p.dt]:
            res[p.dt] = []
        res[p.dt].append(p)
    return res


class Tilewidget(object):
    """The base class for tilewidgets that represent the content of tiles. They
    fetch and format data from data series and manage series' colors and titles.

    :param ~mqe.tiles.Tile tile: the tile to which this tilewidget belongs
    """

    #: The string used to identify the class, used in :attr:`tile_config.tw_type`
    tw_type = None

    def __init__(self, tile):
        self.tile = tile

    @property
    def tile_options(self):
        return self.tile.tile_options

    def postprocess_new_tile_options(self, tile_config):
        """The method is called after creating a tile from a tile config.
        The tilewidget can modify and fill the tile's :data:`tile_options`."""
        pass

    def generate_tile_title(self, tile_data):
        series_names = [sd['name'] for sd in tile_data.get('series_data', [])]
        if not series_names:
            return '?'
        if not 2 <= len(series_names) <= 5:
            return self.tile.report.report_name
        if all(len(n) == 1 and n.isdigit() for n in series_names):
            return self.tile.report.report_name
        return '%s (%s)' % (self.tile.report.report_name, ', '.join(series_names))

    def generate_tile_title_postfix(self):
        if not self.tile_options['tags']:
            return ''
        return ''.join('[%s]' % tag for tag in self.tile_options['tags'])

    def _empty_series_dict(self, series_index):
        return {
            'series_id': self.tile.tile_options['series_configs'][series_index]['series_id'],
            'name': self.tile.tile_options['series_configs'][series_index]['series_spec'].name(),
            'common_header': None,
            'data_points': [],
        }

    def _set_combined_colors(self, data):
        if not data.get('series_data'):
            return
        default_options_list = dataseries.select_default_series_spec_options(
            self.tile.report_id, self.tile.series_specs())
        # compute combined_colors
        data['combined_colors'] = []
        for series_index, series_data in enumerate(data['series_data']):
            tw_color = safeget(self.tile_options.get('colors'), series_index)
            if tw_color:
                data['combined_colors'].append(tw_color)
                continue
            default_color = nestedget(default_options_list[series_index], 'color')
            if default_color:
                data['combined_colors'].append(default_color)
                continue
            fallback_color = cyclicget(mqeconfig.DEFAULT_COLORS, series_index)
            data['combined_colors'].append(fallback_color)

    def get_series_configs(self, series_spec_list):
        raise NotImplementedError()

    def get_tile_data(self, limit=None):
        """Called by :meth:`~mqe.tiles.Tile.get_tile_data`"""
        data = {}

        data['report_name'] = self.tile.report.report_name

        data['latest_extra_ri_data'] = {}
        latest_rid = self.tile.report.fetch_latest_instance_id(self.tile_options['tags'])
        if latest_rid is not None:
            latest_extra_ri_data = c.dao.ReportInstanceDAO.select_extra_ri_data(self.tile.report_id,
                                                                              latest_rid)
            if latest_extra_ri_data:
                data['latest_extra_ri_data'] = serialize.json_loads(latest_extra_ri_data)

        data['series_data'] = []

        self.fill_tile_data(data, limit=limit)

        if data['series_data']:
            data['common_header'] = util.common_value(sd['common_header'] for sd in data['series_data']
                                                      if sd.get('data_points'))

        data['generated_tile_title'] = self.generate_tile_title(data)
        data['generated_tile_title_postfix'] = self.generate_tile_title_postfix()

        self._set_combined_colors(data)

        drawer = create_drawer(self)
        drawer.process_tile_data(data)
        drawer.process_full_tile_data(data)

        return data

    def fill_tile_data(self, tile_data, limit):
        """The method is called to fill the full ``tile_data`` dict. At least the
        :attr:`tile_data.series_data` must be filled."""
        raise NotImplementedError()

    def get_new_tile_data(self, after_report_instance_id):
        """Called by :meth:`~mqe.tiles.Tile.get_new_tile_data`"""
        data = {}

        data['series_data'] = []

        self.fill_new_tile_data(data, after_report_instance_id)

        drawer = create_drawer(self)
        drawer.process_tile_data(data)
        drawer.process_new_tile_data(data)
        return data

    def fill_new_tile_data(self, new_tile_data, after_report_instance_id):
        """The method is called to fill the partial ``new_tile_data`` dict. At least the
        :attr:`tile_data.series_data` must be filled."""
        raise NotImplementedError()



class TilewidgetForRange(Tilewidget):
    """An implementation of the :class:`Tilewidget` class that fetches data from a time range of
    report instances.
    """

    #:
    tw_type = 'Range'

    def get_series_configs(self, series_spec_list):
        series_spec_list = series_spec_list[:mqeconfig.MAX_SERIES]
        tags_series_spec_list = [(self.tile_options['tags'], series_spec)
                                 for series_spec in series_spec_list]
        series_ids = dataseries.SeriesDef.select_id_or_insert_multi(
            self.tile.report_id, tags_series_spec_list)
        return [{'series_id': series_ids[i],
                 'series_spec': series_spec_list[i]} \
                for i in xrange(len(series_spec_list))]

    def _guess_drawer(self):
        chart = {'drawer_type': 'ChartRangeDrawer'}
        text = {'drawer_type': 'TextTableDrawer'}

        self.tile_options.update(text)
        data = self.get_tile_data(limit=100)
        points = util.flatten(sd['data_points'] for sd in data['series_data'])
        if not points:
            self.tile_options.update(chart)
            return
        by_dt = data_points_by_dt(points)
        last_dt = next(reversed(by_dt))
        if not by_dt[last_dt] or not all(is_value_charts_compatible(p.value) for p in by_dt[last_dt]):
            self.tile_options.update(text)
            return
        # the last dt being compatible is enough?
        self.tile_options.update(chart)

    def postprocess_new_tile_options(self, tile_config):
        if 'seconds_back' not in self.tile_options:
            self.tile_options['seconds_back'] = DEFAULT_SECONDS_BACK
        if 'drawer_type' not in self.tile_options:
            self._guess_drawer()

    def _set_series_data(self, data, from_dt=None, to_dt=None, after=None, limit=None):
        data['series_data'] = []

        series_def_list = dataseries.SeriesDef.select_multi(
            self.tile.report_id,
            [(self.tile.tags, sc['series_id']) for sc in self.tile_options['series_configs']])
        if not after:
            latest_instance_id = self.tile.report.fetch_latest_instance_id(self.tile.tags)
        else:
            latest_instance_id = None

        for series_def, series_config in zip(series_def_list, self.tile_options['series_configs']):
            if not series_def:
                log.warn('tile_data: series_def does not exist for report_id=%s series_config=%s', self.tile.report_id, series_config)
                continue

            if from_dt is not None or to_dt is not None:
                rows = dataseries.get_series_values(
                    series_def, self.tile.report, data['fetched_from_dt'], data['fetched_to_dt'],
                    limit=limit or mqeconfig.MAX_SERIES_POINTS_IN_TILE,
                    latest_instance_id=latest_instance_id)
            else:
                assert after is not None
                rows = dataseries.get_series_values_after(series_def, self.tile.report,
                            after, limit or mqeconfig.MAX_SERIES_POINTS_IN_TILE)

            value_list = []
            common_header = CommonValue()
            for row in rows:
                point = DataPoint.from_series_value(row)
                if point:
                    value_list.append(point)
                if row.header:
                    common_header.present(row.header)

            data['series_data'].append({
                'series_id': series_def.series_id,
                'name': series_def.series_spec.name(True),
                'data_points': value_list,
                'common_header': common_header.value,
            })

    def fill_tile_data(self, data, limit):
        now = datetime.datetime.utcnow()
        data['fetched_from_dt'] = now - datetime.timedelta(seconds=self.tile_options['seconds_back'])
        data['fetched_to_dt'] = now

        self._set_series_data(data, from_dt=data['fetched_from_dt'], to_dt=data['fetched_to_dt'],
                              limit=limit)

    def fill_new_tile_data(self, data, after_report_instance_id):
        if not after_report_instance_id:
            after_report_instance_id = util.min_uuid_with_dt(
                datetime.datetime.utcnow() - \
                datetime.timedelta(seconds=self.tile_options['seconds_back']))
        self._set_series_data(data, after=after_report_instance_id)


class TilewidgetForSingle(Tilewidget):
    """An implementation of the :class:`Tilewidget` that fetches data from a newest report instance."""

    #:
    tw_type = 'Single'

    def get_series_configs(self, series_spec_list):
        return [{
            'series_spec': series_spec,
            'series_id': util.uuid_for_string(mjson({'series_spec': series_spec, 'tags': self.tile_options['tags']})),
        } for series_spec in series_spec_list[:mqeconfig.MAX_SERIES]]

    def _guess_drawer(self):
        chart = {'drawer_type': 'ChartSingleDrawer'}
        text = {'drawer_type': 'TextSingleDrawer'}

        self.tile_options.update(chart)
        data = self.get_tile_data()
        points = util.flatten(sd['data_points'] for sd in data['series_data'])
        if not points:
            self.tile_options.update(text)
            return
        if all(is_value_charts_compatible(p.value) for p in points):
            self.tile_options.update(chart)
            return
        self.tile_options.update(text)

    def postprocess_new_tile_options(self, tile_config):
        if 'drawer_type' not in self.tile_options:
            self._guess_drawer()

    def _set_series_data(self, data, ri):
        for series_config in self.tile_options['series_configs']:
            cell = series_config['series_spec'].get_cell(ri)
            if not cell:
                data_points = []
            else:
                data_points = [DataPoint(ri['report_instance_id'],
                                         util.datetime_from_uuid1(ri['report_instance_id']),
                                         cell.value)]
            data['series_data'].append({
                'series_id': series_config['series_id'],
                'data_points': data_points,
                'name': series_config['series_spec'].name(True),
                'common_header': ri.table.header(cell.colno) if cell else None,
            })

    def _fetch_ri(self, report_instance_id=None):
        tags = self.tile_options['tags']
        if not report_instance_id:
            report_instance_id = self.tile.report.fetch_latest_instance_id(tags)
        if not report_instance_id:
            return None
        return self.tile.report.fetch_single_instance(report_instance_id, tags)

    def fill_tile_data(self, data, limit):
        ri = self._fetch_ri()
        if not ri:
            return
        self._set_series_data(data, ri)

    def fill_new_tile_data(self, data, after_report_instance_id):
        ri = self._fetch_ri()
        if not ri:
            return
        if after_report_instance_id and \
                (ri.report_instance_id == after_report_instance_id or \
                 util.uuid_lt(ri.report_instance_id, after_report_instance_id)):
            data['series_data'] = [self._empty_series_dict(i)
                                   for i in xrange(len(self.tile.tile_options['series_configs']))]
            return
        self._set_series_data(data, ri)



TILEWIDGET_CLASS_BY_TW_TYPE = {}

def get_tilewidget_class(tw_type):
    return TILEWIDGET_CLASS_BY_TW_TYPE.get(tw_type)

def register_tilewidget_class(tilewidget_cls):
    """Register a new :class:`Tilewidget` implementation class based on the class'
    :attr:`Tilewidget.tw_type` attribute (which can override an existing value).

    :param tilewidget_cls: a class implementing :class:`Tilewidget` methods
    """
    global TILEWIDGET_CLASS_BY_TW_TYPE
    TILEWIDGET_CLASS_BY_TW_TYPE[tilewidget_cls.tw_type] = tilewidget_cls
    return tilewidget_cls

register_tilewidget_class(TilewidgetForRange)
register_tilewidget_class(TilewidgetForSingle)



### Drawers

class Drawer(object):
    """A base abstract class for "drawers" that extend and modify :data:`tile_data` for drawing
    the data in a specific format, like a chart or a text table. A :class:`Drawer` is an
    extension of a :class:`Tilewidget`.
    """

    #: the drawer type that can be set as :attr:`tile_options.drawer_type`
    drawer_type = None

    def __init__(self, tw):
        #: the :class:`Tilewidget` instance
        self.tw = tw

    @property
    def tile(self):
        """the :class:`.Tile` object"""
        return self.tw.tile

    def process_tile_data(self, tile_data):
        """The method is called for :attr:`tile_data` computed for both
        :meth:`~mqe.tiles.Tile.get_tile_data` and :meth:`~mqe.tiles.Tile.get_new_tile_data`"""
        pass

    def process_full_tile_data(self, tile_data):
        """The method is called for :attr:`tile_data` computed for
        :meth:`~mqe.tiles.Tile.get_tile_data`"""
        pass

    def process_new_tile_data(self, tile_data):
        """The method is called for :attr:`tile_data` computed for
        :meth:`~mqe.tiles.Tile.get_new_tile_data`"""
        pass


class ChartDrawerBase(Drawer):

    def set_extra_options(self, data):
        data['extra_options'] = {}

        num_values = []
        for sd in data['series_data']:
            for data_point in sd.get('data_points', []):
                if util.is_number_or_bool(data_point.value):
                    num_values.append(data_point.value)

        if not num_values:
            return

        min_value = min(num_values)
        max_value = max(num_values)
        value_range = max_value - min_value
        all_0_1 = all(x in (0, 1, True, False) for x in num_values)

        if min_value < 0:
            return

        if all_0_1:
            data['extra_options']['y_axis_min'] = 0
            data['extra_options']['y_axis_max'] = 1
        else:
            would_extend_bottom = min_value
            if would_extend_bottom <= value_range:
                data['extra_options']['y_axis_min'] = 0

            if max_value <= 1:
                if max_value >= 0.5 and value_range >= 0.25:
                    data['extra_options']['y_axis_max'] = 1

        if 'y_axis_min' not in data['extra_options']:
            data['extra_options']['y_axis_min'] = None
        if 'y_axis_max' not in data['extra_options']:
            data['extra_options']['y_axis_max'] = None

    def wants_numbers(self, data):
        for sd in data['series_data']:
            non_number_idxs = set()
            for i, data_point in enumerate(sd.get('data_points', [])):

                ev = enrichment.EnrichedValue(data_point.value)
                converted = ev.optimistic_as_number
                if converted is not None:
                    sd['data_points'][i] = data_point._replace(value=converted)
                else:
                    non_number_idxs.add(i)

            # remove non-numbers
            if non_number_idxs:
                sd['data_points'] = [x for i, x in enumerate(sd.get('data_points', []))
                                     if i not in non_number_idxs]


class TextDrawerBase(Drawer):

    def _html_color_to_rgb(self, html_color):
        if not html_color.startswith('#'):
            return None
        value = html_color[1:]
        if len(value) != 6:
            return None
        return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)

    def _rgb_to_html_color(self, r, g, b):
        return '#%02x%02x%02x' % (r, g, b)

    def _change_brightness(self, html_color, factor):
        if not html_color or not html_color[0] == '#':
            return html_color
        rgb = self._html_color_to_rgb(html_color)
        if not rgb:
            return html_color
        res = [int(x*factor) for x in rgb]
        res = [max(0, min(x, 255)) for x in res]
        return res

    def darken_colors(self, data):
        def change_color(color):
            rgb = self._html_color_to_rgb(color)
            if not rgb:
                return color
            yiq = colorsys.rgb_to_yiq(*(x / 255.0 for x in rgb))
            if yiq[0] > 0.5:
                rgb = self._change_brightness(color, float(1 - (yiq[0] - 0.5)))
                return self._rgb_to_html_color(*rgb)
            return color

        if 'combined_colors' in data:
            data['combined_colors'] = [change_color(color) for color in data['combined_colors']]




class TextTableDrawer(TextDrawerBase):
    """A :class:`Drawer` implementation meant for rendering data series as a chart. It
    computes the :attr:`tile_data.series_data_as_rows` attribute and darkens color
    which are too bright to be easily visible on white background.
    """

    #:
    drawer_type = 'TextTableDrawer'

    def process_tile_data(self, data):
        self.darken_colors(data)
        self.compute_series_data_as_rows(data)

    def compute_series_data_as_rows(self, data):
        rows_dict = OrderedDict()
        for series_index, series_dict in enumerate(data['series_data']):
            for point_index, data_point in enumerate(series_dict['data_points']):
                if (data_point.rid, data_point.dt) not in rows_dict:
                    rows_dict[(data_point.rid, data_point.dt)] = {}
                rows_dict[(data_point.rid, data_point.dt)][series_index] = data_point.value
        rows_items = rows_dict.items()
        rows_items.sort(key=lambda ((rid, dt), _): dt)
        data['series_data_as_rows'] = rows_items



class TextSingleDrawer(TextDrawerBase):
    """A :class:`Drawer` implementation meant for rendering a newest report
    instance as text data. It darkens color which are too bright to be easily visible
    on white background."""

    #:
    drawer_type = 'TextSingleDrawer'

    def process_tile_data(self, data):
        self.darken_colors(data)


class ChartSingleDrawer(ChartDrawerBase):
    """A :class:`Drawer` implementation meant for rendering a newest report
    instance as a chart. If the input data points are not numeric, it tries to convert them to a
    number. If it's not possible, the points are skipped.
    """

    #:
    drawer_type = 'ChartSingleDrawer'

    def process_tile_data(self, data):
        self.wants_numbers(data)
        self.set_extra_options(data)


class ChartRangeDrawer(ChartDrawerBase):
    """A :class:`Drawer` implementation meant for rendering data series points as a chart.
    If the input data points are not numeric, it tries to convert them to a
    number. If it's not possible, the points are skipped.
    """

    #:
    drawer_type = 'ChartRangeDrawer'

    def process_tile_data(self, data):
        self.wants_numbers(data)
        self.set_extra_options(data)



DRAWER_CLASS_BY_DRAWER_TYPE = {}
def get_drawer_class(drawer_type):
    return DRAWER_CLASS_BY_DRAWER_TYPE.get(drawer_type)

def create_drawer(tw):
    drawer_class = get_drawer_class(tw.tile_options.get('drawer_type')) or Drawer
    return drawer_class(tw)

def register_drawer_class(drawer_cls):
    """Register a new :class:`Drawer` implementation class based on the class'
    :attr:`Drawer.drawer_type` attribute, which can override an existing value.

    :param drawer_cls: a class implementing :class:`Drawer` methods
    """
    global DRAWER_CLASS_BY_DRAWER_TYPE
    DRAWER_CLASS_BY_DRAWER_TYPE[drawer_cls.drawer_type] = drawer_cls
    return drawer_cls


register_drawer_class(TextTableDrawer)
register_drawer_class(TextSingleDrawer)
register_drawer_class(ChartRangeDrawer)
register_drawer_class(ChartSingleDrawer)
