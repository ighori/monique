from __future__ import division

import logging

import datetime

from mqe import c
from mqe import dataseries
from mqe import mqeconfig
from mqe import reports
from mqe import serialize
from mqe import tilewidgets
from mqe import util
from mqe.dbutil import Row, Column, JsonColumn
from mqe.util import nestedget, cached_property


log = logging.getLogger('mqe.tiles')


DEFAULT_TW_TYPE = 'Range'


### Tile

@serialize.json_type('Tile')
class Tile(Row):
    """A dashboard tile specified using :attr:`tile_config`. A tile has a unique
    :attr:`tile_id` and once created, cannot be modified (see :ref:`guide_layouts`).
    Tiles are included in dashboard layouts.

    A tile contains a :class:`~mqe.tilewidget.Tilewidget` and uses it in the
    :meth:`get_tile_data` and :meth:`get_new_tile_data` methods to return :data:`tile_data` -
    preformatted series data ready for rendering a chart or a table.

    .. autoattribute:: owner_id
    .. autoattribute:: report_id

    """

    @property
    def owner_id(self):
        return self.tile_options.get('owner_id')

    @property
    def report_id(self):
        return self.tile_options.get('report_id')

    #: the dashboard ID to which the tile belongs
    dashboard_id = Column('dashboard_id')

    #: the ID of the tile (a timeuuid)
    tile_id = Column('tile_id')

    #: :attr:`tile_options` of the tile - the full specification of the tile's data
    tile_options = JsonColumn('tile_options', default=lambda: {}) # type: dict

    @property
    def tags(self):
        return self.tile_options.get('tags')

    @cached_property
    def report(self):
        """The :class:`~mqe.reports.Report` of this tile"""
        if not self.report_id:
            raise ValueError('report_id not set in tile_options')
        if not self.owner_id:
            raise ValueError('owner_id not set in tile_options')
        res = reports.Report.select(self.report_id)
        if res is None:
            raise ValueError('The report specified in tile_options does not exist (owner_id=%s '
                             'report_id=%s)' % (self.owner_id, self.report_id))
        return res

    @cached_property
    def tilewidget(self):
        """The :class:`~mqe.tilewidgets.Tilewidget` of this tile"""
        tilewidget_class = tilewidgets.get_tilewidget_class(self.tile_options['tw_type'])
        if tilewidget_class is None:
            raise ValueError('Invalid tw_type %r' % self.tile_options['tw_type'])
        return tilewidget_class(self)

    @classmethod
    def select(cls, dashboard_id, tile_id):
        """Return a single :class:`Tile` (``None`` if it doesn't exist)"""
        rows = c.dao.TileDAO.select_multi(dashboard_id, [tile_id])
        return Tile(rows[0]) if rows else None

    @classmethod
    def select_multi(cls, dashboard_id, tile_id_list):
        """Return a dict mapping :attr:`tile_id` values from the list to :class:`Tile` objects"""
        if not tile_id_list:
            return {}
        rows = c.dao.TileDAO.select_multi(dashboard_id, tile_id_list)
        return {row['tile_id']: Tile(row) for row in rows}

    @classmethod
    def insert(cls, owner_id, report_id, dashboard_id, tile_config, skip_db=False):
        """Insert and return a new :class:`Tile` specified using the :data:`tile_config`.
        If ``skip_db`` is ``True``, the tile is not inserted into the database."""
        cls._postprocess_tile_config(tile_config)
        cls._validate_tile_config(tile_config)

        tile_options = {
            'tw_type': tile_config.get('tw_type', DEFAULT_TW_TYPE),
            'owner_id': owner_id,
            'report_id': report_id,
            'tags': tile_config.get('tags', []),
        }

        tile_options.update(tile_config.get('tile_options', {}))

        partial_tile = Tile({'dashboard_id': dashboard_id, 'tile_id': None,
                             'tile_options': serialize.mjson(tile_options)})
        partial_tile.tile_options['series_configs'] = \
            partial_tile.tilewidget.get_series_configs(tile_config['series_spec_list'])

        cls._postprocess_tile_options(partial_tile.tile_options)

        partial_tile.tilewidget.postprocess_new_tile_options(tile_config)


        ### do update

        if skip_db:
            return partial_tile

        return Tile.insert_with_tile_options(dashboard_id, partial_tile.tile_options)


    @classmethod
    def _postprocess_tile_config(cls, tile_config):
        tile_config['series_spec_list'] = util.uniq_sameorder(tile_config['series_spec_list'])

        if 'tile_options' in tile_config and 'owner_id' in tile_config[ 'tile_options']:
            del tile_config['tile_options']['owner_id']

        if 'tile_options' in tile_config and 'report_id' in tile_config[ 'tile_options']:
            del tile_config['tile_options']['report_id']

        if 'tile_options' in tile_config and 'series_configs' in tile_config[
            'tile_options']:
            del tile_config['tile_options']['series_configs']

    @classmethod
    def _postprocess_tile_options(cls, tile_options):
        if tile_options.get('tags'):
            tile_options['tags'] = util.uniq_sameorder(tile_options['tags'])
            tile_options['tags'].sort()


    @classmethod
    def insert_with_tile_options(cls, dashboard_id, tile_options):
        """Insert and return a new :class:`Tile` specified using full :data:`tile_options`"""
        cls._postprocess_tile_options(tile_options)
        return cls.insert_with_tile_options_multi(dashboard_id, [tile_options])[0]

    @classmethod
    def insert_with_tile_options_multi(cls, dashboard_id, tile_options_list):
        """Insert and return multiple :class:`Tile` objects at once. A :class:`Tile`
         is created for each :data:`tile_options` from the list."""
        assert isinstance(tile_options_list, list)
        if not tile_options_list:
            return []

        for tile_options in tile_options_list:
            cls._postprocess_tile_options(tile_options)

        rows = c.dao.TileDAO.insert_multi(tile_options_list[0]['owner_id'], dashboard_id,
                                       [serialize.mjson(to) for to in tile_options_list])
        return [Tile(row) for row in rows]

    def insert_similar(self, tile_config):
        """Insert a tile using the same :attr:`owner_id`, :attr:`report_id`, :attr:`dashboard_id`
        values as the source tile"""
        return Tile.insert(self.owner_id, self.report_id, self.dashboard_id, tile_config)

    def delete(self):
        """Delete the tile"""
        self.delete_multi([self])

    @classmethod
    def delete_multi(cls, tile_list):
        """Delete a list of :class:`Tile` objects at once"""
        c.dao.TileDAO.delete_multi(tile_list)

    def copy(self, target_dashboard_id):
        """Copy the tile to a different dashboard. Returns the copied :class:`Tile` """
        return Tile.insert_with_tile_options(target_dashboard_id, self.tile_options)

    def is_master_tile(self):
        """Tells whether the tile is a master tile"""
        return bool(self.tile_options.get('tpcreator_uispec'))

    def has_sscs(self):
        """Tells whether this tile has an associated SSCS"""
        return bool(self.tile_options.get('sscs'))

    def get_master_tile_id(self):
        """For tiles created from a master tile, returns the master tile's ID.
        Otherwise, returns ``None``."""
        return nestedget(self.tile_options, 'tpcreator_data', 'master_tile_id')

    def series_specs(self):
        """Returns a list of :class:`SeriesSpec` objects associated with the tile"""
        return [sc['series_spec'] for sc in self.tile_options['series_configs']]

    def get_tile_config(self):
        """Creates a :data:`tile_config` from which the same tile can be created"""
        res = {
            'tw_type': self.tile_options['tw_type'],
            'series_spec_list': self.series_specs(),
            'tags': self.tile_options['tags'],
            'tile_options': {}
        }
        for k in self.tile_options:
            if k not in res and k not in ('owner_id', 'report_id', 'series_configs'):
                res['tile_options'][k] = self.tile_options[k]
        return res

    @classmethod
    def _validate_tile_config(cls, tile_config):
        if 'tw_type' in tile_config and tilewidgets.get_tilewidget_class(tile_config['tw_type']) is None:
            raise ValueError('Invalid tw_type %r' % tile_config['tw_type'])

        if len(tile_config.get('tags', [])) > mqeconfig.MAX_TAGS or \
            'tile_options' in tile_config and len(tile_config['tile_options'].get('tags', []))\
                        > mqeconfig.MAX_TAGS:
            raise ValueError('Too many tags specified')

        for ss in tile_config.get('series_spec_list', []):
            if not isinstance(ss, dataseries.SeriesSpec):
                raise ValueError('series_spec_list must be a list of SeriesSpec objects')

        tile_options = tile_config.get('tile_options')
        if tile_options:

            if tile_options.get('sscs'):
                if not isinstance(tile_options['sscs'], dataseries.SeriesSpec):
                    raise ValueError('sscs must a SeriesSpec object')

            if tile_options.get('tpcreator_uispec'):
                if not isinstance(tile_options['tpcreator_uispec'], list):
                    raise ValueError('tpcreator_uispec must be a list')
                for d in tile_options['tpcreator_uispec']:
                    if not isinstance(d, dict):
                        raise ValueError('Each element of tpcreator_uispec must be a dict')
                    if not 'tag' in d or not 'prefix' in d:
                        raise ValueError('tpcreator_uispec dict must have "tag" and "prefix" keys')
                    if not all(isinstance(v, basestring) for v in d.values()):
                        raise ValueError('tpcreator_uispec dict values must be strings')
                    if not d['tag'].startswith(d['prefix']):
                        raise ValueError('tpcreator_uispec\'s tag %r doesn\'t start with prefix %r' %
                                         (d['tag'], d['prefix']))


    def get_tile_data(self, limit=None):
        """Returns :attr:`tile_data` based on the tile's :attr:`tile_options`, possibly
        limiting the number of returned data points for each data series"""
        return self.tilewidget.get_tile_data(limit=limit)

    def get_new_tile_data(self, after_report_instance_id, limit=None):
        """Returns partial :attr:`tile_data` that can be merged into previously retrieved
        full :attr:`tile_data`. The :attr:`tile_data.series_data` is retrieved for
        report instances created after the specified report instance ID.

        The limit of the number of returned data points for each data series can be
        set using the ``limit`` argument.

        The returned dict contains the following keys: :attr:`tile_data.series_data`,
        :attr:`tile_data.extra_options`, :attr:`tile_data.series_data_as_rows`."""
        return self.tilewidget.get_new_tile_data(after_report_instance_id, limit)

    @staticmethod
    def from_rawjson(obj):
        return Tile(obj)

    def for_json(self):
        return {'tile_id': self.tile_id, 'dashboard_id': self.dashboard_id,
                'tile_options': self.tile_options}

    def __str__(self):
        return 'Tile(%s)' % {'tile_id': self.tile_id}
    __repr__ = __str__

    def key(self):
        if self.tile_id is None:
            raise ValueError('Cannot compute hash/cmp key for non-inserted tile')
        return (self.dashboard_id, self.tile_id)


def expire_tiles_without_data(tile_list, max_seconds_without_data, for_layout_id,
                              optimize_check=False):
    """Delete and detach tiles from a dashboard which don't have data for
    at least the specified time period.

    :param tile_list: a list of :class:`Tile` objects to expire, belonging to the same dashboard
    :param int max_seconds_without_data: the maximal age (specified in seconds) of the tile's
        data to avoid the expiration
    :param ~uuid.UUID for_layout_id: the version of the layout to perform the expiration
    :param bool optimize_check: whether to allow an optimization: checking only the latest
        report instance ID of a tile's report instead of full :data:`tile_data`. This will
        not work correctly when a tile's series are not present in the latest instance.
    :return: ``layout_id`` of the new layout if the operation was successful, ``None``
        otherwise
    """
    from mqe import layouts
    from mqe import tpcreator

    min_valid = datetime.datetime.utcnow() - datetime.timedelta(seconds=max_seconds_without_data)
    regular_tiles = [t for t in tile_list if not t.is_master_tile() \
                     and _should_expire_tile(t, min_valid, optimize_check)]
    master_tiles = [t for t in tile_list if t.is_master_tile() \
                    and _should_expire_tile(t, min_valid, optimize_check)]

    log.info('Will try to expire %s regular and %s master tiles out of %s passed',
             len(regular_tiles), len(master_tiles), len(tile_list))

    layout_id = for_layout_id

    if regular_tiles:
        repl_res = layouts.replace_tiles({tile: None for tile in regular_tiles},
                                        layout_id)
        if repl_res:
            log.info('Successfully expired regular tiles')
            layout_id = repl_res.new_layout.layout_id
        else:
            log.warn('Failed to expire regular tiles')

    master_repl = {}
    for master_tile in master_tiles:
        tpcreated_tile_ids = tpcreator.select_tpcreated_tile_ids(master_tile, layout_id, sort=True)
        if not tpcreated_tile_ids:
            continue
        new_master_base = Tile.select(master_tile.dashboard_id, tpcreated_tile_ids[0])
        if not new_master_base:
            log.warn('Could not select master tile replacement')
            continue
        new_master = tpcreator.make_master_from_tpcreated(master_tile, new_master_base)
        master_repl[master_tile] = new_master
        master_repl[new_master_base] = None

    if master_repl:
        repl_res = layouts.replace_tiles(master_repl, layout_id)
        if repl_res:
            log.info('Successfully expired master tiles')
            layout_id = repl_res.new_layout.layout_id
        else:
            log.warn('Failed to expire master tiles')

    if layout_id == for_layout_id:
        return None
    return layout_id


def _should_expire_tile(tile, min_valid, optimize_check):
    max_dt = None
    if optimize_check:
        latest_report_instance_id = tile.report.fetch_latest_instance_id(tile.tags)
        if latest_report_instance_id:
            max_dt = util.datetime_from_uuid1(latest_report_instance_id)
    else:
        tile_data = tile.tilewidget.get_tile_data()
        last_points = [sd['data_points'][-1] for sd in tile_data['series_data'] if sd['data_points']]
        if last_points:
            max_dt = max(p.dt for p in last_points)
    if max_dt is None:
        # if there's no data, use the tile's creation datetime
        max_dt = util.datetime_from_uuid1(tile.tile_id)
    return max_dt < min_valid

