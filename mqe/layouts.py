import logging
from copy import deepcopy
import sys
import re

from mqe import util
from mqe.util import try_complete, NotCompleted
from mqe import mqeconfig
from mqe import serialize
from mqe.dbutil import gen_timeuuid
from mqe import c
from mqe.tiles import Tile


log = logging.getLogger('mqe.layouts')


MAX_LAYOUT_MODIFICATION_TRIES = 20


class Layout(object):
    """The layout definition for a dashboard - a list of tiles with associated
    :data:`visual_options`. An instance of a layout is immutable and is identified
    by the :attr:`layout_id` attribute.

    """

    def __init__(self, layout_dict=None):
        #: the owner of the layout
        self.owner_id = None
        #: the dashboard ID for which the layout is defined
        self.dashboard_id = None
        #: the :class:`~uuid.UUID` uniquely identifying the layout instance
        self.layout_id = None
        #: the definition of the dashboard's tiles and visual options:
        #: a dictionary mapping :class:`~mqe.tiles.Tile` IDs to :data:`visual_options`.
        self.layout_dict = layout_dict or {}

        self.layout_props = {'by_tile_id': {}}
        self._included_tiles = {}

    @staticmethod
    def select(owner_id, dashboard_id):
        """Selects the :class:`Layout` associated with the dashboard"""
        row = c.dao.LayoutDAO.select(owner_id, dashboard_id)
        if not row:
            return None
        res = Layout()
        res.layout_dict = dict(serialize.json_loads(row['layout_def']))
        res.layout_props = serialize.json_loads(row['layout_props']) if row['layout_props'] \
            else {'by_tile_id': {}}
        res.layout_props['by_tile_id'] = dict(res.layout_props['by_tile_id'])
        res.owner_id = owner_id
        res.dashboard_id = dashboard_id
        res.layout_id = row['layout_id']
        return res

    def set(self, owner_id=None, dashboard_id=None, old_layout_id=None):
        """Set a new layout definition for the dashboard (replacing the existing one), using
        the current content of the :attr:`layout_dict`. The parameters are optional - if not
        specified, the current values of :attr:`owner_id`, :attr:`dashboard_id` and
        :attr:`layout_id` are used.

        :param owner_id: the owner ID of the dashboard
        :param dashboard_id: the dashboard's ID
        :param old_layout_id: ``None`` if this should be a new layout definition
            for the dashboard, ``layout_id`` of the existing layout otherwise
        :return: a ``layout_id`` of a newly set layout if the operation was successful,
            ``None`` otherwise (ie. when the passed ``old_layout_id`` didn't match the
            version in the database)
        """
        owner_id = owner_id or self.owner_id
        if not owner_id:
            raise ValueError('owner_id not set in Layout and not passed as an argument')
        dashboard_id = dashboard_id or self.dashboard_id
        if not dashboard_id:
            raise ValueError('dashboard_id not set in Layout and not passed as an argument')
        old_layout_id = old_layout_id or self.layout_id

        # a layout def is a layout_dict serialized as a list of items. The list is
        # sorted by tile creation time (but this assumption should not be generally made).
        new_layout_def = serialize.mjson(sorted(self.layout_dict.items(),
                                                key=lambda (tile_id, vo): tile_id.time))


        # Merge old layout_props with new data

        old_layout_props_row = c.dao.LayoutDAO.select(owner_id, dashboard_id, ['layout_props'])

        if not old_layout_props_row and old_layout_id:
            return None

        if old_layout_props_row and old_layout_props_row['layout_props']:
            old_layout_props = serialize.json_loads(old_layout_props_row['layout_props'])
        else:
            old_layout_props = {'by_tile_id': []}

        by_tile_id = {}
        old_by_tile_id = dict(old_layout_props['by_tile_id'])

        tile_ids_to_fetch = []
        for tile_id in self.layout_dict:
            if tile_id in old_by_tile_id:
                by_tile_id[tile_id] = old_by_tile_id[tile_id]
            elif tile_id in self._included_tiles:
                by_tile_id[tile_id] = self.props_of_tile(self._included_tiles[tile_id])
            else:
                tile_ids_to_fetch.append(tile_id)

        tile_dict = Tile.select_multi(dashboard_id, tile_ids_to_fetch)
        for tile_id, tile in tile_dict.items():
            by_tile_id[tile.tile_id] = self.props_of_tile(tile)


        # Compute data for sscreator and tpcreator

        sscs_data = set()
        master_data = set()
        for props in by_tile_id.values():
            if props.get('sscs'):
                #sscs_data.add((props['report_id'], tuple(props['tags'])))
                sscs_data.add(props['report_id'])
            if props.get('is_master'):
                master_data.add(props['report_id'])

        new_layout_props = serialize.mjson({'by_tile_id': by_tile_id.items()})


        # Set the new layout

        new_layout_id = gen_timeuuid()
        res = c.dao.LayoutDAO.set(owner_id, dashboard_id, old_layout_id, new_layout_id,
                         new_layout_def, new_layout_props)
        if not res:
            log.info('Setting new layout failed')
            return None

        # Insert layout_by_report for sscs and tpcreator

        c.dao.LayoutDAO.insert_layout_by_report_multi(owner_id, sscs_data, [], 'sscs',
                                                   dashboard_id, new_layout_id)
        c.dao.LayoutDAO.insert_layout_by_report_multi(owner_id, master_data, [], 'tpcreator',
                                                   dashboard_id, new_layout_id)

        self.layout_id = new_layout_id

        return new_layout_id


    def include_tile_data(self, tile):
        self._included_tiles[tile.tile_id] = tile

    def get_tile_props(self, tile_id):
        props = self.layout_props['by_tile_id'].get(tile_id)
        if props:
            return props

        if tile_id in self._included_tiles:
            return self.props_of_tile(self._included_tiles[tile_id])

        return None

    def get_tpcreated_tile_ids(self, master_tile_id):
        res = []
        for tile_id, props in self.layout_props['by_tile_id'].items():
            if props.get('master_id') == master_tile_id:
                res.append(tile_id)
        return res

    def props_of_tile(self, tile):
        from mqe import tpcreator

        props = {}

        props['report_id'] = tile.tile_options['report_id']
        props['tags'] = tile.tile_options['tags']

        if tile.has_sscs():
            props['sscs'] = 1

        if tile.is_master_tile():
            props['is_master'] = 1
            props['tpcreator_spec'] = tpcreator.tpcreator_spec_from_tpcreator_uispec(
                tile.tile_options['tpcreator_uispec'])
        else:
            master_tile_id = tile.get_master_tile_id()
            if master_tile_id:
                props['master_id'] = master_tile_id

        return props

    @property
    def tile_dict(self):
        """The :attr:`layout_dict` converted to a dictionary mapping full
        :class:`mqe.tiles.Tile` objects to its :data:`visual_options`"""
        from mqe import tiles

        tile_by_id = tiles.Tile.select_multi(self.dashboard_id, self.layout_dict.keys())
        res = {}
        for tile_id, tile in tile_by_id.iteritems():
            res[tile] = self.layout_dict[tile_id]
        return res

    def copy(self):
        res = Layout()
        res.owner_id = self.owner_id
        res.dashboard_id = self.dashboard_id
        res.layout_id = self.layout_id
        res.layout_dict = {tile_id: vo.copy() for tile_id, vo in self.layout_dict.items()}
        res.layout_props = self.layout_props.copy()
        res._included_tiles = self._included_tiles.copy()
        return res



#### High-level API


def place_tile(tile, for_layout_id=None, size_of=None, initial_visual_options=None):
    """Place the tile on its dashboard's layout. The tile is put in the first available area. If no
    ``initial_visual_options`` are given, the size is taken from the
    :attr:`~mqe.mqeconfig.TILE_DEFAULT_WIDTH` and :attr:`~mqe.mqeconfig.TILE_DEFAULT_HEIGHT`
    config variables.

    :param ~mqe.tiles.Tile tile: the tile to place on the layout
    :param ~uuid.UUID size_of: if passed, use the width and the height of the tile having the ID
        equal to the parameter (the tile must be already placed on the dashboard)
    :param ~uuid.UUID for_layout_id: if ``None``, place the tile in a layout with any
        :attr:`~mqe.layouts.Layout.layout_id`, possibly making multiple tries. Otherwise,
        perform the operation only if the current :attr:`~mqe.layouts.Layout.layout_id`
        matches the parameter
    :param dict initial_visual_options: a subset of :data:`visual_options` that should be
        used, containing the ``width`` and the ``height`` keys.
    :return: a :class:`LayoutModificationResult` if the operation was successful, ``None``
        otherwise.
    """
    mods = [place_tile_mod(tile=tile, size_of=size_of,
                           initial_visual_options=initial_visual_options)]
    # repack if it's a tpcreated tile
    if tile.get_master_tile_id():
        mods.append(repack_mod())
    return apply_mods(mods, tile.owner_id, tile.dashboard_id, for_layout_id)

def detach_tile(tile, for_layout_id=None):
    """Remove the given tile from its dashboard's layout.

    :param ~mqe.tiles.Tile tile: the tile to detach
    :param ~uuid.UUID for_layout_id: if ``None``, detach the tile from a layout with any
        :attr:`~mqe.layouts.Layout.layout_id`, possibly making multiple tries. Otherwise,
        perform the operation only if the current :attr:`~mqe.layouts.Layout.layout_id`
        matches the parameter
    :return: a :class:`LayoutModificationResult` if the operation was successful, ``None``
        otherwise.
    """
    return replace_tiles({tile: None}, for_layout_id)

def repack(owner_id, dashboard_id, for_layout_id=None):
    """Compress the layout definition by removing free space present between tiles and
    group tpcreated tiles. The operation preserves the order of regular tiles. The
    tpcreated tiles are sorted by tag values.

    :param owner_id: the owner ID
    :param dashboard_id: the dashboard ID
    :param ~uuid.UUID for_layout_id: if ``None``, do repacking of a layout with any
        :attr:`~mqe.layouts.Layout.layout_id`, possibly making multiple tries. Otherwise,
        perform the operation only if the current :attr:`~mqe.layouts.Layout.layout_id`
        matches the parameter
    :return: a :class:`LayoutModificationResult` if the operation was successful, ``None``
        otherwise.
    """
    mods = [repack_mod()]
    return apply_mods(mods, owner_id, dashboard_id, for_layout_id)


def replace_tiles(old_to_new_tile_dict, for_layout_id, sync_tpcreated=True):
    """Replace tiles in the :attr:`~Layout.layout_dict`. The ``old_to_new_tile_dict``
    maps a :class:`~mqe.tiles.Tile` object already present in the layout to either
    a new :class:`~mqe.tiles.Tile` object acting as the replacement, or ``None``
    which means that the old tile should be detached.

    If the operations is successful, the tiles present in the ``old_to_new_tile_dict``
    as keys are deleted. Otherwise, the values of the dictionary are deleted.

    :param old_to_new_tile_dict: a dictionary defining the replacement
    :param for_layout_id:
    :param ~uuid.UUID for_layout_id: if ``None``, do the replacement in a layout with any
        :attr:`~mqe.layouts.Layout.layout_id`, possibly making multiple tries. Otherwise,
        perform the operation only if the current :attr:`~mqe.layouts.Layout.layout_id`
        matches the parameter
    :param sync_tpcreated: relevant if the ``old_to_new_tile_dict`` contains a master tile -
        tells whether the :data:`tile_options` of tpcreated tiles should be synchronized
        (by replacing the tpcreated tiles)
    :return: a :class:`LayoutModificationResult` if the operation was successful, ``None``
        otherwise.
    """
    sample_tile = util.first(old_to_new_tile_dict.iterkeys())
    dashboard_id = sample_tile.dashboard_id

    return apply_mods([replace_tiles_mod(old_to_new_tile_dict,
                                         sync_tpcreated=sync_tpcreated)],
                      sample_tile.owner_id, sample_tile.dashboard_id, for_layout_id)



### LayoutModification implementation


class LayoutModificationFailed(Exception):
    pass


class LayoutModificationImpossible(Exception):
    """The exception raised when a layout mod function can't perform its operation"""
    pass


class LayoutModification(object):
    """A :class:`LayoutModification` object is passed to a layout mod function.
    The function should modify the :attr:`layout` attribute and its ``layout_dict``. The result
    of the modification should be expressed by putting tiles into one of the
    attributes: :attr:`tile_replacement`, :attr:`new_tiles`, :attr:`detached_tiles`.
    """

    def __init__(self, modifications=[]):
        #: the :class:`Layout` to modify
        self.layout = None

        #: the result of a modification as a dict mapping old tiles to replacement tiles
        self.tile_replacement = {}
        #: the result of a modification as a dict mapping new tiles to their :data:`visual_options`
        self.new_tiles = {}
        #: the result of a modification as a list of tiles detached from the ``layout_dict``
        self.detached_tiles = []

        self.old_layout = None
        self.new_layout = None

        self.modifications = list(modifications)

    def add_modification(self, f):
        self.modifications.append(f)

    def _apply_modifications(self):
        for f in self.modifications:
            try:
                f(self)
            except LayoutModificationImpossible:
                return False
            for tile in self.tile_replacement.values() + self.new_tiles.keys():
                if tile:
                    self.layout.include_tile_data(tile)
        return True

    def _on_success(self):
        Tile.delete_multi(self.detached_tiles)
        Tile.delete_multi(self.tile_replacement.keys())

    def _on_failure(self):
        Tile.delete_multi(self.new_tiles.keys())
        Tile.delete_multi(self.tile_replacement.values())

        self.tile_replacement.clear()
        self.new_tiles.clear()
        self.detached_tiles[:] = []

    def any_changes_made(self):
        return self.tile_replacement or self.new_tiles or self.detached_tiles or \
            self.old_layout.layout_dict != self.layout.layout_dict

    def apply(self, owner_id, dashboard_id, for_layout_id, max_tries=MAX_LAYOUT_MODIFICATION_TRIES):
        if for_layout_id is not None:
            self.old_layout = Layout.select(owner_id, dashboard_id)
            if not self.old_layout:
                return None
            if self.old_layout.layout_id != for_layout_id:
                return None

            self.layout = self.old_layout.copy()
            applied = self._apply_modifications()
            if not applied:
                self._on_failure()
                return None

            if self.any_changes_made():
                new_layout_id = self.layout.set(owner_id, dashboard_id, for_layout_id)
                if not new_layout_id:
                    self._on_failure()
                    return None
            self.new_layout = self.layout
            self._on_success()
            return LayoutModificationResult(self)

        def do_apply():
            self.old_layout = Layout.select(owner_id, dashboard_id)
            if not self.old_layout:
                self.old_layout = Layout()

            self.layout = self.old_layout.copy()
            applied = self._apply_modifications()
            if not applied:
                self._on_failure()
                raise LayoutModificationFailed()

            if self.any_changes_made():
                new_layout_id = self.layout.set(owner_id, dashboard_id, self.layout.layout_id)
                if not new_layout_id:
                    self._on_failure()
                    return None
            self.new_layout = self.layout
            self._on_success()
            return LayoutModificationResult(self)

        def warn_about_failure(try_no):
            log.warn('Layout modification failed attempt %s/%s', try_no + 1, max_tries)

        log.info('Layout modification attempt using mods %s and up to %s tries',
                 [f.__name__ for f in self.modifications], max_tries)
        try:
            lmr = try_complete(max_tries, do_apply, after_fail=warn_about_failure)
            log.info('Layout modification successful, new layout set: %s',
                     lmr.old_layout.layout_id != lmr.new_layout.layout_id)
            return lmr
        except LayoutModificationFailed:
            log.warn('Layout modification failure: mod raised LayoutModificationImpossible')
            return None
        except NotCompleted:
            log.warn('Layout modification failure: all tries failed')
            return None


    def apply_for_noninserted_layout(self, layout):
        self.old_layout = layout
        self.layout = layout
        applied = self._apply_modifications()
        if not applied:
            self._on_failure()
            return None
        self._on_success()
        self.new_layout = layout
        return LayoutModificationResult(self)


class LayoutModificationResult(object):
    """The result of modifying a layout"""

    def __init__(self, layout_mod):
        #: The :class:`Layout` before the modification
        self.old_layout = layout_mod.old_layout
        #: The :class:`Layout` after the modification
        self.new_layout = layout_mod.new_layout

        #: The replaced tiles - a dictionary mapping an old :class:`~mqe.tiles.Tile` object
        #: to a new :class:`~mqe.tiles.Tile` object
        self.tile_replacement = layout_mod.tile_replacement
        #: The newly created tiles - a dictionary mapping a new :class:`~mqe.tiles.Tile`
        #: object to its :data:`visual_options`
        self.new_tiles = layout_mod.new_tiles
        #: The detached tiles as a list of :class:`~mqe.tiles.Tile` objects
        self.detached_tiles = layout_mod.detached_tiles

        #: A list of applied layout modifications - functions receiving a
        #: :class:`LayoutModification` and modifying a :attr:`Layout.layout_dict`
        self.modifications = layout_mod.modifications

    def __repr__(self):
        return 'LayoutModificationResult(new_tiles=%s, detached_tiles=%s, ' \
               'tile_replacement=%s)' % (self.new_tiles, self.detached_tiles,
                                         self.tile_replacement)
    __str__ = __repr__


def apply_mods(mods, owner_id, dashboard_id, for_layout_id,
               max_tries=MAX_LAYOUT_MODIFICATION_TRIES):
    """Apply a list of layout mod functions ``mods`` to a layout of the given dashboard.
    If ``for_layout_id`` is ``None``, the functions are applied up to ``max_tries``
    for the current versions of the layout. Otherwise, the functions are applied for the
    layout with ID ``for_layout_id``

    :return: a :class:`LayoutModificationResult` describing
        the modifications if the operation was successful, ``None`` otherwise"""
    layout_mod = LayoutModification(mods)
    return layout_mod.apply(owner_id, dashboard_id, for_layout_id, max_tries)

def apply_mods_for_noninserted_layout(mods, layout):
    layout_mod = LayoutModification(mods)
    return layout_mod.apply_for_noninserted_layout(layout)



### mod functions


def place_tile_mod(tile, size_of=None, initial_visual_options=None):
    """A layout mod placing a tile. See :func:`place_tile` for a description of parameters."""
    def do_place_tile(layout_mod):
        layout_dict = layout_mod.layout.layout_dict

        if tile.tile_id in layout_dict:
            return

        if not initial_visual_options:
            visual_options = {}
        else:
            visual_options = deepcopy(initial_visual_options)

        if size_of and layout_dict.get(size_of):
            visual_options['width'] = layout_dict.get(size_of).get('width',
                                                       mqeconfig.TILE_DEFAULT_WIDTH)
            visual_options['height'] = layout_dict.get(size_of).get('height',
                                                        mqeconfig.TILE_DEFAULT_HEIGHT)
        else:
            visual_options.setdefault('width', mqeconfig.TILE_DEFAULT_WIDTH)
            visual_options.setdefault('height', mqeconfig.TILE_DEFAULT_HEIGHT)

        visual_options = _xy_visual_options_first_match(layout_dict, visual_options)

        layout_dict[tile.tile_id] = visual_options

        layout_mod.new_tiles[tile] = visual_options

    return do_place_tile


def replace_tiles_mod(old_to_new_tile_dict, sync_tpcreated=True):
    """A layout mod replacing tiles. See :func:`place_tile` for a description of parameters."""

    from mqe import tpcreator

    if not old_to_new_tile_dict:
        return

    all_tiles = old_to_new_tile_dict.keys() + old_to_new_tile_dict.values()
    assert util.all_equal(t.dashboard_id for t in all_tiles if t)
    assert util.all_equal(t.owner_id for t in all_tiles if t)

    old_to_new_master_dict = {old: new for old, new in old_to_new_tile_dict.items()
                              if new and old.is_master_tile() and new.is_master_tile()}

    def do_replace_tiles(layout_mod):
        layout_dict = layout_mod.layout.layout_dict

        for old_tile, new_tile in old_to_new_tile_dict.items():
            if new_tile:
                layout_mod.tile_replacement[old_tile] = new_tile
            else:
                layout_mod.detached_tiles.append(old_tile)

        ### replace masters

        # skip replacing tpcreated which are replaced as regular tiles
        skip_replacements = set(t.tile_id for t in old_to_new_tile_dict)

        tpcreated_tile_replacement = {}
        for old_master, new_master in old_to_new_master_dict.items():
            tpcreated_tile_replacement.update(
                tpcreator.replace_tpcreated(layout_mod.layout, old_master, new_master, sync_tpcreated,
                                            skip_replacements=skip_replacements))

        layout_mod.tile_replacement.update(tpcreated_tile_replacement)

        ### replace ids

        all_tile_items = old_to_new_tile_dict.items() + tpcreated_tile_replacement.items()
        for old_tile, new_tile in all_tile_items:
            if old_tile.tile_id not in layout_mod.layout.layout_dict:
                # if any of the old tiles is not present in the layout dict,
                # the whole function fails
                raise LayoutModificationImpossible()

        for old_tile, new_tile in all_tile_items:
            if new_tile:
                layout_dict[new_tile.tile_id] = layout_dict[old_tile.tile_id]
            del layout_dict[old_tile.tile_id]

        # repack layout if deletes of master or tpcreated tiles were made,
        # pack upwards if regular deletes were made
        involves_deletes = any(not new_tile for old_tile, new_tile in all_tile_items)
        involves_tpcreator = any(not new_tile and (old_tile.is_master_tile() or \
                                                   old_tile.get_master_tile_id())
                                 for old_tile, new_tile in all_tile_items)
        if involves_tpcreator:
            repack_mod()(layout_mod)
        elif involves_deletes:
            pack_upwards_mod()(layout_mod)

    return do_replace_tiles




def _visual_options_intersect(visual_options, layout_dict_values):
    return any(util.rectangles_intersect(visual_options, vo) for vo in layout_dict_values)

def _visual_options_outside_of_screen(visual_options):
    if visual_options['x'] + visual_options['width'] > mqeconfig.DASHBOARD_COLS:
        return True
    if visual_options['y'] < 0:
        return True

def _visual_options_of_beside_or_below_last(layout_dict, visual_options):
    last = max(layout_dict.values(), key=lambda vo: (vo['y'] + vo['height'], vo['x']))
    beside_last_vo = dict(visual_options,
                          x=last['x'] + last['width'],
                          y=last['y'])
    if not _visual_options_outside_of_screen(beside_last_vo) \
            and not _visual_options_intersect(beside_last_vo, layout_dict.values()):
        return beside_last_vo
    below_last_vo = dict(visual_options,
                         x=0,
                         y=(max(vo['y'] + vo['height'] for vo in layout_dict.values())))
    return below_last_vo

def _gen_x_y(x=0, y=0):
    while True:
        yield (x, y)
        if x + 1 >= mqeconfig.DASHBOARD_COLS:
            x = 0
            y += 1
        else:
            x += 1

def _xy_visual_options_first_match(layout_dict, visual_options, start_x=0, start_y=0):
    layout_dict_values = layout_dict.values()
    for (x, y) in _gen_x_y(start_x, start_y):
        candidate = dict(visual_options, x=x, y=y)
        if _visual_options_outside_of_screen(candidate):
            continue
        if _visual_options_intersect(candidate, layout_dict_values):
            continue
        return candidate

def _sort_layout_items(layout_dict, by):
    if by == 'x':
        key = lambda (ud, vo): (vo['x'], vo['y'])
    else:
        key = lambda (ud, vo): (vo['y'], vo['x'])
    items = deepcopy(layout_dict.items())
    return sorted(items, key=key)

def pack_upwards_mod():
    """A layout mod packing the layout upwards. It deletes vertical space existing
    between tiles."""

    def do_pack_upwards(layout_mod):
        layout_dict_items = _sort_layout_items(layout_mod.layout.layout_dict, 'y')
        for i, (tile_id, vo) in enumerate(layout_dict_items):
            while True:
                if vo['y'] <= 0:
                    break
                vo['y'] -= 1
                if _visual_options_intersect(vo, [vo2 for tile_id, vo2 in util.without_idx(layout_dict_items, i)]):
                    vo['y'] += 1
                    break
        layout_mod.layout.layout_dict = dict(layout_dict_items)

    return do_pack_upwards


def pack_leftwards_mod():
    """A layout mod packing the layout leftwards. It deletes the leading horizontal space
    existing before the first tile in a row."""

    def do_pack_leftwards(layout_mod):
        layout_dict_items = _sort_layout_items(layout_mod.layout.layout_dict, 'x')
        for i, (ud, vo) in enumerate(layout_dict_items):
            while True:
                if vo['x'] <= 0:
                    break
                vo['x'] -= 1
                if _visual_options_intersect(vo, [vo2 for ud, vo2 in util.without_idx(layout_dict_items, i)]):
                    vo['x'] += 1
                    break
        layout_mod.layout.layout_dict = dict(layout_dict_items)

    return do_pack_leftwards


_DEFAULT_TAGS_SORT_KEY = ((-sys.maxint,), ('',))
_re_number = re.compile(r'\d+')

def tags_sort_key(tags):
    if not tags:
        return _DEFAULT_TAGS_SORT_KEY

    # the list of ints found within the "property value" of the tags
    num_keys = []
    # the list of strings found within whole tags, with numbers deleted
    str_keys = []
    for tag in tags:
        if ':' in tag:
            val_part = tag.split(':')[1]
            num_match = _re_number.search(val_part)
            if num_match:
                num_keys.append(int(num_match.group(0)))
                str_keys.append(_re_number.sub('', tag))
            else:
                str_keys.append(tag)
    if not num_keys:
        num_keys.append(-sys.maxint)
    if not str_keys:
        str_keys.append('')
    return (tuple(num_keys), tuple(str_keys))


def repack_mod(only_if_new_tiles_present=False):
    """A mod compressing and re-sorting the layout. See :func:`repack`.

    :param bool only_if_new_tiles_present: perform the repacking only if new tiles
        were placed in the layout.
    """

    def do_repack(layout_mod):
        if only_if_new_tiles_present and not layout_mod.new_tiles:
            return

        layout_dict_items = _sort_layout_items(layout_mod.layout.layout_dict, 'y')
        tile_id_to_index = {item[0]: i for i, item in enumerate(layout_dict_items)}
        by_tile_id = layout_mod.layout.layout_props['by_tile_id']

        def key((tile_id, vo)):
            """Returns (master_pos, (num_keys_tuple, str_keys_tuple))"""
            props = layout_mod.layout.get_tile_props(tile_id)
            master_id = props.get('master_id') if props else None
            if not master_id:
                return (tile_id_to_index[tile_id], _DEFAULT_TAGS_SORT_KEY)
            if master_id not in tile_id_to_index:
                #log.warn('No master in layout_dict')
                return (tile_id_to_index[tile_id], _DEFAULT_TAGS_SORT_KEY)
            return (tile_id_to_index[master_id], tags_sort_key(props.get('tags', [])))

        layout_dict_items.sort(key=key)

        res = {}
        start_x = 0
        start_y = 0
        for (ud, vo) in layout_dict_items:
            new_vo = deepcopy(vo)
            new_vo.pop('x', None)
            new_vo.pop('y', None)

            new_vo = _xy_visual_options_first_match(res, new_vo, start_x, start_y)

            res[ud] = new_vo
            start_x = new_vo['x']
            start_y = new_vo['y']
        layout_mod.layout.layout_dict = res

    return do_repack


