import logging
import copy
from collections import defaultdict

from mqe import tiles
from mqe.tiles import Tile
from mqe import c
from mqe import util
from mqe.util import try_complete, NotCompleted
from mqe import mqeconfig
from mqe.signals import fire_signal, layout_modified
from mqe import layouts


log = logging.getLogger('mqe.tpcreator')


TPCREATOR_SEPARATOR = ':'

MAX_TPCREATE_TRIES = 10


### Utilities


def suggested_tpcreator_uispec(tags):
    """Returns a suggested :data:`tpcreator_uispec` for the given list of tags (strings).
    When a tag contains the ``:`` character, the part until the character is used.
    Otherwise, the full tag value is included.
    """
    res = []
    for tag in tags:
        if TPCREATOR_SEPARATOR in tag:
            res.append({'tag': tag, 'prefix': _tpcreator_prefix(tag)})
        else:
            res.append({'tag': tag, 'prefix': tag})
    return res

def suggested_tpcreator_prefixes(tag):
    """Returns a list of suggested tag prefixes that can be included in a
    :data:`tpcreator_uispec`: an empty string, the part until the ``:`` character
    and the full tag value."""
    res = []
    res.append('')
    if TPCREATOR_SEPARATOR in tag:
        res.append(_tpcreator_prefix(tag))
    res.append(tag)
    return res

def tpcreator_spec_from_tpcreator_uispec(tpcreator_uispec):
    res = {
        'full_tags': [d['tag'] for d in tpcreator_uispec if d['tag'] == d['prefix']],
        'prefixes': util.uniq_sameorder(d['prefix'] for d in tpcreator_uispec if d['tag'] != d['prefix']),
    }
    res['prefixes'].sort(key=lambda p: len(p))
    return res

def tags_matching_tpcreator_spec(tpcreator_spec, tags):
    if not tpcreator_spec:
        return None
    if not tags:
        return None

    res = []
    for full_tag in tpcreator_spec['full_tags']:
        if full_tag not in tags:
            return None
        res.append(full_tag)

    for prefix in tpcreator_spec['prefixes']:
        prefix_matched = False
        for tag in tags:
            if tag.startswith(prefix):
                res.append(tag)
                prefix_matched = True
        if not prefix_matched:
            return None

    res = util.uniq_sameorder(res)
    res.sort()

    return res

def select_tpcreated_tile_ids(master_tile, for_layout_id=None, sort=False):
    """Return a list of tile IDs created from the master tile, possibly for the given
    layout version. If ``sort`` is ``True``, sort the IDs wrt. their visual position
    in a repacked layout (see :func:`.repack`)."""
    layout = layouts.Layout.select(master_tile.owner_id, master_tile.dashboard_id)
    if for_layout_id and layout.layout_id != for_layout_id:
        return None
    tile_ids = layout.get_tpcreated_tile_ids(master_tile.tile_id)
    if not sort:
        return tile_ids

    tpcreated_ids_tags = []
    for tile_id in layout.get_tpcreated_tile_ids(master_tile.tile_id):
        props = layout.get_tile_props(tile_id)
        if not props:
            continue
        tpcreated_ids_tags.append((tile_id, props.get('tags', [])))
    if not tpcreated_ids_tags:
        return None
    tpcreated_ids_tags.sort(key=lambda (tile_id, tags): layouts.tags_sort_key(tags))
    return [tile_id for tile_id, tags in tpcreated_ids_tags]


def _tpcreator_prefix(tag):
    return tag.split(TPCREATOR_SEPARATOR, 1)[0] + TPCREATOR_SEPARATOR



### TPCreator handling

def handle_tpcreator(owner_id, report_id, report_instance):
    """The method calls the TPCreator (see :ref:`guide_tpcreator`) for the given report instance,
    possibly creating new tiles from a master tile and altering dashboards'
    layouts. The signal :attr:`~mqe.signals.layout_modified` is issued for each
    modification."""
    rows = c.dao.LayoutDAO.select_layout_by_report_multi(owner_id, report_id, [], 'tpcreator',
                                                         mqeconfig.MAX_TPCREATORS_PER_REPORT)
    if not rows:
        log.debug('No layout_by_report tpcreator rows')
        return

    log.info('tpcreator is processing %s rows for owner_id=%s report_id=%s report_instance_id=%s',
             len(rows), owner_id, report_id, report_instance.report_instance_id)
    for row in rows:
        try:
            try_complete(MAX_TPCREATE_TRIES,
                         lambda: handle_tpcreator_row(row, report_instance),
                         lambda try_no: log.warn('tpcreator failed attempt %s/%s',
                                                 try_no, MAX_TPCREATE_TRIES))
        except NotCompleted:
            log.warn('tpcreator failure %s', row)
        else:
            log.info('tpcreator finished successfully')


def handle_tpcreator_row(row, report_instance):
    log.debug('Processing row %s', row)

    layout = layouts.Layout.select(row['owner_id'], row['dashboard_id'])
    if not layout:
        log.warn('No layout')
        return True

    layout_id = layout.layout_id

    tpcreator_spec_by_master_id, tpcreated_tags_by_master_id = _get_tpcreator_data(layout, row['report_id'])
    log.debug('tpcreator data: %s, %s', tpcreator_spec_by_master_id, tpcreated_tags_by_master_id)

    if not tpcreator_spec_by_master_id:
        log.info('Deleting obsoleted layout_by_report tpcreator row')
        c.dao.LayoutDAO.delete_layout_by_report(row['owner_id'], row['report_id'], row['tags'],
                                                row['label'], row['dashboard_id'], row['layout_id'])
        return True

    for master_id, tpcreator_spec in tpcreator_spec_by_master_id.items():
        log.debug('Processing master_id=%s tpcreator_spec=%s', master_id, tpcreator_spec)
        tpcreated_tags = tpcreated_tags_by_master_id[master_id]
        if len(tpcreated_tags) >= mqeconfig.MAX_TPCREATED:
            log.warn('Too many tpcreated for master_id=%s', master_id)
            continue

        matching_tags = tags_matching_tpcreator_spec(tpcreator_spec, report_instance.all_tags)
        if not matching_tags:
            log.debug('No tags match the tpcreator_spec')
            continue
        if tuple(matching_tags) in tpcreated_tags:
            log.debug('A tpcreated tile already exists for the matched tags %s', matching_tags)
            continue

        master_tile = Tile.select(row['dashboard_id'], master_id)
        if not master_tile:
            log.warn('No master_tile')
            continue

        new_tile_options = _tile_options_of_tpcreated(master_tile, tpcreator_spec, matching_tags)
        new_tile = Tile.insert_with_tile_options(master_tile.dashboard_id, new_tile_options)
        log.info('tpcreator created new tile with tags %s for report_id=%s', matching_tags,
                 row['report_id'])
        mres = layouts.place_tile(new_tile, size_of=master_tile.tile_id,
                                  for_layout_id=layout_id)
        if not mres:
            log.debug('Placing new tile failed')
            return False

        fire_signal(layout_modified,
                    reason='tpcreator',
                    layout_modification_result=mres)

        layout_id = mres.new_layout.layout_id

    return True


def _get_tpcreator_data(layout_data, report_id):
    tpcreator_spec_by_master_id = {}
    tpcreated_tags_by_master_id = defaultdict(set)

    for tile_id, props in layout_data.layout_props['by_tile_id'].items():
        if props['report_id'] != report_id:
            continue
        if props.get('is_master'):
            tpcreator_spec_by_master_id[tile_id] = props['tpcreator_spec']
            tpcreated_tags_by_master_id[tile_id].add(tuple(sorted(props['tags'])))
            continue
        master_id = props.get('master_id')
        if not master_id:
            continue
        tpcreated_tags_by_master_id[master_id].add(tuple(sorted(props['tags'])))

    return tpcreator_spec_by_master_id, tpcreated_tags_by_master_id


def _tile_options_of_tpcreated(master_tile, tpcreator_spec, tags):
    partial_new_tile = Tile.insert(master_tile.owner_id, master_tile.report_id,
        master_tile.dashboard_id, skip_db=True, tile_config={
            'tw_type': master_tile.tile_options['tw_type'],
            'tags': tags,
            'series_spec_list': master_tile.series_specs(),
            'tile_options': {k: v for k, v in master_tile.tile_options.items()
                                  if k not in {'series_configs', 'tags', 'tile_title'}}
    })
    new_tile_options = partial_new_tile.tile_options
    if 'tpcreator_data' not in new_tile_options:
        new_tile_options['tpcreator_data'] = {}

    tile_title = master_tile.tile_options.get('tile_title')
    if tile_title:
        postfix = master_tile.tilewidget.generate_tile_title_postfix()
        if postfix:
            tile_title = tile_title.replace(postfix, '').strip()
        new_tile_options['tpcreator_data']['tile_title_base'] = tile_title

    new_tile_options.pop('tpcreator_uispec', None)
    new_tile_options['tpcreator_data']['master_tile_id'] = master_tile.tile_id
    new_tile_options['tpcreator_data']['master_tpcreator_uispec'] = copy.deepcopy(master_tile.tile_options['tpcreator_uispec'])
    new_tile_options['tpcreator_data']['master_tpcreator_spec'] = tpcreator_spec
    return new_tile_options

def _sync_tpcreator_data(master_tile, tpcreated, tpcreator_spec):
    new_tile_options = copy.deepcopy(tpcreated.tile_options)
    if 'tpcreator_data' not in new_tile_options:
        new_tile_options['tpcreator_data'] = {}

    new_tile_options['tpcreator_data']['master_tile_id'] = master_tile.tile_id
    new_tile_options['tpcreator_data']['master_tpcreator_uispec'] = copy.deepcopy(master_tile.tile_options['tpcreator_uispec'])
    new_tile_options['tpcreator_data']['master_tpcreator_spec'] = tpcreator_spec
    return new_tile_options


### Other functions manipulating the master and tpcreated tiles


def replace_tpcreated(layout, old_master, new_master, sync_tpcreated=True,
                      skip_replacements=set()):
    tpcreator_spec = layout.layout_props['by_tile_id'][old_master.tile_id]['tpcreator_spec']

    tpcreated_tile_id_list = layout.get_tpcreated_tile_ids(old_master.tile_id)
    if skip_replacements:
        tpcreated_tile_id_list = [tid for tid in tpcreated_tile_id_list
                                  if tid not in skip_replacements]
    tpcreated_tiles = tiles.Tile.select_multi(old_master.dashboard_id,
                                              tpcreated_tile_id_list)
    tpcreated_tiles_new_tos = []
    for tile in tpcreated_tiles.itervalues():
        if sync_tpcreated:
            to = _tile_options_of_tpcreated(new_master, tpcreator_spec, tile.tags)
        else:
            to = _sync_tpcreator_data(new_master, tile, tpcreator_spec)
        tpcreated_tiles_new_tos.append(to)

    tile_replacement = {}
    tpcreated_tiles_repl = Tile.insert_with_tile_options_multi(old_master.dashboard_id,
                                                               tpcreated_tiles_new_tos)
    for tt, ttr in zip(tpcreated_tiles.values(), tpcreated_tiles_repl):
        tile_replacement[tt] = ttr

    return tile_replacement


def make_master_from_tpcreated(old_master, tpcreated):
    """Based on an old master |Tile| ``old_master``, creates a new master |Tile| from
    ``tpcreated`` |Tile| which must be a |Tile| tpcreated from ``old_master``."""
    assert old_master.is_master_tile()
    assert not tpcreated.is_master_tile()

    new_master_tile_options = copy.deepcopy(tpcreated.tile_options)
    new_master_tile_options['tpcreator_uispec'] = old_master.tile_options['tpcreator_uispec']
    del new_master_tile_options['tpcreator_data']

    return Tile.insert_with_tile_options(old_master.dashboard_id, new_master_tile_options)


def synchronize_sizes_of_tpcreated_mod(master_tile):
    """A layout mod the synchronizes sizes of tpcreated tiles"""
    def do_synchronize(layout_mod):
        master_vo = layout_mod.layout.layout_dict.get(master_tile.tile_id)
        if not master_vo:
            log.warn('No master_tile_id in layout_data.layout_dict for %s', master_tile)
            raise layouts.LayoutModificationImpossible()

        changed = False
        for tile_id, vo in layout_mod.layout.layout_dict.iteritems():
            props = layout_mod.layout.get_tile_props(tile_id)
            if tile_id != master_tile.tile_id and \
                            props.get('master_id') == master_tile.tile_id:
                if vo['width'] != master_vo['width']:
                    vo['width'] = master_vo['width']
                    changed = True
                if vo['height'] != master_vo['height']:
                    vo['height'] = master_vo['height']
                    changed = True
        if not changed:
            raise layouts.LayoutModificationImpossible()
        layouts.repack_mod()(layout_mod)
        layouts.pack_upwards_mod()(layout_mod)

    return do_synchronize


def synchronize_sizes_of_tpcreated(master_tile, for_layout_id):
    """Changes the sizes of tpcreated tiles to match the size of the master tile.
    Returns :class:`~mqe.layouts.LayoutModificationResult`.
    """
    return layouts.apply_mods([synchronize_sizes_of_tpcreated_mod(master_tile)],
              master_tile.owner_id, master_tile.dashboard_id, for_layout_id)


