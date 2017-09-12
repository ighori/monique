import logging

from mqe import mqeconfig
from mqe import util
from mqe.util import try_complete, NotCompleted
from mqe import tiles
from mqe import c
from mqe.signals import fire_signal, layout_modified
from mqe import layouts


log = logging.getLogger('mqe.sscs')


MAX_SSCS_TRIES = 10


def handle_sscreator(owner_id, report_id, report_instance):
    """The method calls the SSCS (see :ref:`guide_sscreator`) for the given report instance,
    possibly creating new series definitions for tiles and altering dashboards'
    layouts. The signal :attr:`~mqe.signals.layout_modified` is issued for each
    modification."""
    rows = c.dao.LayoutDAO.select_layout_by_report_multi(owner_id, report_id, [], 'sscs',
                                            mqeconfig.MAX_DASHBOARDS_WITH_SSCS_PER_REPORT)
    if not rows:
        log.debug('No layout_by_report sscs rows')
        return

    log.info('sscreator is processing %s rows for owner_id=%s report_id=%s report_instance_id=%s',
             len(rows), owner_id, report_id, report_instance.report_instance_id)
    for row in rows:
        try:
            try_complete(MAX_SSCS_TRIES,
                         lambda: handle_sscreator_row(row, report_instance),
                         lambda try_no: log.warn('sscreator failed attempt %s/%s', try_no, MAX_SSCS_TRIES))
        except NotCompleted:
            log.warn('sscreator failure %s', row)
        else:
            log.info('sscreator finished successfully')


def handle_sscreator_row(row, report_instance):
    log.debug('Processing row %s', row)

    layout = layouts.Layout.select(row['owner_id'], row['dashboard_id'])
    if not layout:
        log.warn('No layout')
        return True

    tile_ids = []
    any_sscs_found = False
    for tile_id, props in layout.layout_props['by_tile_id'].items():
        if not props.get('sscs'):
            continue
        if props['report_id'] != row['report_id']:
            continue
        any_sscs_found = True
        if not set(props['tags']).issubset(set(report_instance.all_tags)):
            continue
        tile_ids.append(tile_id)

    if not any_sscs_found:
        log.info('Deleting obsoleted layout_by_report sscs row')
        c.dao.LayoutDAO.delete_layout_by_report(row['owner_id'], row['report_id'], row['tags'],
                                row['label'], row['dashboard_id'], row['layout_id'])
        return True

    if not tile_ids:
        log.debug('No tiles to process')
        return True

    # this sorting is only for the predictability of processing
    tile_ids.sort(key=lambda ud: ud.time)

    tile_list = tiles.Tile.select_multi(row['dashboard_id'], tile_ids).values()
    tile_replacement = {}
    for tile in tile_list:
        new_tile = create_new_series(tile, report_instance)
        if not new_tile:
            continue
        tile_replacement[tile] = new_tile

    if not tile_replacement:
        return True

    log.debug('Replacing %s tiles with new series', len(tile_replacement))
    repl_res = layouts.replace_tiles(tile_replacement, layout.layout_id,
                                     sync_tpcreated=False)
    if not repl_res:
        log.debug('Replacing the tiles failed')
        return False

    fire_signal(layout_modified, reason='sscreator', layout_modification_result=repl_res)

    return True


def create_new_series(tile, report_instance):
    """Create new series based on :data:`tile_config.sscs` present in the ``tile`` and the
    rows present in the ``report_instance``. The function returns a new |Tile| if new
    series were created, ``None`` otherwise."""
    log.debug('Creating new series for %s %s', tile, report_instance)
    sscs_def = tile.tile_options.get('sscs')
    if not sscs_def:
        log.warn('No sscs in tile_options')
        return None

    if sscs_def.actual_data_colno(report_instance) is None or \
       sscs_def.actual_filtering_colno(report_instance) is None:
        log.debug('sscs_def specifies invalid colnos')
        return None

    ss_list = tile.series_specs()
    ss_conforming = [ss for ss in ss_list
                     if ss.actual_data_colno(report_instance) == sscs_def.actual_data_colno(report_instance) \
                     and ss.actual_filtering_colno(report_instance) == sscs_def.actual_filtering_colno(report_instance) \
                     and ss.params['filtering_expr']['op'] == 'eq']
    filtering_vals = util.flatten(ss.params['filtering_expr']['args'] for ss in ss_conforming)
    filtering_vals_set = set(filtering_vals)

    if sscs_def.actual_filtering_colno(report_instance) == -1:
        string_vals = [str(i) for i in report_instance.table.value_idxs]
    else:
        string_vals = [ev.to_string_key() for i, ev in report_instance.table.value_column(sscs_def.actual_filtering_colno(report_instance))]
    new_ss_list = []
    for s in string_vals:
        if s and s not in filtering_vals_set:
            new_ss = sscs_def.copy(without_params=['name'])
            new_ss.params['filtering_expr']['args'] = [s]
            new_ss_list.append(new_ss)
    if not new_ss_list:
        log.debug('No new series specs')
        return None
    if len(ss_list) + len(new_ss_list) > mqeconfig.MAX_SERIES:
        log.warn('Too many series')
        return None

    log.debug('Creating new series specs %s', new_ss_list)

    tile_config = tile.get_tile_config()
    tile_config['series_spec_list'].extend(new_ss_list)

    new_tile = tile.insert_similar(tile_config)

    return new_tile


