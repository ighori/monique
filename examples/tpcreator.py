import tutorial
from tutorial import SECTION
import json

def main():
    vars = tutorial.main()
    owner_id = vars['owner_id']
    owner_dashboards = vars['owner_dashboards']
    dashboard = vars['dashboard']


    SECTION('Using tags for identifying entities')


    from mqe.reports import Report

    cpu_report = Report.select_or_insert(owner_id, 'cpu_usage')
    metrics = [
        ('user', 42.3),
        ('system', 13.4),
        ('io', 8.4),
    ]
    cpu_report.process_input(json.dumps(metrics), tags=['ip:192.168.1.18'])


    SECTION('Creating a master tile')


    from mqe.dataseries import SeriesSpec
    from mqe.tiles import Tile
    from mqe.layouts import place_tile, Layout, replace_tiles

    dashboard = owner_dashboards.insert_dashboard('CPU')

    master_tile_config = {
        'tw_type': 'Range',
        'tags': ['ip:192.168.1.18'],
        'series_spec_list': [
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['system']}),
        ],
        'tile_options': {
            'tile_title': 'CPU usage',
            'tpcreator_uispec': [{'tag': 'ip:192.168.1.18', 'prefix': 'ip:'}]
        }
    }
    master_tile = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id,
                              master_tile_config)
    print place_tile(master_tile)


    SECTION('Creating tiles from a master tile')


    metrics = json.dumps(metrics)

    cpu_report.process_input(metrics, tags=['ip:192.168.1.30'])
    cpu_report.process_input(metrics, tags=['ip:192.168.2.51'])
    cpu_report.process_input(metrics, tags=['ip:192.168.2.51'])

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    for tile in layout.tile_dict:
        print tile.tags


    SECTION('Synchronizing options of tpcreated tiles')


    new_master_tile_config = {
        'tw_type': 'Range',
        'tags': ['ip:192.168.1.18'],
        'series_spec_list': [
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['system']}),
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['io']}),
        ],
        'tile_options': {
            'tile_title': 'CPU usage',
            'tpcreator_uispec': [{'tag': 'ip:192.168.1.18', 'prefix': 'ip:'}]
        }
    }
    new_master_tile = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id,
                                  new_master_tile_config)
    assert replace_tiles({master_tile: new_master_tile}, for_layout_id=None)

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    for tile in layout.tile_dict:
        print len(tile.get_tile_data()['series_data'])


    SECTION('Expiring tiles and promoting new masters')


    from mqe.tpcreator import make_master_from_tpcreated

    old_master = [tile for tile in layout.tile_dict if tile.is_master_tile()][0]
    new_chosen_master = [tile for tile in layout.tile_dict if tile.tags == ['ip:192.168.2.51']][0]
    assert not new_chosen_master.is_master_tile()

    new_master = make_master_from_tpcreated(old_master, new_chosen_master)
    res = replace_tiles({old_master: new_master, new_chosen_master: None}, for_layout_id=None)
    print 'replaced %d tiles' % len(res.tile_replacement)

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    tile = [tile for tile in layout.tile_dict if tile.tags == ['ip:192.168.2.51']][0]
    print tile.is_master_tile()


if __name__ == '__main__':
    main()
