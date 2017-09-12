import tutorial
from tutorial import SECTION
from pprint import pprint
import uuid

def main():
    vars = tutorial.main()
    points_report = vars['points_report']
    owner_id = vars['owner_id']
    owner_dashboards = vars['owner_dashboards']
    dashboard = vars['dashboard']


    SECTION('Tile_config and tile_options')


    from mqe.dataseries import SeriesSpec
    from mqe.tiles import Tile

    tile_config = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'tile_title': 'Points by user',
        }
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id,
                       tile_config)
    pprint(tile.tile_options)

    tile_options2 = tile.tile_options.copy()
    tile_options2['owner_id'] = uuid.uuid4()
    tile2 = Tile.insert_with_tile_options(dashboard.dashboard_id, tile_options2)


    SECTION('Updating tile\'s config')


    from mqe.layouts import Layout, replace_tiles

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    tile = layout.tile_dict.keys()[0]

    tile_config = tile.get_tile_config()
    tile_config['tile_options']['seconds_back'] = 3600

    repl_tile = tile.insert_similar(tile_config)
    replace_tiles({tile: repl_tile}, for_layout_id=layout.layout_id)


    SECTION('Formatting tile data - tilewidgets and drawers')


    tile_config = {
        'tw_type': 'Range',
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'tile_title': 'Points by user',
            'drawer_type': 'ChartRangeDrawer',
            'colors': ['red', 'blue'],
            'seconds_back': 3600,
        }
    }


    SECTION('Updating tile data')


    tile_data = tile.get_tile_data()

    input = """\
    user_name is_active points
    john      true      144
    monique   true      241
    """
    res = points_report.process_input(input)

    last_report_instance_id = tile_data['series_data'][0]['data_points'][-1].rid
    new_tile_data = tile.get_new_tile_data(last_report_instance_id)


    SECTION('Managing colors')


    from mqe.dataseries import update_default_options

    tile_config = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'colors': ['blue', 'red'],
        }
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)
    print tile.get_tile_data()['combined_colors']

    update_default_options(tile)


    tile_config_2 = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
    }
    tile_2 = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config_2)
    print tile_2.get_tile_data()['combined_colors']


    SECTION('Data series names')


    series_spec = SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']})
    series_spec.set_name("monique's points")
    tile_config = {
       'series_spec_list': [series_spec]
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)
    print tile.get_tile_data()['series_data'][0]['name']


    SECTION('Creating custom tilewidgets and drawers')


    from mqe.tilewidgets import register_drawer_class, Drawer

    @register_drawer_class
    class MaxNumberDrawer(Drawer):

        drawer_type = 'MaxNumberDrawer'

        def process_tile_data(self, tile_data):
            max_number = 0
            for series_data in tile_data['series_data']:
                for point in series_data['data_points']:
                    if int(point.value) > max_number:
                        max_number = int(point.value)

            tile_data['max_number'] = max_number

    tile_config = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'drawer_type': 'MaxNumberDrawer',
        }
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)
    print tile.get_tile_data()['max_number']

if __name__ == '__main__':
    main()
