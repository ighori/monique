import tutorial
from tutorial import SECTION
from mqe.dataseries import SeriesSpec
from mqe.tiles import Tile
from mqe.layouts import place_tile, Layout

def main():
    vars = tutorial.main()
    points_report = vars['points_report']
    tile = vars['tile']
    owner_id = vars['owner_id']
    owner_dashboards = vars['owner_dashboards']


    SECTION('Auto-creating new data series')


    input = """\
    user_name is_active points
    john      true      128
    monique   true      210
    alex      true      12
    """
    points_report.process_input(input)

    series_names = [series_data['name'] for series_data in tile.get_tile_data()['series_data']]
    print series_names

    new_dashboard = owner_dashboards.insert_dashboard('Points')

    tile_config = {
        'tw_type': 'Range',
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'seconds_back': 86400,
            'tile_title': 'Points by user',
            'sscs': SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']})
        }
    }
    new_tile = Tile.insert(owner_id, points_report.report_id, new_dashboard.dashboard_id, tile_config)
    place_tile(new_tile)

    input = """\
    user_name is_active points
    john      true      133
    monique   true      220
    alex      true      18
    andrew    true      6
    """
    points_report.process_input(input)

    layout = Layout.select(owner_id, new_dashboard.dashboard_id)
    tile_id = layout.layout_dict.keys()[0]
    tile = Tile.select(new_dashboard.dashboard_id, tile_id)

    series_names = [series_data['name'] for series_data in tile.get_tile_data()['series_data']]
    print series_names


if __name__ == '__main__':
    main()