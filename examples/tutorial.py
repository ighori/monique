from pprint import pprint

def SECTION(name):
    print('\n>>> SECTION %s\n' % name)

def main():
    SECTION('Creating a report and report instances')


    import uuid
    from mqe.reports import Report

    owner_id = uuid.uuid4()

    simple_report = Report.insert(owner_id, 'simple')
    res = simple_report.process_input('10 20')
    print res.report_instance.table

    points_report = Report.insert(owner_id, 'points')
    input = """\
    user_name is_active points
    john      true      128
    monique   true      210
    """
    res = points_report.process_input(input)
    print res.report_instance.table


    SECTION('Creating a dashboard and a tile')


    from mqe.dashboards import OwnerDashboards

    owner_dashboards = OwnerDashboards(owner_id)
    dashboard = owner_dashboards.insert_dashboard('My Dashboard')

    from mqe.dataseries import SeriesSpec
    tile_config = {
        'tw_type': 'Range',
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'seconds_back': 86400,
            'tile_title': 'Points by user',
        }
    }

    from mqe.tiles import Tile
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)
    pprint(tile.get_tile_data())


    SECTION('Placing a tile in a dashboard layout')


    from mqe.layouts import place_tile

    res = place_tile(tile)
    if not res:
        raise ValueError('Placing the tile unsuccessful')

    from mqe.layouts import Layout

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    pprint(layout.layout_dict)


    def render_dashboard(owner_id, dashboard):
        print 'Rendering dashboard %r' % dashboard.dashboard_name
        layout = Layout.select(owner_id, dashboard.dashboard_id)
        for tile, visual_options in layout.tile_dict.items():
            tile_data = tile.get_tile_data()
            print 'Rendering tile %r at position %s/%s' % (
                tile_data['generated_tile_title'], visual_options['x'], visual_options['y'])
            # render tile_data['series_data']

    render_dashboard(owner_id, dashboard)

    return locals()


if __name__ == '__main__':
    main()