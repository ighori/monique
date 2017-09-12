import uuid

from mqe.dashboards import OwnerDashboards
from mqe.tiles import Tile
from mqe.reports import Report
from mqe.dataseries import SeriesSpec


def main():
    owner_id = uuid.uuid4()
    report = Report.select_or_insert(owner_id, 'simple_report')
    res = report.process_input('1 2 3')
    assert res.report_instance

    owner_dashboards = OwnerDashboards(owner_id, insert_if_no_dashboards='Default Dashboard')
    assert owner_dashboards.dashboards

    tile_config = {
        'series_spec_list': [SeriesSpec(0, -1, {'op': 'eq', 'args': ['0']})],
    }
    tile = Tile.insert(owner_id, report.report_id, owner_dashboards.dashboards[0].dashboard_id,
                       tile_config)
    assert tile

    assert tile.get_tile_data()

    print 'Successfully created a dashboard with a tile'

    owner_dashboards.dashboards[0].delete()


if __name__ == '__main__':
    main()
