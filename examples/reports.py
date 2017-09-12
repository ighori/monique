import tutorial
from tutorial import SECTION
import json
from mqe.reports import Report
from mqe.dataseries import SeriesSpec
from mqe.tiles import Tile

def main():
    vars = tutorial.main()
    owner_id = vars['owner_id']
    owner_dashboards = vars['owner_dashboards']
    dashboard = vars['dashboard']


    SECTION('tags')


    cpu_report = Report.select_or_insert(owner_id, 'cpu_usage')
    metrics = [
        ('user', 92.3),
        ('system', 3.4),
        ('io', 4.4),
    ]
    cpu_report.process_input(json.dumps(metrics), tags=['ip:192.168.1.18',
                                                        'warning'])

    tile_config_1 = {
        'tags': ['ip:192.168.1.18'],
        'series_spec_list': [
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
        ],
    }
    tile_1 = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id, tile_config_1)

    tile_config_2 = {
        'tags': ['ip:192.168.1.18', 'warning'],
        'series_spec_list': [
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
        ],
    }
    tile_2 = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id, tile_config_2)

if __name__ == '__main__':
    main()
