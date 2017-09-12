import unittest
import json
import datetime
import uuid
from collections import OrderedDict

from mqe import tpcreator
from mqe import dataseries
from mqe.tiles import Tile
from mqe import tilewidgets
from mqe import mqeconfig
from mqe import layouts
from mqe import util

from mqe.tests.tutil import new_report_data


class GetDataTest(unittest.TestCase):

    def test_range__chart_range_drawer(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        tile_config = {
            'tw_type': 'Range',
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'Points by user',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)
        self.assertEqual(tile.tile_options['drawer_type'], 'ChartRangeDrawer')

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['ip:192.168.1.1'], extra_ri_data={'ed': 88})

        tile = rd.only_tile_from_layout()
        data = tile.get_tile_data()

        self.assertEqual('points (monique, robert3)', data['generated_tile_title'])
        self.assertEqual('[ip:192.168.1.1]', data['generated_tile_title_postfix'])
        self.assertEqual(0, data['extra_options']['y_axis_min'])
        self.assertIsInstance(data['fetched_from_dt'], datetime.datetime)
        self.assertIsInstance(data['fetched_to_dt'], datetime.datetime)
        self.assertEqual({'ed': 88}, data['latest_extra_ri_data'])
        self.assertEqual('points', data['report_name'])
        self.assertEqual(2, len(data['series_data']))
        for sd in data['series_data']:
            self.assertIsInstance(sd['series_id'], uuid.UUID)
            self.assertEqual('points', data['common_header'])
            self.assertTrue(sd['data_points'])
            for dp in sd['data_points']:
                self.assertIsInstance(dp, tilewidgets.DataPoint)
        self.assertEqual(mqeconfig.DEFAULT_COLORS[:2], data['combined_colors'])
        self.assertEqual('points', data['common_header'])

        return tile

    def test_range__text_table_drawer(self):
        tile = self.test_range__chart_range_drawer()
        tc = tile.get_tile_config()
        self.assertEqual('ChartRangeDrawer', tc['tile_options']['drawer_type'])

        tc['tile_options']['drawer_type'] = 'TextTableDrawer'
        tile = tile.insert_similar(tc)

        data = tile.get_tile_data()

        self.assertEqual('points (monique, robert3)', data['generated_tile_title'])
        self.assertEqual('[ip:192.168.1.1]', data['generated_tile_title_postfix'])
        # self.assertEqual(0, data['extra_options']['y_axis_min'])
        self.assertIsInstance(data['fetched_from_dt'], datetime.datetime)
        self.assertIsInstance(data['fetched_to_dt'], datetime.datetime)
        self.assertEqual({'ed': 88}, data['latest_extra_ri_data'])
        self.assertEqual('points', data['report_name'])
        self.assertEqual(2, len(data['series_data']))
        for sd in data['series_data']:
            self.assertIsInstance(sd['series_id'], uuid.UUID)
            self.assertEqual('points', data['common_header'])
            self.assertTrue(sd['data_points'])
            for dp in sd['data_points']:
                self.assertIsInstance(dp, tilewidgets.DataPoint)
        self.assertEqual(2, len(data['combined_colors']))
        self.assertEqual('points', data['common_header'])

        self.assertEqual(4, len(data['series_data_as_rows']))

    def test_single__chart_single_drawer(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        tile_config = {
            'tw_type': 'Single',
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['mike'])),
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['nonexisting'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'Points by user',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)
        self.assertEqual(tile.tile_options['drawer_type'], 'ChartSingleDrawer')

        d = [
            OrderedDict([('user_name', 'mike'), ('is_active', True), ('points', 32)]),
            OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 210)]),
        ]
        rd.report.process_input(json.dumps(d), tags=['ip:192.168.1.1'], extra_ri_data={'ed': 88})

        tile = rd.only_tile_from_layout()
        data = tile.get_tile_data()

        self.assertEqual('points (mike, nonexisting, robert3)', data['generated_tile_title'])
        self.assertEqual('[ip:192.168.1.1]', data['generated_tile_title_postfix'])
        self.assertEqual(0, data['extra_options']['y_axis_min'])
        self.assertNotIn('fetched_from_dt', data)
        self.assertEqual({'ed': 88}, data['latest_extra_ri_data'])
        self.assertEqual('points', data['report_name'])
        self.assertEqual(3, len(data['series_data']))
        for i, sd in enumerate(data['series_data']):
            self.assertIsInstance(sd['series_id'], uuid.UUID)
            self.assertEqual('points', data['common_header'])
            if i == 1:
                self.assertFalse(sd['data_points'])
            else:
                self.assertEqual(1, len(sd['data_points']))
            for dp in sd['data_points']:
                self.assertIsInstance(dp, tilewidgets.DataPoint)
        self.assertEqual(mqeconfig.DEFAULT_COLORS[:3], data['combined_colors'])
        self.assertEqual('points', data['common_header'])

        return tile

    def test_single__text_single_drawer(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        tile_config = {
            'tw_type': 'Single',
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['mike'])),
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['nonexisting'])),
            ],
            'tile_options': {
                'drawer_type': 'TextSingleDrawer',
                'seconds_back': 600,
                'tile_title': 'Points by user',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)
        self.assertEqual(tile.tile_options['drawer_type'], 'TextSingleDrawer')

        d = [
            OrderedDict([('user_name', 'mike'), ('is_active', True), ('points', 32)]),
            OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)]),
        ]
        rd.report.process_input(json.dumps(d), tags=['ip:192.168.1.1'], extra_ri_data={'ed': 88})

        tile = rd.only_tile_from_layout()
        data = tile.get_tile_data()

        self.assertEqual('points (mike, nonexisting, robert3)', data['generated_tile_title'])
        self.assertEqual('[ip:192.168.1.1]', data['generated_tile_title_postfix'])
        self.assertNotIn('extra_options', data)
        self.assertNotIn('fetched_from_dt', data)
        self.assertEqual({'ed': 88}, data['latest_extra_ri_data'])
        self.assertEqual('points', data['report_name'])
        self.assertEqual(3, len(data['series_data']))
        for i, sd in enumerate(data['series_data']):
            self.assertIsInstance(sd['series_id'], uuid.UUID)
            self.assertEqual('points', data['common_header'])
            if i == 1:
                self.assertFalse(sd['data_points'])
            else:
                self.assertEqual(1, len(sd['data_points']))
            for dp in sd['data_points']:
                self.assertIsInstance(dp, tilewidgets.DataPoint)
        self.assertEqual(3, len(data['combined_colors']))
        self.assertEqual('points', data['common_header'])

        return tile

    def test_filling_default_options(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        for tw_type in ('Range', 'Single'):
            ss1 = dataseries.SeriesSpec(2, 0, dict(op='eq', args=['mike']))
            ss1.promote_colnos_to_headers(rd.instances[-1])
            ss1.set_name('mikepoints')
            tile_config = {
                'tw_type': tw_type,
                'tags': ['ip:192.168.1.1'],
                'series_spec_list': [
                    ss1,
                    dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                ],
                'tile_options': {
                    'colors': ['red', 'blue', 'pink', 'cyan'],
                    'seconds_back': 600,
                    'tile_title': 'Points by user',
                    'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                }
            }

            tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
            dataseries.update_default_options(tile)
            data = tile.get_tile_data()
            self.assertEqual(['red', 'blue'], data['combined_colors'])

            tile_config2 = {
                'tw_type': tw_type,
                'tags': ['ip:192.168.1.1'],
                'series_spec_list': [
                    dataseries.guess_series_spec(rd.report, rd.instances[-1], 1, 2),
                    dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                ],
            }
            self.assertEqual('mikepoints', tile_config2['series_spec_list'][0].name())
            tile2 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
            dataseries.update_default_options(tile2)
            data2 = tile2.get_tile_data()
            self.assertEqual('mikepoints', data2['series_data'][0]['name'])
            self.assertEqual(['red', 'blue'], data2['combined_colors'])



class GetNewDataTest(unittest.TestCase):

    def test_range__chart_range_drawer(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        tile_config = {
            'tw_type': 'Range',
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'Points by user',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)
        self.assertEqual(tile.tile_options['drawer_type'], 'ChartRangeDrawer')

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        pr = rd.report.process_input(json.dumps(d), tags=['ip:192.168.1.1'], extra_ri_data={'ed': 88})

        tile = rd.only_tile_from_layout()

        data = tile.get_new_tile_data(pr.report_instance.report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual([], data['series_data'][1]['data_points'])

        data = tile.get_new_tile_data(rd.instances[-1].report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(0, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-2].report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(0, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-3].report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(1, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(265, data['series_data'][0]['data_points'][0].value)
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-4].report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(2, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(220, data['series_data'][0]['data_points'][0].value)
        self.assertEqual(265, data['series_data'][0]['data_points'][1].value)
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        self.assertEqual(tile.get_new_tile_data(util.min_uuid_with_dt(
            rd.instances[0].created)), tile.get_new_tile_data(None))

        return tile, pr, rd


    def test_range__text_table_drawer(self):
        tile, pr, rd = self.test_range__chart_range_drawer()

        tc = tile.get_tile_config()
        self.assertEqual('ChartRangeDrawer', tc['tile_options']['drawer_type'])

        tc['tile_options']['drawer_type'] = 'TextTableDrawer'
        tile = tile.insert_similar(tc)
        self.assertEqual('TextTableDrawer', tc['tile_options']['drawer_type'])

        data = tile.get_tile_data()

        data = tile.get_new_tile_data(pr.report_instance.report_instance_id)
        self.assertIn('series_data_as_rows', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual([], data['series_data'][1]['data_points'])

        data = tile.get_new_tile_data(rd.instances[-1].report_instance_id)
        self.assertIn('series_data_as_rows', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(0, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-2].report_instance_id)
        self.assertIn('series_data_as_rows', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(0, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-3].report_instance_id)
        self.assertIn('series_data_as_rows', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(1, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(265, data['series_data'][0]['data_points'][0].value)
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-4].report_instance_id)
        self.assertIn('series_data_as_rows', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual(2, len(data['series_data'][0]['data_points']))
        self.assertEqual(1, len(data['series_data'][1]['data_points']))
        self.assertEqual(220, data['series_data'][0]['data_points'][0].value)
        self.assertEqual(265, data['series_data'][0]['data_points'][1].value)
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        self.assertEqual(tile.get_new_tile_data(util.min_uuid_with_dt(
            rd.instances[0].created)), tile.get_new_tile_data(None))

    def test_single__chart_single_drawer(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        tile_config = {
            'tw_type': 'Single',
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'Points by user',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                'drawer_type': 'ChartSingleDrawer'
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)
        self.assertEqual(tile.tile_options['drawer_type'], 'ChartSingleDrawer')

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        pr = rd.report.process_input(json.dumps(d), tags=['ip:192.168.1.1'], extra_ri_data={'ed': 88})

        tile = rd.only_tile_from_layout()
        data = tile.get_new_tile_data(pr.report_instance.report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual([], data['series_data'][1]['data_points'])

        data = tile.get_new_tile_data(rd.instances[-1].report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-3].report_instance_id)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(None)
        self.assertIn('extra_options', data)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        return tile

    def test_single__text_single_drawer(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        tile_config = {
            'tw_type': 'Single',
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'Points by user',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                'drawer_type': 'TextSingleDrawer'
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)
        self.assertEqual(tile.tile_options['drawer_type'], 'TextSingleDrawer')

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        pr = rd.report.process_input(json.dumps(d), tags=['ip:192.168.1.1'], extra_ri_data={'ed': 88})

        tile = rd.only_tile_from_layout()
        data = tile.get_new_tile_data(pr.report_instance.report_instance_id)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual([], data['series_data'][1]['data_points'])

        data = tile.get_new_tile_data(rd.instances[-1].report_instance_id)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(rd.instances[-3].report_instance_id)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        data = tile.get_new_tile_data(None)
        self.assertEqual(2, len(data['series_data']))
        self.assertEqual([], data['series_data'][0]['data_points'])
        self.assertEqual(128, data['series_data'][1]['data_points'][0].value)

        return tile

    def test_filling_default_options(self):
        rd = new_report_data('points', tags=['ip:192.168.1.1'])

        for tw_type in ('Range', 'Single'):
            ss1 = dataseries.SeriesSpec(2, 0, dict(op='eq', args=['mike']))
            ss1.promote_colnos_to_headers(rd.instances[-1])
            ss1.set_name('mikepoints')
            tile_config = {
                'tw_type': tw_type,
                'tags': ['ip:192.168.1.1'],
                'series_spec_list': [
                    ss1,
                    dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                ],
                'tile_options': {
                    'colors': ['red', 'blue', 'pink', 'cyan'],
                    'seconds_back': 600,
                    'tile_title': 'Points by user',
                    'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                }
            }

            tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
            data = tile.get_new_tile_data(rd.instances[-3].report_instance_id)

            tile_config2 = {
                'tw_type': tw_type,
                'tags': ['ip:192.168.1.1'],
                'series_spec_list': [
                    dataseries.guess_series_spec(rd.report, rd.instances[-1], 1, 2),
                    dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                ],
            }
            tile2 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
            data2 = tile2.get_new_tile_data(rd.instances[-3].report_instance_id)
            self.assertEqual('mikepoints', data2['series_data'][0]['name'])
