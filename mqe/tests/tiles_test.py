import uuid
import unittest
import json
import datetime
import time
from collections import OrderedDict

from mqe import dataseries
from mqe import tilewidgets
from mqe import tiles
from mqe.tiles import Tile
from mqe.tests.tutil import report_data, new_report_data, ReportData
from mqe import c
from mqe import tpcreator
from mqe.util import dictwithout, first
from mqe.layouts import place_tile, detach_tile, Layout
from mqe.dashboards import _select_tile_ids
from mqe import util


class TileTest(unittest.TestCase):

    def test_insert(self, tile_config_ext={}, tile_options_ext={}, dashboard_id=None, rd=None):
        rd = rd or report_data('points')
        owner_id = rd.owner_id
        report_id = rd.report.report_id
        dashboard_id = dashboard_id or rd.dashboard_id
        tile_config = {
            'tags': ['ip:192.168.1.1'],
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['john'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
                'tile_title': 'Points by user'
            }
        }
        tile_config.update(tile_config_ext)
        tile_config['tile_options'].update(tile_options_ext)

        tile = Tile.insert(owner_id, report_id, dashboard_id, tile_config)

        self.assertEqual(tile.owner_id, owner_id)
        self.assertEqual(tile.report_id, report_id)
        self.assertEqual(tile.report.report_id, report_id)
        self.assertEqual(dashboard_id, tile.dashboard_id)
        self.assertEqual(tile.tile_options['tags'], ['ip:192.168.1.1'])
        self.assertEqual(tile.tile_options['seconds_back'], 86400)
        self.assertEqual(tile_config['series_spec_list'], tile.series_specs())
        self.assertIsNone(tile.get_master_tile_id())
        self.assertIsInstance(tile.tilewidget, tilewidgets.TilewidgetForRange)
        return tile

    def test_insert_no_tags(self):
        owner_id = uuid.uuid1()
        report_id = report_data('points').report.report_id
        dashboard_id = report_data('points').dashboard_id
        tile_config = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['john'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
                'tile_title': 'Points by user'
            }
        }
        tile = Tile.insert(owner_id, report_id, dashboard_id, tile_config)
        self.assertEqual(tile.tile_options['tags'], [])


    def test_equality(self):
        tile1 = self.test_insert()
        tile2 = self.test_insert()
        self.assertNotEqual(tile1, tile2)

        tile3 = Tile.select(tile1.dashboard_id, tile1.tile_id)
        self.assertEqual(tile3, tile1)
        self.assertDictEqual(tile1.tile_options, tile3.tile_options)

    def test_select(self):
        inserted_tile = self.test_insert()
        tile = Tile.select(inserted_tile.dashboard_id, inserted_tile.tile_id)
        self.assertIsNotNone(tile)
        self.assertEqual(inserted_tile.tile_options, tile.tile_options)

    def test_select_multi(self):
        tiles = [self.test_insert() for _ in range(3)]
        tile_ids = [t.tile_id for t in tiles]
        selected_tiles = Tile.select_multi(report_data('points').dashboard_id, tile_ids)
        self.assertItemsEqual(tile_ids, selected_tiles.keys())
        self.assertItemsEqual(tiles, selected_tiles.values())

    def test_insert_similar(self):
        inserted_tile = self.test_insert()
        tile = Tile.select(inserted_tile.dashboard_id, inserted_tile.tile_id)

        tile_config = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'Points by user'
            }
        }
        tile = tile.insert_similar(tile_config)

        self.assertEqual([], tile.tile_options['tags'])
        self.assertEqual(tile.tile_options['seconds_back'], 600)
        self.assertEqual(tile_config['series_spec_list'], tile.series_specs())
        return tile

    def test_insert_similar_inserting(self):
        old_tile = self.test_insert_similar()
        tile = Tile.select(old_tile.dashboard_id, old_tile.tile_id)

        self.assertEqual([], tile.tile_options['tags'])
        self.assertEqual(tile.tile_options['seconds_back'], 600)
        self.assertEqual([dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique']))], tile.series_specs())

    def test_insert_skip_db(self):
        inserted_tile = self.test_insert()

        tile = Tile.select(inserted_tile.dashboard_id, inserted_tile.tile_id)
        tile_config = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'Points by user'
            }
        }
        tile = tile.insert(tile.owner_id, tile.report_id, tile.dashboard_id,
                           tile_config, skip_db=True)

        self.assertEqual([], tile.tile_options['tags'])
        self.assertEqual(tile.tile_options['seconds_back'], 600)
        self.assertEqual(tile_config['series_spec_list'], tile.series_specs())

        db_tile = Tile.select(inserted_tile.dashboard_id, inserted_tile.tile_id)
        self.assertEqual(db_tile.tile_options['tags'], ['ip:192.168.1.1'])
        self.assertEqual(db_tile.tile_options['seconds_back'], 86400)

    def test_insert_different_tw_type(self):
        tile = self.test_insert()
        self.assertIsInstance(tile.tilewidget, tilewidgets.TilewidgetForRange)

        tile_config = {
            'tw_type': 'Single',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'tile_title': 'Points by user'
            }
        }
        tile = tile.insert_similar(tile_config)
        self.assertIsInstance(tile.tilewidget, tilewidgets.TilewidgetForSingle)
        tile = Tile.select(tile.dashboard_id, tile.tile_id)
        self.assertIsInstance(tile.tilewidget, tilewidgets.TilewidgetForSingle)

    def test_tile_options_default_tw(self):
        rd = report_data('points')
        tile_config = {
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [],
        }
        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        self.assertEqual('Range', tile.tile_options['tw_type'])
        self.assertIsInstance(tile.tilewidget, tilewidgets.TilewidgetForRange)

        tile_config['tw_type'] = 'Single'
        tile = tile.insert_similar(tile_config)
        self.assertEqual('Single', tile.tile_options['tw_type'])

        del tile_config['tw_type']
        tile = tile.insert_similar(tile_config)
        self.assertEqual('Range', tile.tile_options['tw_type'])

    def test_tile_options_validation(self):
        rd = report_data('points')
        tile_config = {
            'tags': ['1', '2', '3', '4'],
            'series_spec_list': [],
        }
        self.assertRaises(ValueError, lambda: Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config))

    def test_insert_invalid_report(self):
        tile_config = {
            'tags': ['ip:192.168.1.1'],
            'series_spec_list': [],
            'tile_options': {
                'seconds_back': 86400,
                'tile_title': 'Points by user'
            }
        }
        self.assertRaises(ValueError, lambda: \
            Tile.insert(uuid.uuid1(), uuid.uuid1(), uuid.uuid1(), tile_config))

    def _select_sscs_rows(self, tile):
        return c.dao.LayoutDAO.select_layout_by_report_multi(tile.owner_id,
                                                             tile.report_id, [], 'sscs', 100)

    def test_sscs_creation_empty(self):
        rd = new_report_data('points')
        tile = self.test_insert(rd=rd)
        place_tile(tile)
        self.assertFalse(self._select_sscs_rows(tile))

    def test_sscs_creation(self):
        tile = self.test_insert(tile_options_ext={'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique']))})
        place_tile(tile)
        self.assertTrue(self._select_sscs_rows(tile))

    def _select_tpcreator_rows(self, tile):
        return c.dao.LayoutDAO.select_layout_by_report_multi(tile.owner_id,
                    tile.report_id, [], 'tpcreator', 100)

    def test_tpcreator_creation_empty(self):
        rd = new_report_data('points')
        tile = self.test_insert(rd=rd)
        place_tile(tile)
        self.assertFalse(self._select_tpcreator_rows(tile))

    def test_tpcreator_creation(self):
        tile = self.test_insert(tile_options_ext={'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['ip:192.168.1.1'])})
        place_tile(tile)
        self.assertTrue(self._select_tpcreator_rows(tile))
        return tile

    def test_is_master_tile(self):
        tile = self.test_tpcreator_creation()
        self.assertTrue(tile.is_master_tile())

        tile = Tile.select(tile.dashboard_id, tile.tile_id)
        self.assertTrue(tile.is_master_tile())

    def test_delete(self):
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
                'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['ip:192.168.1.1'])
            }
        }
        owner_id = uuid.uuid1()
        report_id = report_data('points').report.report_id
        dashboard_id = report_data('points').dashboard_id
        tile = Tile.insert(owner_id, report_id, dashboard_id, tile_config)
        place_tile(tile)
        self.assertTrue(self._select_tpcreator_rows(tile))
        self.assertTrue(self._select_sscs_rows(tile))

        tile = tile.select(tile.dashboard_id, tile.tile_id)
        detach_tile(tile)
        deleted_tile = tile.select(tile.dashboard_id, tile.tile_id)
        self.assertIsNone(deleted_tile)
        #self.assertFalse(self._select_tpcreator_rows(tile))
        #self.assertFalse(self._select_sscs_rows(tile))


    def test_copy(self):
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
                'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['ip:192.168.1.1'])
            }
        }
        owner_id = uuid.uuid1()
        report_id = report_data('points').report.report_id
        dashboard_id = report_data('points').dashboard_id

        tile = Tile.insert(owner_id, report_id, dashboard_id, tile_config)
        self.assertTrue(place_tile(tile))
        self.assertTrue(self._select_tpcreator_rows(tile))
        self.assertTrue(self._select_sscs_rows(tile))

        new_dashboard_id = uuid.uuid1()
        tile2 = tile.copy(new_dashboard_id)
        tile2 = Tile.select(tile2.dashboard_id, tile2.tile_id)
        place_tile(tile2)
        self.assertNotEqual(tile, tile2)
        self.assertEqual(new_dashboard_id, tile2.dashboard_id)
        self.assertTrue(self._select_tpcreator_rows(tile2))
        self.assertTrue(self._select_sscs_rows(tile2))

    def test_get_tile_config(self):
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
                'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['ip:192.168.1.1'])
            }
        }
        owner_id = uuid.uuid1()
        report_id = report_data('points').report.report_id
        dashboard_id = report_data('points').dashboard_id

        tile = Tile.insert(owner_id, report_id, dashboard_id, tile_config)
        res = tile.get_tile_config()
        self.assertDictContainsSubset(dictwithout(tile_config, 'tile_options'),
                                      dictwithout(res, 'tile_options'))
        self.assertDictContainsSubset(tile_config['tile_options'],
                                      res['tile_options'])

    def test_get_tile_data(self):
        tile = self.test_insert()
        res = tile.get_tile_data()
        self.assertIn('series_data', res)

    def test_get_new_tile_data(self):
        tile = self.test_insert()
        res = tile.get_new_tile_data(uuid.uuid1())
        self.assertIn('series_data', res)


class TilesModuleTest(unittest.TestCase):


    def test_expire_tiles_without_data(self):
        rd1 = new_report_data('points')
        rd2 = new_report_data('points')

        tile_config_1 = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
            }
        }
        tile1 = tiles.Tile.insert(rd1.owner_id, rd1.report.report_id, rd1.dashboard_id, tile_config_1)
        place_tile(tile1)
        self.assertTrue(tile1.get_tile_data()['series_data'][0]['data_points'])

        tile_config_2 = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['john20'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
            }
        }
        tile2 = tiles.Tile.insert(rd2.owner_id, rd2.report.report_id, rd2.dashboard_id, tile_config_2)
        place_tile(tile2)
        self.assertFalse(tile2.get_tile_data()['series_data'][0]['data_points'])
        res = tiles.expire_tiles_without_data([tile1, tile2], 3600, Layout.select(rd1.owner_id, rd1.dashboard_id).layout_id, optimize_check=True)
        self.assertIsNone(res)

        time.sleep(0.5)
        rd2.report.process_input('0')

        res = tiles.expire_tiles_without_data([tile1, tile2], 0.5, Layout.select(rd1.owner_id, rd1.dashboard_id).layout_id, optimize_check=True)
        self.assertTrue(res)
        self.assertFalse(Tile.select(rd1.dashboard_id, tile1.tile_id))
        self.assertTrue(Tile.select(rd2.dashboard_id, tile2.tile_id))


    def test_expire_tiles_without_data_optimize_check(self):
        owner_id = report_data('points').report.owner_id
        report_id = report_data('points').report.report_id
        dashboard_id = report_data('points').dashboard_id

        report_data('points').report.process_input(json.dumps(
            [
                OrderedDict([('user_name', 'xxx'), ('is_active', True), ('points', 1000)]),
                OrderedDict([('user_name', 'yy'), ('is_active', True), ('points', 2000)]),
            ]
        ), created=datetime.datetime.utcnow() - datetime.timedelta(hours=8))

        tile_config_1 = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
            }
        }
        tile1 = tiles.Tile.insert(owner_id, report_id, dashboard_id, tile_config_1)
        self.assertTrue(tile1.get_tile_data()['series_data'][0]['data_points'])

        tile_config_2 = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['john'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
            }
        }
        tile2 = tiles.Tile.insert(owner_id, report_id, dashboard_id, tile_config_2)
        self.assertTrue(tile2.get_tile_data()['series_data'][0]['data_points'])

        tile_config_3 = {
            'tw_type': 'Range',
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['xxx'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
            }
        }
        tile3 = tiles.Tile.insert(owner_id, report_id, dashboard_id, tile_config_3)
        self.assertTrue(tile3.get_tile_data()['series_data'][0]['data_points'])


        place_tile(tile1)
        place_tile(tile2)
        place_tile(tile3)

        res = tiles.expire_tiles_without_data([tile1, tile2, tile3], 3600, Layout.select(owner_id, dashboard_id).layout_id)
        self.assertTrue(res)

        tile1 = tiles.Tile.select(tile1.dashboard_id, tile1.tile_id)
        self.assertIsNotNone(tile1)
        tile2 = tiles.Tile.select(tile2.dashboard_id, tile2.tile_id)
        self.assertIsNotNone(tile2)
        tile3 = tiles.Tile.select(tile3.dashboard_id, tile3.tile_id)
        self.assertIsNone(tile3)

        res = tiles.expire_tiles_without_data([tile1, tile2], 3600, Layout.select(owner_id, dashboard_id).layout_id)
        self.assertFalse(res)

    def test_expire_tiles_without_data_master(self):
        rd = new_report_data('points', ['p1:10'])

        tile_config1 = {
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 1000,
                'tile_title': 'tile1'
            }
        }
        tile1 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config1)
        place_tile(tile1)

        tile_config2 = {
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 1000,
                'tile_title': 'tile2',
                'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['p1:10']),
            }
        }
        tile2 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config2)
        place_tile(tile2)

        d = [
            OrderedDict([('user_name', 'robert'), ('is_active', True), ('points', 128)]),
            OrderedDict([('user_name', 'monique2'), ('is_active', True), ('points', 210)]),
            OrderedDict([('user_name', 'monique3'), ('is_active', True), ('points', 210)]),
            OrderedDict([('user_name', 'monique4'), ('is_active', True), ('points', 210)]),
            OrderedDict([('user_name', 'robert2'), ('is_active', True), ('points', 210)]),
        ]
        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p1:11'])
        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p1:12'])
        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p1:13'])

        self.assertEqual(5, len(rd.layout().layout_dict))

        lid = tiles.expire_tiles_without_data(rd.layout().tile_dict.keys(), 2000, rd.layout().layout_id)
        self.assertFalse(lid)
        self.assertEqual(5, len(rd.layout().layout_dict))

        lid = tiles.expire_tiles_without_data(rd.layout().tile_dict.keys(), 0, rd.layout().layout_id)
        self.assertTrue(lid)

        self.assertEqual(1, len(rd.layout().layout_dict))
        self.assertEqual(1, len(_select_tile_ids(rd.dashboard_id)))
        self.assertEqual(tile2.tile_id, rd.layout().tile_dict.keys()[0].tile_id)

    def test_expire_tiles_without_data_two_masters(self):
        rd = new_report_data('points', ['p3:30'])

        tile_config1 = {
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 1000,
                'tile_title': 'tile1'
            }
        }
        tile1 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config1)
        place_tile(tile1)

        tile_config2 = {
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 1000,
                'tile_title': 'tile2 [p1:10]',
                'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['p1:10']),
            }
        }
        tile2 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config2)
        place_tile(tile2)

        tile_config3 = {
            'tags': ['p2:20'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 1000,
                'tile_title': 'tile3',
                'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['p2:20']),
            }
        }
        tile3 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config3)
        place_tile(tile3)

        #rd.report.process_input(json.dumps([dict(user_name='monique', is_active=True, points=128)]), tags=['p1:10'])
        #rd.report.process_input(json.dumps([dict(user_name='monique', is_active=True, points=128)]), tags=['p2:20'])

        time.sleep(1)

        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p1:11'])
        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p1:12'])
        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p1:13'])

        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p2:21'])
        rd.report.process_input(json.dumps([OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 128)])]), tags=['p2:22'])

        lid = tiles.expire_tiles_without_data(rd.layout().tile_dict.keys(), 2000, rd.layout().layout_id)
        self.assertFalse(lid)

        lid = tiles.expire_tiles_without_data(rd.layout().tile_dict.keys(), 1, rd.layout().layout_id)
        self.assertTrue(lid)

        self.assertEqual(6, len(rd.layout().layout_dict))
        self.assertEqual(6, len(_select_tile_ids(rd.dashboard_id)))
        tile_list = rd.layout().tile_dict.keys()
        self.assertEqual(sorted([[], ['p1:11'], ['p1:12'], ['p1:13'], ['p2:21'], ['p2:22']]), sorted([t.tags for t in tile_list]))
        master1 = first(t for t in tile_list if t.tags and t.tags[0].startswith('p1') and t.is_master_tile())
        master2 = first(t for t in tile_list if t.tags and t.tags[0].startswith('p2') and t.is_master_tile())
        self.assertEqual(2, len(rd.layout().get_tpcreated_tile_ids(master1.tile_id)))
        self.assertEqual(1, len(rd.layout().get_tpcreated_tile_ids(master2.tile_id)))

        self.assertEqual(['p1:11'], master1.tags)
        self.assertEqual(['p2:21'], master2.tags)

        self.assertEqual('tile2 [p1:11]', master1.tile_options['tile_title'])
        self.assertEqual('tile3', master2.tile_options['tile_title'])


    def test_expire_tiles_without_data_losing_sscreated(self):
        rd = ReportData('r')

        ss = dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0']))
        tile_config1 = {
            'series_spec_list': [ss],
            'tags': ['p1:10'],
            'tile_options': {
                'tpcreator_uispec': tpcreator.suggested_tpcreator_uispec(['p1:10']),
                'sscs': ss
            }
        }

        tile1 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config1)
        place_tile(tile1)

        #rd.report.process_input('1', tags=['p1:10'])

        rd.report.process_input('1\n2\n', tags=['p1:11'])
        rd.report.process_input('1\n2\n3\n', tags=['p1:12'])

        self.assertEqual(3, len(rd.layout().layout_dict))
        self.assertEqual(3, len(rd.get_tile_by_tags(['p1:12']).series_specs()))

        tile1_created_ago = datetime.datetime.utcnow() - util.datetime_from_uuid1(tile1.tile_id)
        tiles.expire_tiles_without_data(rd.layout().tile_dict.keys(),
             tile1_created_ago.total_seconds() - 0.00001, rd.layout().layout_id)
        self.assertEqual(2, len(rd.layout().layout_dict))
        self.assertTrue(rd.layout_has_tags([['p1:11'], ['p1:12']]))
        master_tile = rd.get_tile_by_tags(['p1:11'])
        self.assertTrue(master_tile.is_master_tile())
        self.assertEqual(master_tile.tile_id, rd.get_tile_by_tags(['p1:12']).get_master_tile_id())
        self.assertEqual(3, len(rd.get_tile_by_tags(['p1:12']).series_specs()))
