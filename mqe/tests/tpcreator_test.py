import unittest
import json
import uuid
from collections import OrderedDict

from mqe import c
from mqe import tpcreator
from mqe import dataseries
from mqe.tiles import expire_tiles_without_data
from mqe.tiles import Tile
from mqe.dashboards import _select_tile_ids
from mqe import util
from mqe import layouts
from mqe.layouts import Layout
from mqe import reports

from mqe.tests.tutil import new_report_data, patch


class TPCreatorTest(unittest.TestCase):

    def test_handle_tpcreator(self):
        rd = new_report_data('points')

        tile_config = {
            'tw_type': 'Range',
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'm0',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        master_tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(master_tile)

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['p1:10'])
        master_tile = rd.only_tile_from_layout()
        self.assertEqual([], tpcreator.select_tpcreated_tile_ids(master_tile))
        self.assertEqual([master_tile.tile_id], _select_tile_ids(rd.dashboard_id))

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['p1:20'])
        self.assertEqual(2, len(_select_tile_ids(rd.dashboard_id)))
        tiles = Tile.select_multi(rd.dashboard_id, _select_tile_ids(rd.dashboard_id)).values()
        created_tile = util.first(tiles, key=lambda t: not t.is_master_tile())
        self.assertEqual(['p1:20'], created_tile.tile_options['tags'])
        self.assertEqual(600, created_tile.tile_options['seconds_back'])
        self.assertEqual(tile_config['tile_options']['sscs'], created_tile.tile_options['sscs'])
        td = created_tile.get_tile_data()
        self.assertEqual('points (monique, robert3)', td['generated_tile_title'])
        self.assertEqual('[p1:20]', td['generated_tile_title_postfix'])

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['p1:30', 'p2:30'])
        self.assertEqual(3, len(_select_tile_ids(rd.dashboard_id)))

        del tile_config['tile_options']['tpcreator_uispec']
        tile_config['tile_options']['tile_title'] = 'ot0'
        other_tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(other_tile)
        self.assertEqual(4, len(_select_tile_ids(rd.dashboard_id)))

        self.assertEqual(2, len(tpcreator.select_tpcreated_tile_ids(master_tile)))

        for i, tile_id in enumerate(tpcreator.select_tpcreated_tile_ids(master_tile)):
            tile = Tile.select(rd.dashboard_id, tile_id)
            tile_config = tile.get_tile_config()
            tile_config['tile_options']['tile_title'] = 'tpc%d' % i
            new_tile = tile.insert_similar(tile_config)
            layouts.replace_tiles({tile: new_tile}, None)

        return rd, Tile.select(master_tile.dashboard_id, master_tile.tile_id)

    def test_handle_tpcreator_multiple_masters(self):
        rd = new_report_data('points')

        tile_config_1 = {
            'tw_type': 'Range',
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'm0',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config_1['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config_1['tags'])

        master_tile_1 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config_1)
        layouts.place_tile(master_tile_1)

        tile_config_2 = {
            'tw_type': 'Range',
            'tags': ['p2:20'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'm0',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config_2['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config_2['tags'])

        master_tile_2 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config_2)
        layouts.place_tile(master_tile_2)

        # tile config of 2.
        master_tile_3 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config_2)
        layouts.place_tile(master_tile_3)

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['p1:11'])
        rd.report.process_input(json.dumps(d), tags=['p1:12'])
        rd.report.process_input(json.dumps(d), tags=['p2:21'])
        rd.report.process_input(json.dumps(d), tags=['p2:22'])
        rd.report.process_input(json.dumps(d), tags=['p2:23'])

        layout = layouts.Layout.select(rd.owner_id, rd.dashboard_id)
        self.assertEqual(2, len(layout.get_tpcreated_tile_ids(master_tile_1.tile_id)))
        self.assertEqual(3, len(layout.get_tpcreated_tile_ids(master_tile_2.tile_id)))
        self.assertEqual(3, len(layout.get_tpcreated_tile_ids(master_tile_3.tile_id)))


    def test_removing_tiles(self):
        rd, master_tile = self.test_handle_tpcreator()
        tiles = Tile.select_multi(rd.dashboard_id, _select_tile_ids(rd.dashboard_id)).values()
        tpcreated_tile = util.first(t for t in tiles if t.get_master_tile_id())
        layouts.detach_tile(tpcreated_tile)
        self.assertEqual(1, len(tpcreator.select_tpcreated_tile_ids(master_tile)))

    def test_removing_tiles_by_expiring(self):
        rd, master_tile = self.test_handle_tpcreator()
        tiles = Tile.select_multi(rd.dashboard_id, _select_tile_ids(rd.dashboard_id)).values()

        #print tpcreator.select_tpcreated_tile_ids(master_tile)
        #print select_tile_ids(rd.dashboard_id)
        #print rd.layout_data().layout_dict.keys()

        #tile = Tile.select(rd.dashboard_id, util.first(tile_id for tile_id in select_tile_ids(rd.dashboard_id) if tile_id not in rd.layout_data().layout_dict.keys()))
        #print tile

        self.assertTrue(expire_tiles_without_data(tiles, 0, rd.layout().layout_id))

        self.assertEqual(0, len(tpcreator.select_tpcreated_tile_ids(master_tile)))
        self.assertEqual(1, len(_select_tile_ids(rd.dashboard_id)))


    def test_promote_other_master_tile(self):
        rd, master_tile = self.test_handle_tpcreator()
        tpcreated_ids = tpcreator.select_tpcreated_tile_ids(master_tile)
        tpcreated_tiles = Tile.select_multi(rd.dashboard_id, tpcreated_ids).values()
        #print 'tpcreated', [t.tags for t in tpcreated_tiles]

        new_master_repl = util.first(t for t in tpcreated_tiles if t.tile_options['tile_title'] == 'tpc0')
        new_master_tile = tpcreator.make_master_from_tpcreated(master_tile, new_master_repl)
        self.assertEqual('tpc0', new_master_tile.tile_options['tile_title'])

        self.assertTrue(layouts.replace_tiles({master_tile: new_master_tile},
                                             None))
        self.assertFalse(tpcreator.select_tpcreated_tile_ids(master_tile))

        new_tpcreated_ids = tpcreator.select_tpcreated_tile_ids(new_master_tile)
        new_tpcreated_tiles = Tile.select_multi(rd.dashboard_id, new_tpcreated_ids).values()
        self.assertTrue(new_tpcreated_tiles)

        self.assertEqual([['p1:20'], ['p1:30']], sorted([t.tags for t in new_tpcreated_tiles]))

        d = [OrderedDict([('user_name', 'robert10'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['p1:15', 'p2:34'])

        self.assertEqual(len(new_tpcreated_ids) + 1, len(tpcreator.select_tpcreated_tile_ids(new_master_tile)))
        latest_tile = Tile.select(rd.dashboard_id, _select_tile_ids(rd.dashboard_id)[-1])
        self.assertEqual(['p1:15'], latest_tile.tags)
        self.assertEqual(['monique', 'robert3', 'robert10'], [ss.params['filtering_expr']['args'][0] for ss in latest_tile.series_specs()])

    def test_synchronize_options_of_tpcreated(self):
        rd, master_tile = self.test_handle_tpcreator()

        self.assertEqual(2, len(tpcreator.select_tpcreated_tile_ids(master_tile)))

        tile_config = master_tile.get_tile_config()
        tile_config['series_spec_list'].append(dataseries.SeriesSpec(2, 0, dict(op='eq', args=['chris'])))
        tile_config['tile_options']['seconds_back'] = 125
        tile_config['tile_options']['tile_title'] = 'A B C'
        master_tile = master_tile.insert_similar(tile_config)

        tiles = Tile.select_multi(rd.dashboard_id,
                                  tpcreator.select_tpcreated_tile_ids(master_tile)).values()
        for tile in tiles:
            self.assertIn(dataseries.SeriesSpec(2, 0, dict(op='eq', args=['chris'])),
                          tile.series_specs())
            self.assertEqual(125, tile.tile_options['seconds_back'])
            self.assertEqual('A B C', tile.tile_options['tpcreator_data']['tile_title_base'])

    def test_synchronize_sizes_of_tpcreated(self):
        rd, master_tile = self.test_handle_tpcreator()

        layout = layouts.Layout.select(rd.owner_id, rd.dashboard_id)
        ld = layout.layout_dict
        ld[master_tile.tile_id]['width'] = 8
        ld[master_tile.tile_id]['height'] = 3
        ld = layouts.apply_mods_for_noninserted_layout([layouts.repack_mod()],
                                                       Layout(ld)).new_layout.layout_dict
        layout_id = layout.set(rd.owner_id, rd.dashboard_id)
        self.assertTrue(layout_id)

        self.assertTrue(tpcreator.synchronize_sizes_of_tpcreated(master_tile, layout_id))
        layout = Layout.select(rd.owner_id, rd.dashboard_id)
        ld = layout.layout_dict
        for tile_id in tpcreator.select_tpcreated_tile_ids(master_tile):
            self.assertEqual(8, ld[tile_id]['width'])
            self.assertEqual(3, ld[tile_id]['height'])

    def test_layout_sorting(self):
        rd = new_report_data('points', ['p1:10'])
        tile_config = {
            'tw_type': 'Range',
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            ],
            'tile_options': {
                'seconds_back': 600,
                'tile_title': 'm0',
                'sscs': dataseries.SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
            }
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec(tile_config['tags'])

        master_tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(master_tile)

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        nums = [30, 20, 12, 11, 40, 98]
        for num in nums:
            rd.report.process_input(json.dumps(d), tags=['p1:%s' % num])

        ld = rd.layout()
        self.assertEqual(len(nums) + 1, len(ld.layout_dict.keys()))
        self.assertEqual(len(nums) + 1, len(ld.layout_props['by_tile_id'].keys()))

        layout_items = sorted(ld.layout_dict.items(),
                              key=lambda (tile_id, vo): (vo['y'], vo['x']))

        layout_tags = [ld.layout_props['by_tile_id'][tile_id]['tags'][0]
                       for (tile_id, vo) in layout_items]
        expected_tags = ['p1:10'] + sorted(['p1:%s' % num for num in nums])
        self.assertEqual(set(expected_tags), set(layout_tags))
        self.assertEqual(expected_tags, layout_tags)

    def test_set_layout_fails(self):
        owner_id = uuid.uuid1()
        dashboard_id_1 = uuid.uuid1()
        r = reports.Report.insert(owner_id, 'r')
        tile_config = {
            'tw_type': 'Single',
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {}
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec([
            'p1:10'])


        master_tile_1 = Tile.insert(owner_id, r.report_id, dashboard_id_1, tile_config)
        layouts.place_tile(master_tile_1)

        r.process_input('1', tags=['p1:10'])

        self.assertEqual(1, len(Layout.select(master_tile_1.owner_id, master_tile_1.dashboard_id).layout_dict))

        master_tile_2 = master_tile_1.copy(dashboard_id_1)
        layouts.place_tile(master_tile_2)

        r.process_input('1', tags=['p1:11'])

        layout_1 = Layout.select(master_tile_1.owner_id, master_tile_1.dashboard_id)
        self.assertEqual(4, len(layout_1.layout_dict))
        self.assertEqual(1, len(layout_1.get_tpcreated_tile_ids(master_tile_1.tile_id)))
        self.assertEqual(1, len(layout_1.get_tpcreated_tile_ids(master_tile_2.tile_id)))
        self.assertNotEqual(layout_1.get_tpcreated_tile_ids(master_tile_1.tile_id),
                            layout_1.get_tpcreated_tile_ids(master_tile_2.tile_id))

        dashboard_id_2 = uuid.uuid1()
        master_tile_3 = master_tile_1.copy(dashboard_id_2)
        layouts.place_tile(master_tile_3)

        dashboard_id_3 = uuid.uuid1()
        master_tile_4 = master_tile_1.copy(dashboard_id_3)
        layouts.place_tile(master_tile_4)

        r.process_input('1', tags=['p1:12'])

        layout_1 = Layout.select(master_tile_1.owner_id, dashboard_id_1)
        layout_2 = Layout.select(master_tile_1.owner_id, dashboard_id_2)
        layout_3 = Layout.select(master_tile_1.owner_id, dashboard_id_3)

        self.assertEqual(6, len(layout_1.layout_dict))
        self.assertEqual(2, len(layout_2.layout_dict))
        self.assertEqual(2, len(layout_3.layout_dict))

        mock_data = {'call': 0}
        def mock__set_new_layout(*args, **kwargs):
            mock_data['call'] += 1
            if mock_data['call'] >= 2:
                return None
            return mock__set_new_layout.old_fun(*args, **kwargs)

        with patch(layouts.Layout, layouts.Layout.set, mock__set_new_layout):
            r.process_input('1', tags=['p1:13'])

        layout_1 = Layout.select(master_tile_1.owner_id, dashboard_id_1)
        layout_2 = Layout.select(master_tile_1.owner_id, dashboard_id_2)
        layout_3 = Layout.select(master_tile_1.owner_id, dashboard_id_3)

        self.assertEqual(7, len(layout_1.layout_dict))
        self.assertEqual(2, len(layout_2.layout_dict))
        self.assertEqual(2, len(layout_3.layout_dict))


        mock_data = {'call': 0}
        def mock__set_new_layout(*args, **kwargs):
            mock_data['call'] += 1
            if mock_data['call'] % 5 != 0:
                return None
            return mock__set_new_layout.old_fun(*args, **kwargs)

        with patch(layouts.Layout, layouts.Layout.set, mock__set_new_layout):
            r.process_input('1', tags=['p1:14'])

        layout_1 = Layout.select(master_tile_1.owner_id, dashboard_id_1)
        layout_2 = Layout.select(master_tile_1.owner_id, dashboard_id_2)
        layout_3 = Layout.select(master_tile_1.owner_id, dashboard_id_3)

        self.assertEqual(9, len(layout_1.layout_dict))
        self.assertEqual(3, len(layout_2.layout_dict))
        self.assertEqual(3, len(layout_3.layout_dict))

    def test_deleting_layout_by_report_row(self):
        owner_id = uuid.uuid1()
        dashboard_id_1 = uuid.uuid1()
        r = reports.Report.insert(owner_id, 'r')
        tile_config = {
            'tw_type': 'Single',
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {}
        }
        tile_config['tile_options']['tpcreator_uispec'] = tpcreator.suggested_tpcreator_uispec([
            'p1:10'])

        master_tile_1 = Tile.insert(owner_id, r.report_id, dashboard_id_1, tile_config)
        layouts.place_tile(master_tile_1)

        r.process_input('1', tags=['p1:11'])

        self.assertEqual(2, len(Layout.select(master_tile_1.owner_id, master_tile_1.dashboard_id).layout_dict))

        rows = c.dao.LayoutDAO.select_layout_by_report_multi(master_tile_1.owner_id,
                             master_tile_1.report_id, [], 'tpcreator', 100)
        self.assertTrue(rows)

        layouts.detach_tile(master_tile_1)

        r.process_input('1', tags=['p1:12'])

        self.assertEqual(1, len(Layout.select(master_tile_1.owner_id, master_tile_1.dashboard_id).layout_dict))

        rows = c.dao.LayoutDAO.select_layout_by_report_multi(master_tile_1.owner_id,
                                                             master_tile_1.report_id, [], 'tpcreator', 100)
        self.assertFalse(rows)


    def test_multiple_tags_order(self):
        tile_config = {
            'tags': ['p1:10', 'p2:10', 'zzz'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {
                'tpcreator_uispec': [{'tag': 'p1:10', 'prefix': 'p1:'},
                                     {'tag': 'p1:20', 'prefix': 'p1:'},
                                     {'tag': 'zzz', 'prefix': 'z'}],
            }
        }
        owner_id = uuid.uuid1()
        dashboard_id_1 = uuid.uuid1()
        r = reports.Report.insert(owner_id, 'r')
        master_tile = Tile.insert(owner_id, r.report_id, dashboard_id_1, tile_config)
        layouts.place_tile(master_tile)

        r.process_input('1', tags=['p1:11', 'p1:12'])
        self.assertEqual(1, len(Layout.select(owner_id, dashboard_id_1).layout_dict))

        r.process_input('1', tags=['p1:11', 'p2:12', 'zz'])
        layout = Layout.select(owner_id, dashboard_id_1)
        self.assertEqual(2, len(layout.layout_dict))
        self.assertIn(['p1:11', 'zz'], [t.tags for t in layout.tile_dict])

        r.process_input('1', tags=['p2:12', 'aaa', 'p1:11',])
        layout = Layout.select(owner_id, dashboard_id_1)
        self.assertEqual(2, len(layout.layout_dict))

        tile_config_2 = {
            'tags': ['p1:10', 'p2:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {
                'tpcreator_uispec': [{'tag': 'p1:10', 'prefix': 'p1:'},
                                     {'tag': 'p1:20', 'prefix': ''}]
            }
        }
        dashboard_id_2 = uuid.uuid1()
        master_tile_2 = Tile.insert(owner_id, r.report_id, dashboard_id_2, tile_config_2)
        layouts.place_tile(master_tile_2)

        r.process_input('1', tags=['p1:11', 'p2:12'])
        layout = Layout.select(owner_id, dashboard_id_2)
        self.assertEqual(2, len(layout.layout_dict))
        self.assertIn(['p1:11', 'p2:12'], [t.tags for t in layout.tile_dict])

        tile_config_3 = {
            'tags': ['p2:10', 'p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {
                'tpcreator_uispec': [{'tag': 'p1:20', 'prefix': ''},
                                     {'tag': 'p1:10', 'prefix': 'p1:'}]
            }
        }
        dashboard_id_3 = uuid.uuid1()
        master_tile_3 = Tile.insert(owner_id, r.report_id, dashboard_id_3, tile_config_3)
        layouts.place_tile(master_tile_3)

        r.process_input('1', tags=['p1:10', 'p2:10'])
        layout = Layout.select(owner_id, dashboard_id_3)
        self.assertEqual(1, len(layout.layout_dict))

        r.process_input('1', tags=['p2:11', 'p1:12'])
        layout = Layout.select(owner_id, dashboard_id_3)
        self.assertEqual(2, len(layout.layout_dict))
        self.assertIn(['p1:12', 'p2:11'], [t.tags for t in layout.tile_dict])
