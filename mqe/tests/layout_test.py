import unittest
import uuid
from uuid import UUID
from copy import deepcopy
from time import time

from mqe import layouts
from mqe.layouts import place_tile, detach_tile, Layout, repack
from mqe import dataseries
from mqe.tiles import Tile
from mqe import tpcreator
from mqe import c
from mqe.reports import Report

from mqe.tests.tutil import call, ReportData, enable_logging
from mqe.tests import tiles_test


LAYOUT_DICT = {
    UUID('1e102e4f-d016-45cb-adb2-05d83173c2cb'): {u'height': 2,
                                                  u'width': 4,
                                                  u'x': 0,
                                                  u'y': 0},
  UUID('42468699-0437-4e43-8e34-efe8d2e93542'): {u'height': 4,
                                                   u'width': 4,
                                                   u'x': 8,
                                                   u'y': 0},
  UUID('454687fa-01ad-4fee-99eb-c25073a26471'): {u'height': 3,
                                                   u'width': 1,
                                                   u'x': 0,
                                                   u'y': 2},
  UUID('454687fa-01ad-4fee-99eb-c25073a26472'): {u'height': 3,
                                                   u'width': 1,
                                                   u'x': 0,
                                                   u'y': 5},
}


class PackingUpwardsTest(unittest.TestCase):
    maxDiff = None

    def pack(self, ldict):
        return layouts.apply_mods_for_noninserted_layout([layouts.pack_upwards_mod()],
                                                         layouts.Layout(deepcopy(
                                                             ldict))).new_layout.layout_dict

    def test_pack_1(self):
        ld = self.pack(LAYOUT_DICT)
        self.assertEqual(LAYOUT_DICT, ld)

    def test_pack_2(self):
        LD2 = deepcopy(LAYOUT_DICT)
        for vo in LD2.values():
            vo['y'] += 1

        ld = self.pack(LD2)
        self.assertDictEqual(LAYOUT_DICT, ld)

    def test_pack_3(self):
        LD2 = deepcopy(LAYOUT_DICT)
        for vo in LD2.values():
            vo['y'] += 3

        ld = self.pack(LD2)
        self.assertDictEqual(LAYOUT_DICT, ld)

    def test_pack_floating_1(self):
        LD2 = deepcopy(LAYOUT_DICT)
        ud = uuid.uuid1()
        LD2[ud] = {'height': 5, 'width': 3, 'x': 0, 'y': 10}
        ld = self.pack(LD2)

        ld_expected = deepcopy(LAYOUT_DICT)
        ld_expected[ud] = {'height': 5, 'width': 3, 'x': 0, 'y': 8}
        self.assertDictEqual(ld_expected, ld)

    def test_pack_floating_2(self):
        LD2 = deepcopy(LAYOUT_DICT)
        ud = uuid.uuid1()
        LD2[ud] = {'height': 8, 'width': 2, 'x': 5, 'y': 12}
        ld = self.pack(LD2)

        ld_expected = deepcopy(LAYOUT_DICT)
        ld_expected[ud] = {'height': 8, 'width': 2, 'x': 5, 'y': 0}
        self.assertDictEqual(ld_expected, ld)


class PackingLeftwardsTest(unittest.TestCase):
    maxDiff = None

    def pack(self, ldict):
        return layouts.apply_mods_for_noninserted_layout([layouts.pack_leftwards_mod()],
                                                         layouts.Layout(deepcopy(
                                                             ldict))).new_layout.layout_dict

    def test_1(self):
        ld = self.pack(LAYOUT_DICT)
        ld_expected = deepcopy(LAYOUT_DICT)
        ld_expected[UUID('42468699-0437-4e43-8e34-efe8d2e93542')]['x'] = 4
        self.assertDictEqual(ld_expected, ld)

    def test_2(self):
        ld = deepcopy(LAYOUT_DICT)
        ld[UUID('1e102e4f-d016-45cb-adb2-05d83173c2cb')]['x'] = 2
        ld[UUID('454687fa-01ad-4fee-99eb-c25073a26471')]['x'] = 1

        ld_expected = deepcopy(LAYOUT_DICT)
        ld_expected[UUID('42468699-0437-4e43-8e34-efe8d2e93542')]['x'] = 4

        self.assertDictEqual(ld_expected, self.pack(ld))


class TilePlacingDetachingTest(unittest.TestCase):

    def test_place(self):
        tile = call(tiles_test.TileTest.test_insert, dashboard_id=uuid.uuid1())
        res = place_tile(tile)
        self.assertIsNotNone(res)
        vo = {'width': 4, 'height': 4, 'x': 0, 'y': 0}
        self.assertEqual(vo, res.new_tiles[tile])
        layout = Layout.select(tile.owner_id, tile.dashboard_id)
        self.assertEqual(res.new_layout.layout_id, layout.layout_id)
        self.assertEqual({tile.tile_id: vo}, layout.layout_dict)

    def place(self, dashboard_id, **kwargs):
        tile = call(tiles_test.TileTest.test_insert, dashboard_id=dashboard_id)
        res = place_tile(tile, **kwargs)
        self.assertIsNotNone(res)
        return tile

    def ld_from_vos(self, tiles, *vos):
        return dict(zip([t.tile_id for t in tiles], vos))

    def test_place_wrong_id(self):
        dashboard_id = uuid.uuid1()
        tile = self.place(dashboard_id)
        layout = Layout.select(tile.owner_id, tile.dashboard_id)
        tile = self.place(dashboard_id)

        tile = call(tiles_test.TileTest.test_insert, dashboard_id=dashboard_id)
        res = place_tile(tile, for_layout_id=layout.layout_id)
        self.assertIsNone(res)

    def test_place_multiple(self):
        dashboard_id = uuid.uuid1()
        tiles = []
        for kwargs in ({},
                       {'initial_visual_options': {'width': 12, 'height': 10}},
                       {},
                       {'initial_visual_options': {'width': 5, 'height': 1}},
                       {}):
            tiles.append(self.place(dashboard_id, **kwargs))


        layout = Layout.select(tiles[0].owner_id, dashboard_id)
        self.assertEqual(self.ld_from_vos(tiles,
            {u'width': 4, u'height': 4, u'x': 0, u'y': 0},
            {u'width': 12, u'height': 10, u'x': 0, u'y': 4},
            {u'width': 4, u'height': 4, u'x': 4, u'y': 0},
            {u'width': 5, u'height': 1, u'x': 0, u'y': 14},
            {u'width': 4, u'height': 4, u'x': 8, u'y': 0},
        ), layout.layout_dict)
        return tiles

    def test_detach(self):
        tiles = self.test_place_multiple()
        detach_tile(tiles[1])
        del tiles[1]
        layout = Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        self.assertDictEqual(self.ld_from_vos(tiles,
              {u'width': 4, u'height': 4, u'x': 0, u'y': 0},
              {u'width': 4, u'height': 4, u'x': 4, u'y': 0},
              {u'width': 5, u'height': 1, u'x': 0, u'y': 4},
              {u'width': 4, u'height': 4, u'x': 8, u'y': 0},
              ), layout.layout_dict)

    def test_detach_2(self):
        tiles = self.test_place_multiple()
        detach_tile(tiles[0])
        detach_tile(tiles[1])
        del tiles[0]
        del tiles[0]
        layout = Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        self.assertDictEqual(self.ld_from_vos(tiles,
              {u'width': 4, u'height': 4, u'x': 4, u'y': 0},
              {u'width': 5, u'height': 1, u'x': 0, u'y': 4},
              {u'width': 4, u'height': 4, u'x': 8, u'y': 0},
              ), layout.layout_dict)

    def test_detach_2_repack(self):
        tiles = self.test_place_multiple()
        detach_tile(tiles[0])
        repack(tiles[0].owner_id, tiles[0].dashboard_id)
        detach_tile(tiles[1])
        repack(tiles[0].owner_id, tiles[0].dashboard_id)
        del tiles[0]
        del tiles[0]
        layout = Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        self.assertDictEqual(self.ld_from_vos(tiles,
              {u'width': 4, u'height': 4, u'x': 0, u'y': 0},
              {u'width': 5, u'height': 1, u'x': 0, u'y': 4},
              {u'width': 4, u'height': 4, u'x': 4, u'y': 0},
              ), layout.layout_dict)

    def test_detach_all_and_attach(self):
        tiles = self.test_place_multiple()
        dashboard_id = tiles[0].dashboard_id
        for t in tiles:
            detach_tile(t)

        tile = call(tiles_test.TileTest.test_insert, dashboard_id=dashboard_id)
        res = place_tile(tile)
        vo = {'width': 4, 'height': 4, 'x': 0, 'y': 0}
        self.assertEqual(vo, res.new_tiles[tile])

        layout = Layout.select(tile.owner_id, tile.dashboard_id)
        self.assertEqual(res.new_layout.layout_id, layout.layout_id)
        self.assertEqual({tile.tile_id: vo}, layout.layout_dict)

    def test_set_new_layout(self):
        tiles = self.test_place_multiple()
        layout = Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        layout.layout_dict = dict(layout.layout_dict.items()[:3])
        res = layout.set()
        self.assertIsNotNone(res)


        layout2 = Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        layout.layout_dict = dict(layout.layout_dict.items()[:2])
        res = layout.set()
        self.assertIsNotNone(res)

        layout2_reselected = Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        self.assertEqual(2, len(layout2_reselected.layout_dict))

        # wrong id
        res = layout2_reselected.set(tiles[0].owner_id, tiles[0].dashboard_id,
                                     layout2.layout_id)
        self.assertIsNone(res)


class RepackTest(unittest.TestCase):

    def test_no_repack(self):
        tile_config = {
            'tags': ['p1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {
                'tpcreator_uispec': [{'tag': 'p1:10', 'prefix': 'p1:'}],
            }
        }
        rd = ReportData('r')
        master_tile = Tile.insert(rd.owner_id, rd.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(master_tile)

        ri1 = rd.report.process_input('0', tags=['p1:8'], handle_tpcreator=False).report_instance
        ri2 = rd.report.process_input('0', tags=['p1:12'],
                                      handle_tpcreator=False).report_instance
        ri3 = rd.report.process_input('0', tags=['p1:6'],
                                     handle_tpcreator=False).report_instance

        layout_rows_tpcreator = c.dao.LayoutDAO.select_layout_by_report_multi(
            rd.owner_id, rd.report_id, [], 'tpcreator', 100)
        mods = [
            tpcreator.tpcreator_mod(ri1, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri2, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri3, layout_rows_tpcreator[0]),
        ]
        layouts.apply_mods(mods, rd.owner_id, rd.dashboard_id, None)

        self.assertEqual([['p1:10'], ['p1:8'], ['p1:12'], ['p1:6']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])

        return rd

    def test_repack(self):
        rd = self.test_no_repack()

        layouts.apply_mods([layouts.repack_mod()], rd.owner_id, rd.dashboard_id, None)

        self.assertEqual([['p1:10'], ['p1:6'], ['p1:8'], ['p1:12']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])

    # @unittest.skip('Performance testing - run manually')
    def test_repack_performance(self):
        rd = ReportData('r')

        tile_config = {
            'tags': ['p1:1'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {
                'tpcreator_uispec': [{'tag': 'p1:1', 'prefix': 'p1:'}],
            }
        }
        tile = Tile.insert(rd.owner_id, rd.report_id, rd.dashboard_id, tile_config)
        place_tile(tile)

        start = time()
        for i in range(1, 201):
            rd.report.process_input('1', tags=['p1:%s' % i])
        print 'Tiles created in %.1f' % ((time() - start) * 1000)

        start = time()
        layouts.repack(rd.owner_id, rd.dashboard_id)
        print 'Single repack in %.1f' % ((time() - start) * 1000)

    def test_repack_dont_put_master_first(self):
        rd = self.test_no_repack()

        layouts.apply_mods([layouts.repack_mod(put_master_first=False)],
                           rd.owner_id, rd.dashboard_id, None)

        self.assertEqual([['p1:6'], ['p1:8'], ['p1:10'], ['p1:12']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])

        self.assertFalse(rd.tiles_sorted_by_vo()[0].is_master_tile())
        self.assertTrue(rd.tiles_sorted_by_vo()[2].is_master_tile())

        return rd

    def test_promote_first_as_master(self):
        rd = self.test_repack_dont_put_master_first()

        layouts.apply_mods([layouts.promote_first_as_master_mod(),
                            layouts.repack_mod()],
                           rd.owner_id, rd.dashboard_id, None)

        #for tile in rd.tiles_sorted_by_vo():
        #    print tile.tile_id, tile.tags, tile.get_master_tile_id()

        self.assertEqual([['p1:6'], ['p1:8'], ['p1:10'], ['p1:12']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])

        first_tile = rd.tiles_sorted_by_vo()[0]
        self.assertTrue(first_tile.is_master_tile())
        for tile in rd.tiles_sorted_by_vo()[1:]:
            self.assertFalse(tile.is_master_tile())
            self.assertEqual(first_tile.tile_id, tile.get_master_tile_id())
        return rd

    def test_promote_first_as_master_multiple_masters(self):
        rd = self.test_repack_dont_put_master_first()

        tile_config = {
            'tags': ['q1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {
                'tpcreator_uispec': [{'tag': 'q1:10', 'prefix': 'q1:'}],
            }
        }
        r2 = Report.insert(rd.owner_id, 'r2')
        master_tile2 = Tile.insert(rd.owner_id, r2.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(master_tile2)

        ri1 = r2.process_input('0', tags=['q1:8'], handle_tpcreator=False).report_instance
        ri2 = r2.process_input('0', tags=['q1:12'],
                                      handle_tpcreator=False).report_instance
        ri3 = r2.process_input('0', tags=['q1:6'],
                                      handle_tpcreator=False).report_instance
        ri4 = rd.report.process_input('0', tags=['p1:2'],
                                      handle_tpcreator=False).report_instance

        layout_rows_tpcreator = c.dao.LayoutDAO.select_layout_by_report_multi(
            rd.owner_id, rd.report_id, [], 'tpcreator', 100)
        mods = [
            tpcreator.tpcreator_mod(ri1, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri2, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri3, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri4, layout_rows_tpcreator[0]),
            layouts.repack_mod(put_master_first=False),
        ]
        layouts.apply_mods(mods, rd.owner_id, rd.dashboard_id, None)

        self.assertEqual([['p1:2'], ['p1:6'], ['p1:8'], ['p1:10'], ['p1:12'],
                          ['q1:6'], ['q1:8'], ['q1:10'], ['q1:12']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])

        master1 = rd.get_tile_by_tags(['p1:10'])
        master2 = rd.get_tile_by_tags(['q1:10'])
        self.assertTrue(master1.is_master_tile())
        self.assertTrue(master2.is_master_tile())
        for tile in rd.tiles_sorted_by_vo():
            if tile.tags[0].startswith('p') and tile.tile_id != master1.tile_id:
                self.assertEqual(master1.tile_id, tile.get_master_tile_id())
            elif tile.tags[0].startswith('q') and tile.tile_id != master2.tile_id:
                self.assertEqual(master2.tile_id, tile.get_master_tile_id())

        layouts.apply_mods([
            layouts.promote_first_as_master_mod(),
            layouts.if_mod(lambda layout_mod: layout_mod.tile_replacement,
                           layouts.repack_mod())
        ], rd.owner_id, rd.dashboard_id, None)

        self.assertEqual([['p1:2'], ['p1:6'], ['p1:8'], ['p1:10'], ['p1:12'],
                          ['q1:6'], ['q1:8'], ['q1:10'], ['q1:12']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])

        master1 = rd.get_tile_by_tags(['p1:2'])
        master2 = rd.get_tile_by_tags(['q1:6'])
        self.assertTrue(master1.is_master_tile())
        self.assertTrue(master2.is_master_tile())
        for tile in rd.tiles_sorted_by_vo():
            if tile.tags[0].startswith('p') and tile.tile_id != master1.tile_id:
                self.assertEqual(master1.tile_id, tile.get_master_tile_id())
            elif tile.tags[0].startswith('q') and tile.tile_id != master2.tile_id:
                self.assertEqual(master2.tile_id, tile.get_master_tile_id())

    def test_promote_first_as_master_multiple_masters_one_apply_mods_run(self):
        rd = self.test_repack_dont_put_master_first()

        tile_config = {
            'tags': ['q1:10'],
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {
                'tpcreator_uispec': [{'tag': 'q1:10', 'prefix': 'q1:'}],
            }
        }
        r2 = Report.insert(rd.owner_id, 'r2')
        master_tile2 = Tile.insert(rd.owner_id, r2.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(master_tile2)

        ri1 = r2.process_input('0', tags=['q1:8'], handle_tpcreator=False).report_instance
        ri2 = r2.process_input('0', tags=['q1:12'],
                               handle_tpcreator=False).report_instance
        ri3 = r2.process_input('0', tags=['q1:6'],
                               handle_tpcreator=False).report_instance
        ri4 = rd.report.process_input('0', tags=['p1:2'],
                                      handle_tpcreator=False).report_instance

        layout_rows_tpcreator = c.dao.LayoutDAO.select_layout_by_report_multi(
            rd.owner_id, rd.report_id, [], 'tpcreator', 100)
        mods = [
            tpcreator.tpcreator_mod(ri1, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri2, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri3, layout_rows_tpcreator[0]),
            tpcreator.tpcreator_mod(ri4, layout_rows_tpcreator[0]),
            layouts.repack_mod(put_master_first=False),
            layouts.promote_first_as_master_mod(),
            layouts.if_mod(lambda layout_mod: layout_mod.tile_replacement,
                           layouts.repack_mod())
        ]
        layouts.apply_mods(mods, rd.owner_id, rd.dashboard_id, None)

        self.assertEqual([['p1:2'], ['p1:6'], ['p1:8'], ['p1:10'], ['p1:12'],
                          ['q1:6'], ['q1:8'], ['q1:10'], ['q1:12']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])

        master1 = rd.get_tile_by_tags(['p1:2'])
        master2 = rd.get_tile_by_tags(['q1:6'])
        self.assertTrue(master1.is_master_tile())
        self.assertTrue(master2.is_master_tile())
        for tile in rd.tiles_sorted_by_vo():
            if tile.tags[0].startswith('p') and tile.tile_id != master1.tile_id:
                self.assertEqual(master1.tile_id, tile.get_master_tile_id())
            elif tile.tags[0].startswith('q') and tile.tile_id != master2.tile_id:
                self.assertEqual(master2.tile_id, tile.get_master_tile_id())


class LayoutClassTest(unittest.TestCase):

    def test_select_multi(self):
        owner_id = uuid.uuid1()

        d_id1 = uuid.uuid1()
        d_id2 = uuid.uuid1()
        r = Report.insert(owner_id, 'r')

        res = Layout.select_multi(owner_id, [d_id1, d_id2])
        self.assertEqual([], res)

        tile_config = {
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
        }

        t1 = Tile.insert(owner_id, r.report_id, d_id1, tile_config)
        place_tile(t1)

        res = Layout.select_multi(owner_id, [d_id1, d_id2])
        self.assertEqual(1, len(res))
        self.assertEqual(Layout.select(owner_id, d_id1).layout_id, res[0].layout_id)

        t2 = Tile.insert(owner_id, r.report_id, d_id2, tile_config)
        place_tile(t2)
        res = Layout.select_multi(owner_id, [d_id1, d_id2])
        self.assertEqual(2, len(res))
        self.assertEqual(Layout.select(owner_id, d_id1).layout_id, res[0].layout_id)
        self.assertEqual(Layout.select(owner_id, d_id2).layout_id, res[1].layout_id)


class LayoutModuleTest(unittest.TestCase):

    def test_replace_wrong(self):
        tiles = call(TilePlacingDetachingTest.test_place_multiple)
        tile_other = tiles[0].insert_similar(tiles[0].get_tile_config())
        tile_other2 = tiles[0].insert_similar(tiles[0].get_tile_config())
        res = layouts.replace_tiles({tile_other: tile_other2}, None)
        self.assertIsNone(res)

    def test_layout_mod_nop_many_tries(self):
        def nop(layout_mod):
            return
        tiles = call(TilePlacingDetachingTest.test_place_multiple)
        orig_layout = layouts.Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        lmr = layouts.apply_mods([nop], tiles[0].owner_id, tiles[0].dashboard_id, None)
        self.assertTrue(lmr)
        self.assertEqual(orig_layout.layout_dict, lmr.old_layout.layout_dict)
        self.assertEqual(orig_layout.layout_dict, lmr.new_layout.layout_dict)
        self.assertEqual(orig_layout.layout_id, lmr.old_layout.layout_id)
        self.assertEqual(orig_layout.layout_id, lmr.new_layout.layout_id)

    def test_layout_mod_nop_single_try(self):
        def nop(layout_mod):
            return
        tiles = call(TilePlacingDetachingTest.test_place_multiple)
        orig_layout = layouts.Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        lmr = layouts.apply_mods([nop], tiles[0].owner_id, tiles[0].dashboard_id, orig_layout.layout_id)
        self.assertTrue(lmr)
        self.assertEqual(orig_layout.layout_dict, lmr.old_layout.layout_dict)
        self.assertEqual(orig_layout.layout_dict, lmr.new_layout.layout_dict)
        self.assertEqual(orig_layout.layout_id, lmr.old_layout.layout_id)
        self.assertEqual(orig_layout.layout_id, lmr.new_layout.layout_id)

    def test_layout_mod_modify_vo_many_tries(self):
        def modify_vo(layout_mod):
            layout_mod.layout.layout_dict.items()[0][1]['x'] = 100
        tiles = call(TilePlacingDetachingTest.test_place_multiple)
        orig_layout = layouts.Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        lmr = layouts.apply_mods([modify_vo], tiles[0].owner_id, tiles[0].dashboard_id, None)
        self.assertTrue(lmr)
        self.assertEqual(orig_layout.layout_dict, lmr.old_layout.layout_dict)
        self.assertNotEqual(orig_layout.layout_dict, lmr.new_layout.layout_dict)
        self.assertEqual(orig_layout.layout_id, lmr.old_layout.layout_id)
        self.assertNotEqual(orig_layout.layout_id, lmr.new_layout.layout_id)

    def test_layout_mod_modify_vo_single_try(self):
        def modify_vo(layout_mod):
            layout_mod.layout.layout_dict.items()[0][1]['x'] = 100
        tiles = call(TilePlacingDetachingTest.test_place_multiple)
        orig_layout = layouts.Layout.select(tiles[0].owner_id, tiles[0].dashboard_id)
        lmr = layouts.apply_mods([modify_vo], tiles[0].owner_id, tiles[0].dashboard_id, orig_layout.layout_id)
        self.assertTrue(lmr)
        self.assertEqual(orig_layout.layout_dict, lmr.old_layout.layout_dict)
        self.assertNotEqual(orig_layout.layout_dict, lmr.new_layout.layout_dict)
        self.assertEqual(orig_layout.layout_id, lmr.old_layout.layout_id)
        self.assertNotEqual(orig_layout.layout_id, lmr.new_layout.layout_id)

