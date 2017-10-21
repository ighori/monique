import unittest
import uuid
from uuid import UUID
from copy import deepcopy

from mqe import layouts
from mqe.layouts import place_tile, detach_tile, Layout, repack
from mqe import dataseries
from mqe.tiles import Tile
from mqe import tpcreator
from mqe import c

from mqe.tests.tutil import call, ReportData
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
        ld_expected[ud] = {'height': 5, 'width': 3, 'x': 0, 'y': 5}
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

    def test_repack_dont_put_master_first(self):
        rd = self.test_no_repack()

        layouts.apply_mods([layouts.repack_mod(put_master_first=False)],
                           rd.owner_id, rd.dashboard_id, None)

        self.assertEqual([['p1:6'], ['p1:8'], ['p1:10'], ['p1:12']],
                         [tile.tags for tile in rd.tiles_sorted_by_vo()])


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

