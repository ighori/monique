import uuid
import unittest
import json
from collections import OrderedDict

from mqe import c
from mqe.dataseries import SeriesSpec
from mqe.tiles import Tile
from mqe.reports import Report
from mqe import layouts
from mqe.layouts import Layout
from mqe import sscreator
from mqe import dashboards

from mqe.tests.tutil import new_report_data, patch


class SSCSTest(unittest.TestCase):


    def test_sscs(self):
        tile_config = {
            'tags': ['ip:192.168.1.1'],
            'tw_type': 'Range',
            'series_spec_list': [
                SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                SeriesSpec(2, 0, dict(op='eq', args=['john'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
                'tile_title': 'Points by user'
            }
        }
        tile_config['tile_options']['sscs'] = tile_config['series_spec_list'][0]

        rd = new_report_data('points')

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)

        d = [OrderedDict([('user_name', 'john'), ('is_active', True), ('points', 128)]),
             OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 210)])]
        res = rd.report.process_input(json.dumps(d), tags=tile_config['tags'])
        tile = rd.only_tile_from_layout()
        self.assertEqual(2, len(tile.series_specs()))

        d = [OrderedDict([('user_name', 'robert'), ('is_active', True), ('points', 128)]),
             OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 210)])]
        res = rd.report.process_input(json.dumps(d), tags=tile_config['tags'])
        tile = rd.only_tile_from_layout()
        self.assertEqual(3, len(tile.series_specs()))
        self.assertEqual(SeriesSpec(2, 0, dict(op='eq', args=['robert'])),
                         tile.series_specs()[-1])

        d = [OrderedDict([('user_name', 'robert'), ('is_active', True), ('points', 128)]),
             OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 210)]),
             OrderedDict([('user_name', 'robert2'), ('is_active', True), ('points', 210)])]
        res = rd.report.process_input(json.dumps(d), tags=tile_config['tags'])

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        res = rd.report.process_input(json.dumps(d), tags=tile_config['tags'])

        tile = rd.only_tile_from_layout()
        self.assertEqual(5, len(tile.series_specs()))
        self.assertEqual(SeriesSpec(2, 0, dict(op='eq', args=['robert3'])),
                         tile.series_specs()[-1])

        return rd, tile


    def test_sscs_override_static_name(self):
        tile_config = {
            'tags': ['ip:192.168.1.1'],
            'tw_type': 'Range',
            'series_spec_list': [
                SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                SeriesSpec(2, 0, dict(op='eq', args=['john'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
                'tile_title': 'Points by user'
            }
        }
        tile_config['series_spec_list'][0].params['static_name'] = 'monique_points'
        self.assertEqual('monique_points', tile_config['series_spec_list'][0].name())
        tile_config['tile_options']['sscs'] = tile_config['series_spec_list'][0]

        rd = new_report_data('points')

        tile = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile)

        d = [OrderedDict([('user_name', 'robert'), ('is_active', True), ('points', 128)]),
             OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 210)])]
        res = rd.report.process_input(json.dumps(d), tags=tile_config['tags'])
        tile = rd.only_tile_from_layout()
        self.assertEqual(3, len(tile.series_specs()))
        expected_ss = SeriesSpec(2, 0, dict(op='eq', args=['robert']))
        self.assertEqual('robert', expected_ss.name())
        self.assertEqual('robert', tile.series_specs()[-1].name())

    def test_sscs_different_tag(self):
        rd, tile = self.test_sscs()

        tile_config = {
            'tags': [],
            'tw_type': 'Range',
            'series_spec_list': [
                SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
                SeriesSpec(2, 0, dict(op='eq', args=['john'])),
            ],
            'tile_options': {
                'seconds_back': 86400,
                'tile_title': 'Points by user'
            }
        }
        tile_config['tile_options']['sscs'] = tile_config['series_spec_list'][1]

        tile2 = Tile.insert(rd.owner_id, rd.report.report_id, rd.dashboard_id, tile_config)
        layouts.place_tile(tile2)

        d = [
             OrderedDict([('user_name', 'robert'), ('is_active', True), ('points', 128)]),
             OrderedDict([('user_name', 'monique2'), ('is_active', True), ('points', 210)]),
             OrderedDict([('user_name', 'monique3'), ('is_active', True), ('points', 210)]),
             OrderedDict([('user_name', 'monique4'), ('is_active', True), ('points', 210)]),
             OrderedDict([('user_name', 'robert2'), ('is_active', True), ('points', 210)]),
        ]
        res = rd.report.process_input(json.dumps(d))
        tile2 = rd.tile_from_layout(1, 2)
        self.assertEqual(7, len(tile2.series_specs()))
        self.assertEqual(SeriesSpec(2, 0, dict(op='eq', args=['robert2'])),
                         tile2.series_specs()[-1])

        d = [OrderedDict([('user_name', 'monique4'), ('is_active', True), ('points', 128)])]
        res = rd.report.process_input(json.dumps(d), tags=['ip:192.168.1.1'])

        tile2 = rd.tile_from_layout(0, 2)
        self.assertEqual(7, len(tile2.series_specs()))

        tile = rd.tile_from_layout(1, 2)
        self.assertEqual(6, len(tile.series_specs()))
        self.assertEqual(SeriesSpec(2, 0, dict(op='eq', args=['monique4'])),
                         tile.series_specs()[-1])

    def test_sscs_virtual_column(self):
        owner_id = uuid.uuid1()
        od = dashboards.OwnerDashboards(owner_id)
        dashboard_id = od.dashboards[0].dashboard_id

        r = Report.insert(owner_id, 'r')
        tile_config = {
            'tw_type': 'Single',
            'series_spec_list': [
                SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
            'tile_options': {}
        }
        tile_config['tile_options']['sscs'] = tile_config['series_spec_list'][0]

        tile = Tile.insert(owner_id, r.report_id, dashboard_id, tile_config)
        layouts.place_tile(tile)

        for inp in ['0', '1', '2\n3', '3\n4\n5\n']:
            res = r.process_input(inp)

        tile = Tile.select(dashboard_id, Layout.select(owner_id, dashboard_id).layout_dict.keys()[0])
        self.assertEqual(3, len(tile.series_specs()))
        return tile, r

    def test_sscs_set_layout_fails(self):
        tile, report = self.test_sscs_virtual_column()
        dashboard_id_1 = tile.dashboard_id

        dashboard_id_2 = uuid.uuid1()
        tile2 = tile.copy(dashboard_id_2)
        self.assertEqual(3, len(tile.series_specs()))
        self.assertTrue(layouts.place_tile(tile2))

        report.process_input('3\n4\n5\n6')

        layout2 = Layout.select(tile.owner_id, dashboard_id_2)
        self.assertEqual(1, len(layout2.layout_dict))
        self.assertEqual(4, len(layout2.tile_dict.keys()[0].series_specs()))

        layout1 = Layout.select(tile.owner_id, dashboard_id_1)
        self.assertEqual(1, len(layout1.layout_dict))
        self.assertEqual(4, len(layout1.tile_dict.keys()[0].series_specs()))

        dashboard_id_3 = uuid.uuid1()
        tile3 = tile.copy(dashboard_id_3)
        self.assertEqual(3, len(tile.series_specs()))
        self.assertTrue(layouts.place_tile(tile3))

        report.process_input('3\n4\n5\n6\n7')

        layout3 = Layout.select(tile.owner_id, dashboard_id_3)
        self.assertEqual(1, len(layout3.layout_dict))
        self.assertEqual(5, len(layout3.tile_dict.keys()[0].series_specs()))

        layout2 = Layout.select(tile.owner_id, dashboard_id_2)
        self.assertEqual(1, len(layout2.layout_dict))
        self.assertEqual(5, len(layout2.tile_dict.keys()[0].series_specs()))

        layout1 = Layout.select(tile.owner_id, dashboard_id_1)
        self.assertEqual(1, len(layout1.layout_dict))
        self.assertEqual(5, len(layout1.tile_dict.keys()[0].series_specs()))

        mock_data = {'call': 0}
        def mock__set_new_layout(*args, **kwargs):
            mock_data['call'] += 1
            if mock_data['call'] >= 2:
                return None
            return mock__set_new_layout.old_fun(*args, **kwargs)

        with patch(layouts.Layout, layouts.Layout.set, mock__set_new_layout):
            report.process_input('3\n4\n5\n6\n7\n8')

        layout3 = Layout.select(tile.owner_id, dashboard_id_3)
        self.assertEqual(1, len(layout3.layout_dict))

        layout2 = Layout.select(tile.owner_id, dashboard_id_2)
        self.assertEqual(1, len(layout2.layout_dict))

        layout1 = Layout.select(tile.owner_id, dashboard_id_1)
        self.assertEqual(1, len(layout1.layout_dict))

        self.assertEqual([5, 5, 6], sorted(len(layout_x.tile_dict.keys()[0].series_specs())
                                           for layout_x in (layout1, layout2, layout3)))

        mock_data = {'call': 0}
        def mock__set_new_layout(*args, **kwargs):
            mock_data['call'] += 1
            if mock_data['call'] in (2, 3, 4):
                return None
            mock_data['call'] = mock_data['call'] + 1
            return mock__set_new_layout.old_fun(*args, **kwargs)

        with patch(layouts.Layout, layouts.Layout.set, mock__set_new_layout):
            report.process_input('3\n4\n5\n6\n7\n8\n9')

        layout3 = Layout.select(tile.owner_id, dashboard_id_3)
        self.assertEqual(1, len(layout3.layout_dict))
        self.assertEqual(7, len(layout3.tile_dict.keys()[0].series_specs()))

        layout2 = Layout.select(tile.owner_id, dashboard_id_2)
        self.assertEqual(1, len(layout2.layout_dict))
        self.assertEqual(7, len(layout2.tile_dict.keys()[0].series_specs()))

        layout1 = Layout.select(tile.owner_id, dashboard_id_1)
        self.assertEqual(1, len(layout1.layout_dict))
        self.assertEqual(7, len(layout1.tile_dict.keys()[0].series_specs()))

    def test_deleting_layout_by_report_row(self):
        tile, report = self.test_sscs_virtual_column()
        report.process_input('3\n4\n5\n6')

        tile = Tile.select(tile.dashboard_id, Layout.select(tile.owner_id, tile.dashboard_id).layout_dict.keys()[0])
        self.assertEqual(4, len(tile.series_specs()))

        layouts.detach_tile(tile)

        rows = c.dao.LayoutDAO.select_layout_by_report_multi(tile.owner_id,
                                                             tile.report_id, [], 'sscs', 100)
        self.assertTrue(rows)

        report.process_input('3\n4\n5\n6')

        rows = c.dao.LayoutDAO.select_layout_by_report_multi(tile.owner_id,
                                                             tile.report_id, [], 'sscs', 100)
        self.assertFalse(rows)

    def test_deleting_layout_by_report_row_after_deleting_dashboard(self):
        tile, report = self.test_sscs_virtual_column()
        report.process_input('3\n4\n5\n6')

        tile = Tile.select(tile.dashboard_id, Layout.select(tile.owner_id, tile.dashboard_id).layout_dict.keys()[0])
        self.assertEqual(4, len(tile.series_specs()))

        od = dashboards.OwnerDashboards(tile.owner_id)
        for db in od.dashboards:
            db.delete()

        rows = c.dao.LayoutDAO.select_layout_by_report_multi(tile.owner_id,
                                                             tile.report_id, [], 'sscs', 100)
        self.assertTrue(rows)

        report.process_input('3\n4\n5\n6\n7\n8')

        rows = c.dao.LayoutDAO.select_layout_by_report_multi(tile.owner_id,
                                                             tile.report_id, [], 'sscs', 100)
        self.assertFalse(rows)


    def test_sscreator_as_mod(self):
        owner_id = uuid.uuid4()
        dashboard_id = uuid.uuid4()

        tile_config = {
            'series_spec_list': [
                SeriesSpec(1, 0, dict(op='eq', args=['label1'])),
            ],
            'tile_options': {
                'sscs': SeriesSpec(1, 0, dict(op='eq', args=['label1']))
            }
        }
        r = Report.insert(owner_id, 'r')
        tile = Tile.insert(owner_id, r.report_id, dashboard_id, tile_config)
        layouts.place_tile(tile)
        ri1 = r.process_input('label1 1',
                              handle_sscreator=False).report_instance
        ri2 = r.process_input('label11 11\nlabel12 12',
                              handle_sscreator=False).report_instance
        ri3 = r.process_input('label21 21\nlabel22 22',
                              handle_sscreator=False).report_instance
        layout_rows = c.dao.LayoutDAO.select_layout_by_report_multi(owner_id, r.report_id,
                                                                    [], 'sscs', 100)
        mods = [sscreator.sscreator_mod(ri1, layout_rows[0]),
                sscreator.sscreator_mod(ri2, layout_rows[0]),
                sscreator.sscreator_mod(ri3, layout_rows[0])]
        layouts.apply_mods(mods, owner_id, dashboard_id, None)

        layout = Layout.select(owner_id, dashboard_id)
        tile = layout.tile_dict.keys()[0]

        self.assertEqual(5, len(tile.series_specs()))

        tile_ids = dashboards._select_tile_ids(dashboard_id)
        self.assertEqual(1, len(tile_ids))

