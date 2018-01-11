import json
import unittest
import uuid
from collections import OrderedDict

from mqe import signals, reports, util, dataseries, tpcreator, layouts
from mqe import dashboards

from mqe.tests.tutil import call, new_report_data
from mqe.tiles import Tile


class SignalsTest(unittest.TestCase):

    def test_new_dashboard(self):
        data = []
        @signals.new_dashboard.connect
        def on_new_dashboard(c, **kwargs):
            data.append(kwargs['dashboard'])

        owner_id = uuid.uuid1()
        od = dashboards.OwnerDashboards(owner_id)
        self.assertEqual(data[0].dashboard_id, od.dashboards[0].dashboard_id)
        self.assertEqual(data[0].owner_id, od.dashboards[0].owner_id)

        od.insert_dashboard('dash2')
        self.assertEqual(2, len(data))

        od = dashboards.OwnerDashboards(owner_id)
        self.assertEqual(2, len(data))

    def test_on_new_report(self):
        data = []
        @signals.new_report.connect
        def on_new_report(c, **kwargs):
            data.append(kwargs['report'])

        owner_id = uuid.uuid1()
        r1 = reports.Report.insert(owner_id, 'r1')
        r1 = reports.Report.select_or_insert(owner_id, 'r1')
        r2 = reports.Report.select_or_insert(owner_id, 'r2')

        self.assertEqual(2, len(data))
        self.assertEqual(['r1', 'r2'], [r.report_name for r in data])

    def test_layout_modification_by_sscs(self):
        from mqe.tests import sscreator_test

        data = {}
        @signals.layout_modified.connect
        def on_tiles_replaced(c, **kwargs):
            if kwargs['reason'] != 'sscreator':
                return
            lmr = kwargs['layout_modification_result']
            self.assertNotEqual(lmr.old_layout.layout_id, lmr.new_layout.layout_id)
            data.clear()
            data.update(kwargs)

        rd, tile = call(sscreator_test.SSCSTest.test_sscs)
        self.assertEqual(tile, data['layout_modification_result'].tile_replacement.values()[0])
        self.assertEqual(rd.layout().layout_id, data['layout_modification_result'].\
                         new_layout.layout_id)

    def test_layout_modification_by_tpcreator(self):
        data = {}
        @signals.layout_modified.connect
        def on_tiles_replaced(c, **kwargs):
            if kwargs['reason'] != 'tpcreator':
                return
            data.clear()
            data.update(kwargs)


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

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['p1:20'])

        d = [OrderedDict([('user_name', 'robert3'), ('is_active', True), ('points', 128)])]
        rd.report.process_input(json.dumps(d), tags=['p1:30', 'p2:30'])

        tile = util.first(tile for tile in rd.layout().tile_dict if tile.tags == \
            ['p1:30'])
        self.assertIsNotNone(tile)
        self.assertEqual(tile, data['layout_modification_result'].new_tiles.keys()[0])
        self.assertEqual(rd.layout().layout_id, data['layout_modification_result']. \
                         new_layout.layout_id)


