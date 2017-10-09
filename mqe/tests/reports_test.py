import uuid

import unittest
import datetime

from mqe import reports
from mqe.reports import Report
from mqetables.enrichment import EnrichedTable
from mqetables.parsing import Table
from mqe.dataseries import SeriesSpec
from mqe.dashboards import OwnerDashboards
from mqe.tiles import Tile
from mqe import layouts
from mqe import util
from mqe.tests.tutil import enable_logging

utcnow = datetime.datetime.utcnow


class ReportTest(unittest.TestCase):

    def test_insert_and_select(self):
        owner_id = uuid.uuid4()
        r1 = Report.insert(owner_id, 'rep')
        self.assertIsNotNone(r1)
        r2 = Report.insert(owner_id, 'rep')
        self.assertIsNone(r2)

        rs = Report.select_by_name(owner_id, 'rep')
        self.assertEqual(r1, rs)

        rs2 = Report.select(rs.report_id)
        self.assertEqual(rs2, rs)

        r2 = Report.insert(owner_id, 'rep_2')
        self.assertIsNotNone(r2)
        self.assertNotEqual(r2, rs)

        r3 = Report.select_or_insert(owner_id, 'rep_3')
        r3_2 = Report.select(r3.report_id)
        r3_3 = Report.select_or_insert(owner_id, 'rep_3')
        self.assertEqual(r3, r3_2)
        self.assertEqual(r3, r3_3)

    def test_process_input(self):
        owner_id = uuid.uuid4()
        r = Report.select_or_insert(owner_id, 'pi')
        dt = datetime.datetime(2010, 5, 30, 6, 30)
        res = r.process_input('10 20 30', tags=['t1', 't2', 't3'], created=dt, extra_ri_data=[1,2,3])
        self.assertEqual(['t1', 't2', 't3'], res.report_instance.all_tags)
        self.assertEqual(dt, res.report_instance.created)
        self.assertEqual(EnrichedTable(Table([['10', '20', '30']])), res.report_instance.table)
        self.assertEqual([1,2,3], res.report_instance.fetch_extra_ri_data())

        desc = res.report_instance.desc(True, True)
        self.assertEqual(res.report_instance.report_instance_id.hex, desc['id'])
        self.assertEqual(1, len(desc['rows']))
        self.assertIn(' 20 ', desc['input'])

    def test_process_input_format_spec(self):
        owner_id = uuid.uuid4()
        r = Report.select_or_insert(owner_id, 'pi')
        res = r.process_input('v1:10\nv2:20', input_type='csv', ip_options={'delimiter': ':'},
                              force_header=[0])
        self.assertEqual(EnrichedTable(Table([['v1', '10'], ['v2', '20']], [0])),
                         res.report_instance.table)


    def test_process_input_invalid_args(self):
        owner_id = uuid.uuid4()
        r = Report.select_or_insert(owner_id, 'pi')

        self.assertRaises(AssertionError, lambda: r.process_input(43))
        self.assertRaises(ValueError, lambda: r.process_input('3', created=datetime.datetime(1990, 3, 4)))

    def test_fetch_instances(self):
        owner_id = uuid.uuid4()
        r = Report.select_or_insert(owner_id, 'pi')
        for i in range(4):
            r.process_input(str(i), tags=['t1', 't2'])
        r.process_input('4', tags=['t1'], created=datetime.datetime.utcnow())
        for i in range(5, 8):
            r.process_input(str(i), tags=['t1'])
        r.process_input('-1', tags=['t2'], created=datetime.datetime.utcnow() - datetime.timedelta(seconds=10))

        all_ris = r.fetch_instances(datetime.datetime.utcnow() - datetime.timedelta(days=1),
                                    datetime.datetime.utcnow())
        self.assertEqual('-1 0 1 2 3 4 5 6 7'.split(), [ri['input_string'] for ri in all_ris])

        ris = r.fetch_instances(all_ris[4].created, datetime.datetime.utcnow())
        self.assertEqual('3 4 5 6 7'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[4].created, datetime.datetime.utcnow(), order='desc')
        self.assertEqual('7 6 5 4 3'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[2].created, all_ris[4].created)
        self.assertEqual('1 2 3'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(datetime.datetime.utcnow(), datetime.datetime.utcnow())
        self.assertFalse(ris)

        ris = r.fetch_instances(datetime.datetime.utcnow() - datetime.timedelta(days=10), datetime.datetime.utcnow() - datetime.timedelta(days=8))
        self.assertFalse(ris)

        ris = r.fetch_instances(datetime.datetime.utcnow() - datetime.timedelta(days=10), datetime.datetime.utcnow(), order='desc', limit=1)
        self.assertEqual('7', ris[0]['input_string'])

        ris = r.fetch_instances(all_ris[2].created, all_ris[4].created, limit=2)
        self.assertEqual('1 2'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[2].created, all_ris[4].created, limit=2, order='desc')
        self.assertEqual('3 2'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[4].created, datetime.datetime.utcnow(), tags=['t2'])
        self.assertEqual('3'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(after=all_ris[-3].report_instance_id, limit=100)
        self.assertEqual('6 7'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(after=all_ris[-3].report_instance_id, before=all_ris[-1].report_instance_id)
        self.assertEqual('6'.split(), [ri['input_string'] for ri in ris])

    def test_fetch_instances_same_dt(self):
        owner_id = uuid.uuid1()
        r = reports.Report.insert(owner_id, 'rname')

        for i in range(10):
            r.process_input(str(i), created=datetime.datetime(2017, 5, 5))

        ris = r.fetch_instances(from_dt=datetime.datetime(2017, 5, 1), to_dt=utcnow())
        self.assertEqual(10, len(ris))
        self.assertEqual(set(map(str, range(10))), set([v.input_string for v in ris]))

        ris = r.fetch_instances(from_dt=datetime.datetime(2017, 5, 5), to_dt=utcnow())
        self.assertEqual(10, len(ris))
        self.assertEqual(set(map(str, range(10))), set([v.input_string for v in ris]))

        ris = r.fetch_instances(from_dt=datetime.datetime(2017, 5, 5),
                                to_dt=datetime.datetime(2017, 5, 5))
        self.assertEqual(10, len(ris))
        self.assertEqual(set(map(str, range(10))), set([v.input_string for v in ris]))

        ris = r.fetch_instances(from_dt=datetime.datetime(2017, 5, 5, 0, 0, 0, 1),
                                to_dt=datetime.datetime(2017, 5, 5, 0, 0, 0, 1))
        if ris:
            print ris[0].input_string
        self.assertEqual(0, len(ris))

    def create_multi_day_report(self):
        owner_id = uuid.uuid4()
        r = Report.select_or_insert(owner_id, 'pi')
        def process(val, minus_days, tags):
            res = r.process_input(str(val), created=datetime.datetime.utcnow()-datetime.timedelta(days=minus_days), tags=tags)
            self.assertIsNotNone(res.report_instance)

        process(0, 500, ['t1', 't2'])
        process(-1, 532, ['t2'])
        process(1, 432, ['t1', 't2'])
        process(2, 410, ['t1', 't2'])
        process(3, 360, ['t1', 't2'])
        process(4, 330, ['t1'])
        process(5, 73, ['t1'])
        process(6, 12, ['t2'])
        process(7, 11, ['t1'])

        all_ris = r.fetch_instances(datetime.datetime.utcnow() - datetime.timedelta(days=600),
                                    datetime.datetime.utcnow())

        self.assertEqual('-1 0 1 2 3 4 5 6 7'.split(), [ri['input_string'] for ri in all_ris])

        return r, all_ris


    def test_fetch_instances_multiple_days(self):
        r, all_ris = self.create_multi_day_report()

        ris = r.fetch_instances(all_ris[4].created, datetime.datetime.utcnow())
        self.assertEqual('3 4 5 6 7'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[4].created, datetime.datetime.utcnow(), order='desc')
        self.assertEqual('7 6 5 4 3'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[2].created, all_ris[4].created)
        self.assertEqual('1 2 3'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(datetime.datetime.utcnow(), datetime.datetime.utcnow())
        self.assertFalse(ris)

        ris = r.fetch_instances(datetime.datetime.utcnow() - datetime.timedelta(days=600), datetime.datetime.utcnow() - datetime.timedelta(days=550))
        self.assertFalse(ris)

        ris = r.fetch_instances(datetime.datetime.utcnow() - datetime.timedelta(days=600), datetime.datetime.utcnow(), order='desc', limit=1)
        self.assertEqual('7', ris[0]['input_string'])

        ris = r.fetch_instances(all_ris[2].created, all_ris[4].created, limit=2)
        self.assertEqual('1 2'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[2].created, all_ris[4].created, limit=2, order='desc')
        self.assertEqual('3 2'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(all_ris[4].created, datetime.datetime.utcnow(), tags=['t2'])
        self.assertEqual('3 6'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(after=all_ris[-3].report_instance_id, limit=100)
        self.assertEqual('6 7'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(after=all_ris[-3].report_instance_id, before=all_ris[-1].report_instance_id, limit=100)
        self.assertEqual('6'.split(), [ri['input_string'] for ri in ris])

        ris = r.fetch_instances(before=all_ris[5].report_instance_id)
        self.assertEqual(5, len(ris))

    def test_fetch_single_instance(self):
        r, all_ris = self.create_multi_day_report()

        ri = r.fetch_single_instance(uuid.uuid1())
        self.assertIsNone(ri)

        ri = r.fetch_single_instance(all_ris[1].report_instance_id)
        self.assertEqual(ri, all_ris[1])

        ri = r.fetch_single_instance(all_ris[1].report_instance_id, ['t1', 't2'])
        self.assertEqual(all_ris[1].report_instance_id, ri.report_instance_id)
        self.assertEqual(['t1', 't2'], ri.all_tags)

    def test_fetch_latest_instance_id(self):
        r, all_ris = self.create_multi_day_report()

        rid = r.fetch_latest_instance_id()
        self.assertEqual(all_ris[-1].report_instance_id, rid)

        rid = r.fetch_latest_instance_id(['t2'])
        self.assertEqual(all_ris[-2].report_instance_id, rid)

        rid = r.fetch_latest_instance_id(['a'])
        self.assertIsNone(rid)

    def test_fetch_prev_next_instance(self):
        r, all_ris = self.create_multi_day_report()

        ri = r.fetch_next_instance(all_ris[4].report_instance_id)
        self.assertEqual(all_ris[5], ri)
        self.assertNotEqual(all_ris[4], ri)

        ri = r.fetch_next_instance(all_ris[-1].report_instance_id)
        self.assertIsNone(ri)

        ri = r.fetch_prev_instance(all_ris[4].report_instance_id)
        self.assertEqual(all_ris[3], ri)

        ri = r.fetch_prev_instance(all_ris[0].report_instance_id)
        self.assertIsNone(ri)

        ri = r.fetch_next_instance(all_ris[4].report_instance_id, ['t2'])
        self.assertEqual(all_ris[-2], ri)

        ri = r.fetch_next_instance(all_ris[4].report_instance_id, ['t2', 'a'])
        self.assertIsNone(ri)

    def test_find_report_instance_by_dt(self):
        r, all_ris = self.create_multi_day_report()

        ri = r.find_report_instance_by_dt(all_ris[3].created + datetime.timedelta(hours=1))
        self.assertEqual(all_ris[3], ri)

        ri = r.find_report_instance_by_dt(all_ris[3].created - datetime.timedelta(hours=1))
        self.assertEqual(all_ris[3], ri)

        ri = r.find_report_instance_by_dt(util.MIN_DATETIME)
        self.assertEqual(all_ris[0], ri)

        ri = r.find_report_instance_by_dt(util.MAX_DATETIME)
        self.assertEqual(all_ris[-1], ri)

        r2 = Report.insert(uuid.uuid1(), 'r2')
        ri = r2.find_report_instance_by_dt(util.MIN_DATETIME)
        self.assertIsNone(ri)

    def test_report_instance_count_diskspace(self):
        r, all_ris = self.create_multi_day_report()

        self.assertEqual(len(all_ris), r.report_instance_count())
        self.assertEqual(10, r.report_instance_diskspace())

    def test_delete_single_instance(self):
        r, all_ris = self.create_multi_day_report()

        r.delete_single_instance(all_ris[3].report_instance_id)
        self.assertEqual('-1 0 1 3 4 5 6 7'.split(), [ri['input_string'] for ri in r.fetch_instances()])
        self.assertEqual(len(all_ris) - 1, r.report_instance_count())

    def test_delete_single_instance_dont_update_counter(self):
        r, all_ris = self.create_multi_day_report()

        self.assertEqual(9, r.report_instance_count())
        self.assertEqual(10, r.report_instance_diskspace())
        self.assertEqual(9, reports.report_instance_count_for_owner(r.owner_id))
        self.assertEqual(10, reports.report_instance_diskspace_for_owner(r.owner_id))

        r.delete_single_instance(all_ris[3].report_instance_id, update_counters=False)
        self.assertEqual('-1 0 1 3 4 5 6 7'.split(), [ri['input_string'] for ri in r.fetch_instances()])

        self.assertEqual(9, r.report_instance_count())
        self.assertEqual(10, r.report_instance_diskspace())
        self.assertEqual(9, reports.report_instance_count_for_owner(r.owner_id))
        self.assertEqual(10, reports.report_instance_diskspace_for_owner(r.owner_id))

    def test_delete_single_instance_multiple_days(self):
        r, all_ris = self.create_multi_day_report()

        r.delete_single_instance(all_ris[-1].report_instance_id)

        latest_instance_id = r.fetch_latest_instance_id()
        self.assertTrue(latest_instance_id)
        ri = r.fetch_single_instance(latest_instance_id)
        self.assertEqual('6', ri.input_string)

        latest_instance_id = r.fetch_latest_instance_id(['t1'])
        self.assertTrue(latest_instance_id)
        ri = r.fetch_single_instance(latest_instance_id)
        self.assertEqual('5', ri.input_string)

        latest_instance_id = r.fetch_latest_instance_id(['t2'])
        self.assertTrue(latest_instance_id)
        ri = r.fetch_single_instance(latest_instance_id)
        self.assertEqual('6', ri.input_string)


        r2 = Report.insert(r.owner_id, 'r2')
        r2.process_input('1', created=utcnow()-datetime.timedelta(days=5, seconds=2))
        r2.process_input('2', created=utcnow()-datetime.timedelta(days=5, seconds=1))
        latest_instance_id = r2.fetch_latest_instance_id()
        r2.delete_single_instance(latest_instance_id)
        latest_instance_id = r2.fetch_latest_instance_id()
        ri = r2.fetch_single_instance(latest_instance_id)
        self.assertEqual('1', ri.input_string)

    def test_delete_single_instance_same_created_dt(self):
        owner_id = uuid.uuid1()
        r = Report.insert(owner_id, 'r')
        created_ris = []
        for i in xrange(20):
            res = r.process_input(str(i), created=datetime.datetime(2017, 1, 1))
            created_ris.append(res.report_instance)

        r.delete_single_instance(created_ris[10].report_instance_id)
        ris = r.fetch_instances()
        self.assertEqual({str(i) for i in xrange(20) if i != 10},
                         {ri['input_string'] for ri in ris})

    def _day_before(self, minus_days):
        return util.datetime_from_date((datetime.datetime.utcnow() - datetime.timedelta(days=minus_days)).date())

    def test_delete_multiple_instances_delete_by_tag(self):
        r, all_ris = self.create_multi_day_report()
        all_days = r.fetch_days()
        self.assertEqual(9, len(all_days))

        num = r.delete_multiple_instances(['t1'])
        self.assertEqual(7, num)

        ris = r.fetch_instances()
        self.assertEqual('-1 6'.split(), [ri['input_string'] for ri in ris])
        self.assertEqual(2, r.report_instance_count())
        self.assertEqual(3, r.report_instance_diskspace())
        days = r.fetch_days()
        self.assertEqual(2, len(days))
        days.sort()
        self.assertEqual([self._day_before(532), self._day_before(12)], days)

        latest_instance_id = r.fetch_latest_instance_id(['t1'])
        self.assertIsNone(latest_instance_id)

        latest_instance_id = r.fetch_latest_instance_id(['t2'])
        self.assertIsNotNone(latest_instance_id)

    def test_delete_multiple_instances_delete_all(self):
        r, all_ris = self.create_multi_day_report()

        self.assertEqual(9, reports.report_instance_count_for_owner(r.owner_id))
        self.assertEqual(10, reports.report_instance_diskspace_for_owner(r.owner_id))

        r.delete_multiple_instances()

        latest_instance_id = r.fetch_latest_instance_id()
        self.assertIsNone(latest_instance_id)
        self.assertFalse(r.fetch_instances())
        self.assertFalse(r.fetch_days())
        self.assertEqual(0, r.report_instance_count())
        self.assertEqual(0, r.report_instance_diskspace())
        self.assertEqual(0, reports.report_instance_count_for_owner(r.owner_id))
        self.assertEqual(0, reports.report_instance_diskspace_for_owner(r.owner_id))

    def test_delete_multiple_instances_dont_update_counter(self):
        r, all_ris = self.create_multi_day_report()

        self.assertEqual(9, reports.report_instance_count_for_owner(r.owner_id))
        self.assertEqual(10, reports.report_instance_diskspace_for_owner(r.owner_id))

        r.delete_multiple_instances(update_counters=False)

        latest_instance_id = r.fetch_latest_instance_id()
        self.assertIsNone(latest_instance_id)
        self.assertFalse(r.fetch_instances())
        self.assertFalse(r.fetch_days())
        self.assertEqual(9, r.report_instance_count())
        self.assertEqual(10, r.report_instance_diskspace())
        self.assertEqual(9, reports.report_instance_count_for_owner(r.owner_id))
        self.assertEqual(10, reports.report_instance_diskspace_for_owner(r.owner_id))

    def test_delete_multiple_instances_delete_by_ids(self):
        r, all_ris = self.create_multi_day_report()
        r.process_input('8')
        r.process_input('9')
        all_ris = r.fetch_instances()
        self.assertEqual('-1 0 1 2 3 4 5 6 7 8 9'.split(), [ri['input_string'] for ri in all_ris])
        num = r.delete_multiple_instances([], after=all_ris[1].report_instance_id,
                                    before=all_ris[-1].report_instance_id)
        self.assertEqual(8, num)
        all_ris = r.fetch_instances()
        self.assertEqual('-1 0 9'.split(), [ri['input_string'] for ri in all_ris])
        days = r.fetch_days()
        days.sort()
        self.assertEqual(3, len(days))
        self.assertEqual([self._day_before(532), self._day_before(500), self._day_before(0)], days)

        self.assertEqual(3, r.report_instance_count())
        self.assertEqual(4, r.report_instance_diskspace())

        self.assertEqual(all_ris[1].report_instance_id, r.fetch_latest_instance_id(['t1']))

    def test_delete_multiple_instances_delete_by_dts(self):
        r, all_ris = self.create_multi_day_report()
        r.process_input('8')
        r.process_input('9')
        all_ris = r.fetch_instances()
        num = r.delete_multiple_instances(['t2'], from_dt=utcnow()-datetime.timedelta(days=400),
                                          to_dt=utcnow()-datetime.timedelta(days=13))
        self.assertEqual(1, num)
        all_ris = r.fetch_instances()
        self.assertEqual('-1 0 1 2 4 5 6 7 8 9'.split(), [ri['input_string'] for ri in all_ris])

        days = r.fetch_days()
        days.sort()
        self.assertEqual(9, len(days))

    def test_delete_multiple_instances_delete_by_dts_no_tags(self):
        r, all_ris = self.create_multi_day_report()
        r.process_input('8')
        r.process_input('9')
        all_ris = r.fetch_instances()
        num = r.delete_multiple_instances([], to_dt=utcnow()-datetime.timedelta(days=100))
        self.assertEqual(6, num)
        all_ris = r.fetch_instances()
        self.assertEqual('5 6 7 8 9'.split(), [ri['input_string'] for ri in all_ris])

        days = r.fetch_days()
        days.sort()
        self.assertEqual(4, len(days))

    def test_fetch_days(self):
        r, all_ris = self.create_multi_day_report()

        dts = r.fetch_days()
        self.assertEqual(9, len(dts))
        for dt in dts:
            self.assertIsInstance(dt, datetime.datetime)

        r2 = Report.insert(uuid.uuid1(), 'r2')
        self.assertFalse(r2.fetch_days())

        ri = r2.process_input('2').report_instance
        self.assertEqual(datetime.datetime.utcnow().date(), ri.created.date())

    def test_tags(self):
        r, all_ris = self.create_multi_day_report()
        self.assertTrue(r.has_tags())

        self.assertEqual(['t1', 't2'], r.fetch_tags_sample())
        self.assertEqual(['t1'], r.fetch_tags_sample('t1'))

        r2 = Report.insert(uuid.uuid1(), 'r2')
        self.assertFalse(r2.has_tags())
        self.assertFalse(r2.fetch_tags_sample('t'))

    def test_delete_simple(self):
        owner_id = uuid.uuid1()
        r1 = Report.insert(owner_id, 'r1')
        r1.process_input('1')
        r2 = Report.insert(owner_id, 'r2')
        r2.process_input('2')

        ids = reports.fetch_reports_by_name(owner_id, 'r')
        self.assertEqual(2, len(ids))

        r2.delete()

        reps = reports.fetch_reports_by_name(owner_id, 'r')
        self.assertEqual([r1], reps)
        self.assertIsNone(Report.select(r2.report_id))

    def test_delete_with_tiles(self):
        owner_id = uuid.uuid1()
        r1 = Report.insert(owner_id, 'r1')
        r1.process_input('1')
        r2 = Report.insert(owner_id, 'r2')
        r2.process_input('2')
        r3 = Report.insert(owner_id, 'r3')
        r3.process_input('3')

        tile_config = {
            'series_spec_list': [SeriesSpec(0, -1, {'op': 'eq', 'args': '0'})],
        }
        od = OwnerDashboards(owner_id)
        od.insert_dashboard('Second')
        od.insert_dashboard('Third')

        tile1_1 = Tile.insert(owner_id, r1.report_id, od.dashboards[0].dashboard_id, tile_config)
        assert tile1_1.get_tile_data()['series_data']
        tile2_1 = Tile.insert(owner_id, r2.report_id, od.dashboards[1].dashboard_id, tile_config)
        assert tile2_1.get_tile_data()['series_data']
        tile3_1 = Tile.insert(owner_id, r3.report_id, od.dashboards[2].dashboard_id, tile_config)
        assert tile3_1.get_tile_data()['series_data']
        layouts.place_tile(tile1_1)
        layouts.place_tile(tile2_1)
        layouts.place_tile(tile3_1)

        r2.delete()

        reps = reports.fetch_reports_by_name(owner_id, '')
        self.assertEqual([r1, r3], reps)

        self.assertEqual(1, len(layouts.Layout.select(owner_id, od.dashboards[0].dashboard_id).layout_dict))
        self.assertEqual(0, len(layouts.Layout.select(owner_id, od.dashboards[1].dashboard_id).layout_dict))
        self.assertEqual(1, len(layouts.Layout.select(owner_id, od.dashboards[2].dashboard_id).layout_dict))

        tile1_2 = Tile.insert(owner_id, r1.report_id, od.dashboards[0].dashboard_id, tile_config)
        tile1_3 = Tile.insert(owner_id, r1.report_id, od.dashboards[1].dashboard_id, tile_config)
        tile1_4 = Tile.insert(owner_id, r1.report_id, od.dashboards[1].dashboard_id, tile_config)
        tile1_5 = Tile.insert(owner_id, r1.report_id, od.dashboards[2].dashboard_id, tile_config)
        layouts.place_tile(tile1_2)
        layouts.place_tile(tile1_3)
        layouts.place_tile(tile1_4)
        layouts.place_tile(tile1_5)

        r1.delete()

        self.assertEqual(0, len(layouts.Layout.select(owner_id, od.dashboards[0].dashboard_id).layout_dict))
        self.assertEqual(0, len(layouts.Layout.select(owner_id, od.dashboards[1].dashboard_id).layout_dict))
        self.assertEqual(1, len(layouts.Layout.select(owner_id, od.dashboards[2].dashboard_id).layout_dict))

        reps = reports.fetch_reports_by_name(owner_id, '')
        self.assertEqual([r3], reps)

class ReportsModuleTest(unittest.TestCase):

    def test_fetch_reports_by_name(self):
        owner_id = uuid.uuid1()

        all_rs = [Report.insert(owner_id, 'rep%02d' % i) for i in range(10, 20)]
        all_rs[:0] = [Report.insert(owner_id, 'rep%02d' % i) for i in range(10)]

        rs = reports.fetch_reports_by_name(owner_id)
        self.assertEqual(all_rs, rs)

        rs = reports.fetch_reports_by_name(owner_id, limit=5)
        self.assertEqual(all_rs[:5], rs)

        rs = reports.fetch_reports_by_name(owner_id, name_prefix='rep1')
        self.assertEqual(all_rs[10:], rs)

        rs = reports.fetch_reports_by_name(owner_id, name_prefix='rep1', after_name='rep15')
        self.assertEqual(all_rs[16:], rs)

        rs = reports.fetch_reports_by_name(owner_id, after_name='rep15')
        self.assertEqual(all_rs[16:], rs)

        rs = reports.fetch_reports_by_name(owner_id, after_name='x')
        self.assertFalse(rs)

        rs = reports.fetch_reports_by_name(uuid.uuid1())
        self.assertFalse(rs)

        rs = reports.fetch_reports_by_name(owner_id, after_name='a', limit=1)
        self.assertEqual(all_rs[:1], rs)

    def test_owner_has_reports(self):
        owner_id = uuid.uuid1()

        self.assertFalse(reports.owner_has_reports(owner_id))

        Report.insert(owner_id, 'a')
        self.assertTrue(reports.owner_has_reports(owner_id))

    def test_counts(self):
        owner_id = uuid.uuid1()
        r = Report.insert(owner_id, 'a')
        r.process_input('1')
        r.process_input('2')
        r2 = Report.insert(owner_id, 'b')
        r.process_input('1')
        r.process_input('2')

        self.assertEqual(4, reports.report_instance_count_for_owner(owner_id))
        self.assertEqual(4, reports.report_instance_diskspace_for_owner(owner_id))


