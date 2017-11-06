import unittest
import uuid

from mqe import dashboards
from mqe.dashboards import OwnerDashboards, Dashboard
from mqe.tests.tutil import call, report_data
from mqe.tests import tiles_test
from mqe.reports import Report
from mqe import dataseries
from mqe.tiles import Tile
from mqe.layouts import place_tile, detach_tile



class OwnerDashboardsTest(unittest.TestCase):

    def test_init(self):
        owner_id = uuid.uuid1()
        dashboard_id = uuid.uuid1()
        od = OwnerDashboards(owner_id)

        self.assertEqual(owner_id, od.owner_id)
        self.assertEqual(1, len(od.dashboards))
        self.assertEqual(1, len(od.dashboard_by_id))
        self.assertEqual(1, len(od.dashboard_ordering_by_id))
        self.assertIsInstance(od.dashboards[0], Dashboard)
        self.assertEqual(od.dashboards[0].dashboard_name, 'Default Dashboard')
        return od

    def test_init_noinsert(self):
        owner_id = uuid.uuid1()
        dashboard_id = uuid.uuid1()
        od = OwnerDashboards(owner_id, '')

        self.assertEqual(owner_id, od.owner_id)
        self.assertEqual(0, len(od.dashboards))
        self.assertEqual(0, len(od.dashboard_by_id))
        self.assertEqual(0, len(od.dashboard_ordering_by_id))

    def test_inserting(self):
        od = self.test_init()
        od.insert_dashboard('Dash 2')
        od.insert_dashboard('Dash 3')

        od = OwnerDashboards(od.owner_id)
        od.insert_dashboard('Dash 4')
        od.insert_dashboard('Dash 5')

        self.assertEqual(5, len(od.dashboards))
        self.assertEqual(5, len(od.dashboard_by_id))
        self.assertEqual(5, len(od.dashboard_ordering_by_id))
        self.assertEqual([db.dashboard_id for db in od.dashboards],
                         od.dashboard_id_ordering)

        od = OwnerDashboards(od.owner_id)
        self.assertEqual(5, len(od.dashboards))
        self.assertEqual(5, len(od.dashboard_by_id))
        self.assertEqual(5, len(od.dashboard_ordering_by_id))
        self.assertEqual([db.dashboard_id for db in od.dashboards],
                         od.dashboard_id_ordering)
        for i in range(5):
            self.assertEqual(od.dashboard_ordering_by_id[od.dashboards[i].dashboard_id], i)

        return od

    def test_deleting(self):
        od = self.test_inserting()
        od.dashboards[1].delete()
        od = OwnerDashboards(od.owner_id)
        self.assertEqual(4, len(od.dashboards))

        self.assertEqual(['Default Dashboard', 'Dash 3', 'Dash 4', 'Dash 5'],
                         [db.dashboard_name for db in od.dashboards])
        for i in range(4):
            self.assertEqual(od.dashboard_ordering_by_id[od.dashboards[i].dashboard_id], i)

    def test_get_dashboards_displaying_report(self):
        od = self.test_inserting()

        res = od.get_dashboards_displaying_report(uuid.uuid4())
        self.assertEqual([], res)

        r = Report.insert(od.owner_id, 'r')
        tile_config = {
            'series_spec_list': [
                dataseries.SeriesSpec(0, -1, dict(op='eq', args=['0'])),
            ],
        }

        res = od.get_dashboards_displaying_report(r.report_id)
        self.assertEqual([], res)

        t1 = Tile.insert(od.owner_id, r.report_id, od.dashboards[3].dashboard_id, tile_config)
        place_tile(t1)

        res = od.get_dashboards_displaying_report(r.report_id)
        self.assertEqual(1, len(res))
        self.assertEqual('Dash 4', res[0].dashboard_name)

        t2 = Tile.insert(od.owner_id, r.report_id, od.dashboards[1].dashboard_id, tile_config)
        place_tile(t2)

        res = od.get_dashboards_displaying_report(r.report_id)
        self.assertEqual(2, len(res))
        self.assertEqual('Dash 2', res[0].dashboard_name)
        self.assertEqual('Dash 4', res[1].dashboard_name)

        t3 = Tile.insert(od.owner_id, r.report_id, od.dashboards[1].dashboard_id, tile_config)
        place_tile(t3)

        res = od.get_dashboards_displaying_report(r.report_id)
        self.assertEqual(2, len(res))
        self.assertEqual('Dash 2', res[0].dashboard_name)
        self.assertEqual('Dash 4', res[1].dashboard_name)

        detach_tile(t3)

        res = od.get_dashboards_displaying_report(r.report_id)
        self.assertEqual(2, len(res))
        self.assertEqual('Dash 2', res[0].dashboard_name)
        self.assertEqual('Dash 4', res[1].dashboard_name)

        detach_tile(t2)

        res = od.get_dashboards_displaying_report(r.report_id)
        self.assertEqual(1, len(res))
        self.assertEqual('Dash 4', res[0].dashboard_name)

        od.dashboards[3].delete()

        res = od.get_dashboards_displaying_report(r.report_id)
        self.assertEqual(0, len(res))


class DashboardTest(unittest.TestCase):

    def test_select(self):
        od = call(OwnerDashboardsTest.test_inserting)
        db = Dashboard.select(od.dashboards[0]['owner_id'], od.dashboards[0].dashboard_id)
        self.assertEqual(od.dashboards[0], db)
        self.assertNotEqual(od.dashboards[1], db)
        self.assertEqual(od.dashboards[0].dashboard_name, db.dashboard_name)

    def test_select_nonexisting(self):
        self.assertIsNone(Dashboard.select(uuid.uuid1(), uuid.uuid1()))

    def test_update_dashboard_options(self):
        od = call(OwnerDashboardsTest.test_inserting)
        db = od.dashboards[3]
        db.update(dashboard_options={'x': 10})
        db_u = Dashboard.select(db['owner_id'], db.dashboard_id)
        self.assertEqual(db.dashboard_name, db_u.dashboard_name)
        self.assertEqual({'x': 10}, db_u.dashboard_options)

    def test_update_dashboard_name(self):
        od = call(OwnerDashboardsTest.test_inserting)
        db = od.dashboards[3]
        db.update(dashboard_name='ABC', dashboard_options={'x': 20})
        db_u = Dashboard.select(db['owner_id'], db.dashboard_id)
        self.assertEqual('ABC', db_u.dashboard_name)
        self.assertEqual({'x': 20}, db_u.dashboard_options)


class DashboardsModuleTest(unittest.TestCase):

    def test_select_tile_ids(self):
        dashboard_id = uuid.uuid1()
        self.assertEqual([], dashboards._select_tile_ids(dashboard_id))

        dashboard_id = report_data('points').dashboard_id
        tile1 = call(tiles_test.TileTest.test_insert)
        tile2 = call(tiles_test.TileTest.test_insert)
        self.assertIn(tile1.tile_id, dashboards._select_tile_ids(dashboard_id))
        self.assertIn(tile2.tile_id, dashboards._select_tile_ids(dashboard_id))

    def test_change_dashboards_ordering(self):
        owner_id = uuid.uuid1()

        ordering = [uuid.uuid1() for _ in range(5)]
        dashboards.change_dashboards_ordering(owner_id, ordering)
        self.assertEqual(ordering, OwnerDashboards(owner_id, None).dashboard_id_ordering)

        dashboards.change_dashboards_ordering(owner_id, ordering[:-1])
        self.assertEqual(ordering, OwnerDashboards(owner_id, None).dashboard_id_ordering)

        dashboards.change_dashboards_ordering(owner_id, ordering[:-1], assure_no_deletions=False)
        self.assertEqual(ordering[:-1], OwnerDashboards(owner_id, None).dashboard_id_ordering)
