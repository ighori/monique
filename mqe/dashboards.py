import logging
from collections import defaultdict

from mqe import c
from mqe import serialize
from mqe import tiles
from mqe.dbutil import Row, TextColumn, UUIDColumn, JsonColumn
from mqe.signals import fire_signal, new_dashboard


log = logging.getLogger('mqe.dashboards')


class Dashboard(Row):
    """A representation of a dashboard - a container for tiles, with an associated name and
    custom options.

    Note that all dashboards belonging to an owner can be managed using the
    :class:`OwnerDashboards` class.

    """

    #: the owner of the dashboard
    owner_id = UUIDColumn('owner_id')

    #: the ID of the dashboard
    dashboard_id = UUIDColumn('dashboard_id')

    #: the name of the dashboard
    dashboard_name = TextColumn('dashboard_name')

    #: dashboard options - a custom JSON object associated with the dashboard
    dashboard_options = JsonColumn('dashboard_options', default=lambda: {}) # type: dict

    @staticmethod
    def select(owner_id, dashboard_id):
        """Selects the given dashboard as a :class:`Dashboard` object (returns ``None``
        if the dashboard doesn't exist)"""
        row = c.dao.DashboardDAO.select(owner_id, dashboard_id)
        return Dashboard(row) if row else None

    def update(self, dashboard_name=None, dashboard_options=None):
        """Update the dashboard's name (if non-``None``) and/or options (if non-``None``
        and passed as a JSON-serializable document)"""
        if dashboard_options is not None:
            dashboard_options = serialize.mjson(dashboard_options)
        c.dao.DashboardDAO.update(self.owner_id, self.dashboard_id, dashboard_name, dashboard_options)

    def delete(self):
        """Delete the given dashboard. The function also updates the ordering of all dashboards."""
        tile_ids = c.dao.DashboardDAO.select_tile_ids(self.dashboard_id)
        tile_by_id = tiles.Tile.select_multi(self.dashboard_id, tile_ids)
        tiles.Tile.delete_multi(tile_by_id.values())

        c.dao.LayoutDAO.delete(self.owner_id, self.dashboard_id)

        dashboard_id_ordering = c.dao.DashboardDAO.select_all_dashboards_ordering(self.owner_id)
        if dashboard_id_ordering and self.dashboard_id in dashboard_id_ordering:
            if self.dashboard_id in dashboard_id_ordering:
                dashboard_id_ordering.remove(self.dashboard_id)
                c.dao.DashboardDAO.set_all_dashboards_ordering(self.owner_id, dashboard_id_ordering)

        c.dao.DashboardDAO.delete(self.owner_id, self.dashboard_id)

        log.info('Deleted dashboard with %s tiles dashboard_id=%s dashboard_name=%r',
                 len(tile_by_id), self.dashboard_id, self.dashboard_name)

    def key(self):
        return (self.owner_id, self.dashboard_id)



def _select_tile_ids(dashboard_id):
    """Select tile IDs associated with the dashboard. Note that the function shouldn't be
    normally used by the library users, as tile IDs should be retrieved from a
    layout."""
    return c.dao.DashboardDAO.select_tile_ids(dashboard_id)

def change_dashboards_ordering(owner_id, dashboard_id_list, assure_no_deletions=True):
    """Change the ordering of all dashboards of a given user to match ``dashboard_id_list`` (a list
    of all dashboard IDs of the user). If ``assure_no_deletions`` is ``True``, the function
    makes sure that all dashboards IDs from the original ordering are present in the new
    ordering."""

    if assure_no_deletions:
        current_ordering = c.dao.DashboardDAO.select_all_dashboards_ordering(owner_id)
        if current_ordering:
            for dashboard_id in current_ordering:
                if dashboard_id not in dashboard_id_list:
                    dashboard_id_list.append(dashboard_id)

    c.dao.DashboardDAO.set_all_dashboards_ordering(owner_id, dashboard_id_list)



class OwnerDashboards(object):
    """A class representing all dashboards of a given owner.

    :param owner_id: the owner ID which dashboards must be loaded
    :param insert_if_no_dashboards: if it's a non-empty string and the user doesn't have any
        dashboards, insert a dashboard having the name
    """

    def __init__(self, owner_id, insert_if_no_dashboards='Default Dashboard'):
        #: the owner ID whose dashboards must be loaded
        self.owner_id = owner_id
        #: a list of :class:`Dashboard` objects, sorted by the ordering
        self.dashboards = None
        #: a dict mapping a dashboard ID to the :class:`Dashboard` object
        self.dashboard_by_id = None
        #: a list of dashboard IDs defining the ordering of all dashboards
        self.dashboard_id_ordering = None
        #: a dict mapping a dashboard ID to an index on the :attr:`dashboard_id_ordering` list
        self.dashboard_ordering_by_id = {}

        self.reload()
        if not self.dashboards and insert_if_no_dashboards:
            self.insert_dashboard(insert_if_no_dashboards, {})
            self.reload()


    def reload(self):
        self.dashboards = [Dashboard(row) for row in c.dao.DashboardDAO.select_all(self.owner_id)]
        self.dashboard_id_ordering = c.dao.DashboardDAO.select_all_dashboards_ordering(
            self.owner_id) or []

        self.dashboard_by_id = {db.dashboard_id: db for db in self.dashboards}
        self.dashboard_ordering_by_id = {dashboard_id: i for i, dashboard_id in enumerate(self.dashboard_id_ordering)}
        self.dashboards.sort(key=lambda db: self.dashboard_ordering_by_id.get(db.dashboard_id, 1000000))


    def insert_dashboard(self, dashboard_name, dashboard_options={}):
        """Insert a dashboard having the passed ``dashboard_name`` and ``dashboard_options`` (a JSON-serializable value).
        """
        from mqe import layouts

        row = c.dao.DashboardDAO.insert(self.owner_id, dashboard_name,
                                     serialize.mjson(dashboard_options))
        if not row:
            return None
        dashboard = Dashboard(row)

        change_dashboards_ordering(self.owner_id,
                                   self.dashboard_id_ordering + [dashboard.dashboard_id])

        empty_layout = layouts.Layout()
        empty_layout.set(self.owner_id, row['dashboard_id'], None)

        log.info('Inserted new dashboard dashboard_id=%s name=%r',
                 dashboard.dashboard_id, dashboard.dashboard_name)

        self.reload()

        fire_signal(new_dashboard, dashboard=dashboard)

        return Dashboard(row)


    def get_dashboards_displaying_report(self, report_id):
        """Returns a list of :class:`Dashboard` objects that contain a tile
        displaying the given report.
        """
        return self.get_dashboards_by_report_id().get(report_id, [])

    def get_dashboards_by_report_id(self):
        """Returns a dict mapping a report ID to a list of :class:`Dashboard` objects
        that contain a tile displaying the report having the ID.
        """
        from mqe import layouts

        res = defaultdict(list)
        res_sets = defaultdict(set)
        layout_list = layouts.Layout.select_multi(self.owner_id,
                                                  [d.dashboard_id for d in self.dashboards])
        for layout in layout_list:
            if not layout:
                continue
            for tile_id, props in layout.layout_props['by_tile_id'].items():
                report_id = props.get('report_id')
                dashboard = self.dashboard_by_id.get(layout.dashboard_id)
                if report_id and dashboard and dashboard not in res_sets[report_id]:
                    res[report_id].append(dashboard)
                    res_sets[report_id].add(dashboard)
        return res
