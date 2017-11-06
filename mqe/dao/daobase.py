from mqe.util import undefined


class BaseDAO(object):
    """The base Data Access Object class.

    Each base DAO class inheriting from :class:`BaseDAO` specifies rows with typed columns which must be returned. The rows are represented in Python as dictionaries having the specified keys. The mapping of types is as follows:

    * uuid - type 4 :class:`~uuid.UUID`
    * timeuuid - type 1 :class:`~uuid.UUID`
    * text - :class:`str` or :class:`unicode`
    * list[str] - a :class:`list` of :class:`str` or :class:`unicode`

    Other conventions:

    * when the docs say the results must be ordered wrt. a timeuuid column, the ordering must be done wrt. the time component contained in the value
    * all ``select_*_multi`` methods should return an empty list if there are no results, other ``select_*`` methods should return ``None`` if the result doesn't exist
    * when the ``columns`` argument is ``None``, it means that all columns should be selected

    """
    pass


class ReportDAO(BaseDAO):
    """A report row has the following columns:

    * report_id timeuuid
    * report_name text
    * owner_id uuid
    """

    def select(self, report_id):
        """Select a report row"""
        raise NotImplementedError()

    def select_multi(self, owner_id, report_id_list):
        """Select a list of report rows"""
        raise NotImplementedError()

    def select_or_insert(self, owner_id, report_name):
        """Select a report row matching the parameters if it exists, insert and return
         a new report row if it doesn't exist yet. The operation must be atomic."""
        raise NotImplementedError()

    def select_by_name(self, owner_id, report_name):
        """Select a report row matching the parameters"""
        raise NotImplementedError()

    def select_ids_by_name_prefix_multi(self, owner_id, name_prefix, after_name, limit):
        """Select a list of ``report_id`` values from report rows with the ``report_name``
        having the prefix ``name_prefix``, with the ``report_name`` placed lexicographically
        after ``after_name``, limiting the number of results to ``limit``."""
        raise NotImplementedError()

    def insert(self, owner_id, report_name):
        """Insert and return a new report row"""
        raise NotImplementedError()

    def delete(self, owner_id, report_id):
        """Delete the report row"""
        raise NotImplementedError()

    def select_report_instance_diskspace(self, owner_id, report_id):
        """Select the number of disk space bytes consumed by report_instance rows associated
        with a report row with the given ``report_id``"""
        raise NotImplementedError()

    def select_report_instance_days(self, report_id, tags):
        """Select a list of :class:`datetime.date` objects which are days on which
        report_instance rows having the given tags and associated with the report row
        were created"""
        raise NotImplementedError()

    def select_tags_sample(self, report_id, tag_prefix, limit, after_tag):
        """Select a list of lists of strings - tags present in report_instance rows
        associated with the report row, having the prefix ``tag_prefix``, placed
        lexicographically after the tag ``after_tag`` (which can be an empty string), limiting
        the number of returned results to ``limit``"""
        raise NotImplementedError()




class ReportInstanceDAO(BaseDAO):
    """A report_instance row has the following columns:

    * report_id timeuuid
    * report_instance_id timeuuid
    * ri_data text
    * input_string text
    * all_tags list[str]

    An extra_ri_data row has the following columns:

    * report_id timeuuid
    * report_instance_id timeuuid
    * extra_ri_data text

    """
    def insert(self, owner_id, report_id, report_instance_id, all_tags, ri_data, input_string,
               extra_ri_data, custom_created):
        """Insert and return a report_instance row and an extra_ri_data row. The ``custom_created`` parameter is a bool telling if the datetime encoded in ``report_instance_id`` was passed by a user, disallowing assuming that the row will be the newest row"""
        raise NotImplementedError()

    def select_extra_ri_data(self, report_id, report_instance_id):
        """Select the extra_ri_data value from a extra_ri_data row"""
        raise NotImplementedError()


    def select(self, report_id, report_instance_id, tags_subset):
        """Select a report_instance row having the ``tags_subset`` as a subset of ``all_tags``"""
        raise NotImplementedError()

    def select_multi(self, report_id, tags_subset, min_report_instance_id, max_report_instance_id,
                     columns, order, limit):
        """Select a list of report_instance rows having the ``tags_subset`` as a subset of ``all_tags``, with the ``report_instance_id`` contained between ``min_report_instance_id`` and ``max_report_instance_id``, selecting only ``columns`` columns. The ``order`` is a string identifying ordering wrt. ``report_instance_id`` - either ``asc`` or ``desc``."""
        raise NotImplementedError()


    def select_latest_id(self, report_id, tags_subset):
        """Select the newest ``report_instance_id`` of a report_instance row having the ``tags_subset`` as a subset of ``all_tags``"""
        raise NotImplementedError()

    def delete_multi(self, owner_id, report_id, tags, min_report_instance_id, max_report_instance_id,
                     limit, update_counters, use_insertion_datetime):
        """Delete report_instance rows with the ``report_instance_id`` contained between ``min_report_instance_id`` and ``max_report_instance_id``, which have the ``tags_subset``
        as a subset of ``all_tags``.

        ``update_counters`` is a flag telling if report instance and diskspace counters should be updated.

        ``use_insertion_datetime`` is a flag telling if only rows which were inserted
        (possibly with a custom creation datetime) in the time range specified by the
        ``min_report_instance_id``, ``max_report_instance_id`` arguments should be
        deleted.

        The method must return a two-element tuple containing the number of the deleted rows and
        a list of tags subsets present in the deleted rows."""
        raise NotImplementedError()

    def delete(self, owner_id, report_id, report_instance_id, update_counters):
        """Delete a single report_instance row. The result must be the same as for the
        :meth:`delete_multi` method."""

    def select_report_instance_count_for_owner(self, owner_id):
        """Select the count of report_instance rows created for the ``owner_id``"""
        raise NotImplementedError()

    def select_report_instance_diskspace_for_owner(self, owner_id):
        """Select the number of disk space bytes consumed by report_instance rows created for the ``owner_id``"""
        raise NotImplementedError()



class TileDAO(BaseDAO):
    """A tile row has the following columns:

    * dashboard_id uuid
    * tile_id timeuuid
    * tile_options text
    """

    def select_multi(self, dashboard_id, tile_id_list):
        """Select a list of tile rows"""
        raise NotImplementedError()

    def insert_multi(self, owner_id, dashboard_id, tile_options_list):
        """Insert and return tile rows"""
        raise NotImplementedError()

    def delete_multi(self, tile_list):
        """Delete tile rows having the ``dashboard_id`` and ``tile_id`` columns
         equal to the attributes of |Tile| objects from the ``tile_list``"""
        raise NotImplementedError()



class DashboardDAO(BaseDAO):
    """A dashboard row has the following columns:

    * owner_id uuid
    * dashboard_id uuid
    * dashboard_name text
    * dashboard_options text

    """

    def select(self, owner_id, dashboard_id):
        """Select a dashboard row matching the parameters"""
        raise NotImplementedError()

    def select_all(self, owner_id):
        """Select a list of dashboard rows belonging to the owner"""
        raise NotImplementedError()

    def insert(self, owner_id, dashboard_name, dashboard_options):
        """Insert and return a dashboard row having the parameters"""
        raise NotImplementedError()

    def update(self, owner_id, dashboard_id, dashboard_name, dashboard_options):
        """Update a dashboard row identified by ``owner_id`` and ``dashboard_id``"""
        raise NotImplementedError()

    def delete(self, owner_id, dashboard_id):
        """Delete the dashboard row"""
        raise NotImplementedError()

    def select_tile_ids(self, dashboard_id):
        """Select a list of tile IDs assigned to the ``dashboard_id``"""
        raise NotImplementedError()

    def select_all_dashboards_ordering(self, owner_id):
        """Select a list of uuids forming the ordering of all dashboard_ids"""
        raise NotImplementedError()

    def set_all_dashboards_ordering(self, owner_id, dashboard_id_list):
        """Set the list of uuids ``dashboard_id_list`` as the ordering of all dashboards"""
        raise NotImplementedError()



class LayoutDAO(BaseDAO):
    """A layout row has the following columns:

    * owner_id uuid
    * dashboard_id uuid
    * layout_def text
    * layout_props text
    * layout_id timeuuid
    
    A layout_by_report row has the following columns:
    
    * owner_id uuid
    * report_id timeuuid
    * label text
    * tags list[str]
    * dashboard_id uuid
    * layout_id timeuuid
    
    """

    def select(self, owner_id, dashboard_id,
               columns=('layout_id', 'layout_def', 'layout_props')):
        """Select a layout row, possibly limited to the ``columns`` columns"""
        raise NotImplementedError()

    def select_multi(self, owner_id, dashboard_id,
               columns=('layout_id', 'layout_def', 'layout_props')):
        """Select a list of layout rows, possibly limited to the ``columns`` columns.
        The returned rows must include the ``dashboard_id`` column."""
        raise NotImplementedError()

    def set(self, owner_id, dashboard_id, old_layout_id, new_layout_id,
            new_layout_def, new_layout_props):
        """Set a new layout row ``{ 'layout_def': new_layout_def, 'layout_props': new_layout_props, 'layout_id': new_layout_id}`` for the ``owner_id`` and ``dashboard_id`` parameters if the current value of ``layout_id`` is equal to ``old_layout_id``. Return a bool telling if the operation was successful."""
        raise NotImplementedError()

    def delete(self, owner_id, dashboard_id):
        """Delete the layout row"""
        raise NotImplementedError()

    def insert_layout_by_report_multi(self, owner_id, report_id_list, tags, label, dashboard_id,
                                      layout_id):
        """Insert and return a set of layout_by_report rows formed by expanding ``report_id_list``
         into individual ``report_id`` values. The rows should overwrite existing rows
         matching the parameters ``owner_id``, ``report_id``, ``tags``, ``label``,
         ``dashboard_id``."""
        raise NotImplementedError()

    def delete_layout_by_report(self, owner_id, report_id, tags, label, dashboard_id,
                                layout_id):
        """Delete the layout_by_report row"""
        raise NotImplementedError()

    def select_layout_by_report_multi(self, owner_id, report_id, tags, label, limit):
        """Select layout_by_report rows matching the parameters"""
        raise NotImplementedError()


class SeriesDefDAO(BaseDAO):
    """A series_def row has the following columns:

    * report_id timeuuid
    * tags list[str]
    * series_id timeuuid
    * series_spec text
    * from_rid timeuuid
    * to_rid timeuuid
    """

    def select_multi(self, report_id, tags_series_id_list):
        """Select a list of series_def rows. The ``tags_series_id_list`` is a list
        of tuples ``(tags, series_id)``"""
        raise NotImplementedError()

    def select_id_or_insert_multi(self, report_id, tags_series_spec_list):
        """Select a list of ``series_id`` values from series_def rows matching
        the parameters from the ``tags_series_spec_list``, which is a list of tuples
        ``(tags, series_spec)``. Insert new series_def rows if the rows don't exist."""
        raise NotImplementedError()

    def insert_multi(self, report_id, tags_series_spec_list):
        """Insert and return series_def rows"""
        raise NotImplementedError()

    def update_from_rid_to_rid(self, report_id, series_id, tags, from_rid=undefined,
                               to_rid=undefined):
        """Update the series_def row's ``from_rid`` and ``to_rid`` values, unless
        they are :attr:`mqe.util.undefined` objects."""
        raise NotImplementedError()

    def clear_all_series_defs(self, report_id, tags_powerset):
        """Set the ``from_rid``, ``to_rid`` values to NULL/undefined for all series_def
        rows having the ``tags`` contained in ``tags_powerset``."""
        raise NotImplementedError()



class SeriesValueDAO(BaseDAO):
    """A series_value row has the following columns:

    * series_id timeuuid
    * report_instance_id timeuuid
    * json_value text
    * header text

    """
    def insert_multi(self, series_id, data):
        """Insert series_value rows. ``data`` is a list of dictionaries having the keys: ``report_instance_id``, ``json_value``, ``header``. The existing rows with matching
        ``series_id``, ``report_instance_id`` values should be replaced."""
        raise NotImplementedError()

    def select_multi(self, series_id, min_report_instance_id, max_report_instance_id, limit):
        """Select a list of series_value rows, having the ``report_instance_id`` value
        contained between ``min_report_instance_id`` and ``max_report_instance_id``, limiting
        the number of results to ``limit``. The result must be sorted descending wrt.
        ``report_instance_id``."""
        raise NotImplementedError()



class OptionsDAO(BaseDAO):
    """An options row has the following columns:

    * report_id timeuuid
    * kind text
    * options_key text
    * options_value text

    """

    def select_multi(self, report_id, kind, key_list):
        """Select a list of options rows"""
        raise NotImplementedError()

    def set_multi(self, report_id, kind, key_value_list):
        """Set the options rows, possibly overwriting existing rows. The ``key_value_list`` is a list of tuples ``(options_key, options_value)``."""
        raise NotImplementedError()




