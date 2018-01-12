1.3
===

New features:

* support paging tags by passing the `after_tag` parameter to `Report.fetch_tags_sample`
* allow choosing if a master tile should be always the first, or should be sorted with all the tpcreated tiles (see the `put_master_first` parameter to the `layouts.repack` function, the layout mod `layouts.promote_first_as_master_mod`, and the `make_first_master` parameter to the `tpcreator.handle_tpcreator` function).
* `latest_instance_id` can be passed to `get_series_values_after` to ensure consistency of data for multiple data series.
* new config hook `get_parsing_result_desc` for computing custom meta data associated with report instances
* the `Single` tilewidget can display a specific report instance by putting its id into `fetch_params` paremeter of the `Tilewidget.get_tile_data` method.
* new methods `OwnerDashboards.get_dashboards_displaying_report` and `OwnerDashboards.get_dashboards_by_report_id` that allow retrieving dashboards displaying given reports.
* new method `Report.fetch_instances_iter` that returns an iterator instead of a list, allowing processing large numbers of large instances.

Fixes:

* delete also unused tags when deleting report instances
* improve the sorting of tpcreated tiles containg multiple numbers, e.g. IP addresses
* data series consisting of more than 10000 report instances could be only partially created
* improve the design of layout mods and allow combining mods like `tpcreator_mod` and `sscreator_mod`
* using a tpcreator and an sscreator together could cause losing sscreated dataseries in some cases
* sscreator didn't handle deleted dashboards properly

Performance improvements:

* altering layout is now much faster for large dashboards
* data series are now created without loading all report instances into memory at once
* report instances are deleted in chunks, lowering the memory usage
* processing Cassandra results is faster on PyPy thanks to using a special 'instance' dict for representing a row


1.2
===

* support for deleting reports and report instances
* made tpcreator and sscreator available as layout mods
* improved the algorithm that applies layout mods
* improved sorting of tpcreated tiles with numbers within tag names
* introduced `SeriesSpec.tweak_computed_name()` which improves the quality of generated series names
* improved JSON encoding performance


1.1.1
=====

* A hot-fix for the change introduced in 1.1

1.1
===

* Separate parsing data into tables into a new package monique-tables


1.0
===

Initial release
