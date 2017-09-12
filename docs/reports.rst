Reports and report instances
============================

An example of creating a report and report instances has been shown in the :ref:`tutorial <tutorial_report>`. The example could be all you need - data from report instances can be accessed by fetching :data:`tile_data` containing extracted values in the :data:`~tile_data.series_data` dictionaries. For more complex use cases the library offers direct access to report instances, customizing their creation and attaching tags.


Tags
----

Tags are custom string labels attached to report instances. For example, when sending CPU usage metrics, we could attach a tag identifying a source IP address and a ``warning`` tag::


    cpu_report = Report.select_or_insert(owner_id, 'cpu_usage')
    metrics = [
        ('user', 92.3),
        ('system', 3.4),
        ('io', 4.4),
    ]
    cpu_report.process_input(json.dumps(metrics), tags=['ip:192.168.1.18',
                                                        'warning'])

A tile can be configured to display data from report instances having a given **tags subset** by using the :data:`tile_config.tags` attribute:

.. code-block:: python
    :emphasize-lines: 2,10

    tile_config_1 = {
        'tags': ['ip:192.168.1.18'],
        'series_spec_list': [
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
        ],
    }
    tile_1 = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id, tile_config_1)

    tile_config_2 = {
        'tags': ['ip:192.168.1.18', 'warning'],
        'series_spec_list': [
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
        ],
    }
    tile_2 = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id, tile_config_2)

``tile_1`` will get data from the ``cpu_report`` from report instances having the tag ``ip:192.168.1.18`` attached - regardless if other tags present - and ``tile_2`` will get data from the report instances having both tags attached.

While the described usage of tags for filtering report instances is useful in itself, the main feature of tags is the ability to auto-create tiles having similar tags, described in the chapter :ref:`guide_tpcreator`.


process_input arguments
-----------------------

The :meth:`.process_input` method that creates a report instance from a string input takes extra arguments.

A creation :class:`~datetime.datetime` can be set explicitly with the ``created`` parameter. Note that the database model isn't optimized for submitting many report instances with a past datetime, and the parameter cannot be a future datetime.

Setting an explicit :func:`input type <mqe.pars.parseany.parse_input>` saves time needed to detect an input format, and assures the parsing is correct.

An explicit field delimiter can be passed explicitly as ``ip_options={'delimiter': delimiter}``.

The indexes of table rows forming a header can be specified with ``force_header``, saving time needed to detect them automatically and ensuring correctness.

The ``extra_ri_data`` parameter allows attaching a custom JSON object to a report instance. The object can be retrieved with a call to :meth:`.fetch_extra_ri_data`.


Fetching report instances
-------------------------

The :class:`.Report` class has several methods for fetching report instances belonging to a report.

The main method is :meth:`.fetch_instances`, which fetches a list of :class:`.ReportInstance` objects for a given time range, possibly containing a given tags subset. The time range can be specified in two ways - either as :class:`~datetime.datetime` objects (``from_dt``, ``to_dt``) or the minimal and the maximal |rid| values, which are UUIDs (``after``, ``before``). Whenever the documentation mentions ordering of UUID values, the meant ordering is wrt. a time component encoded in an UUID1. A |rid| is an UUID1 with a report instance creation time encoded in the value.

Generally, working directly with |rid| values is recommended. That way the selection is based on exact values, while datetimes are prone to losing their precision while transmitting data between different components (e.g. Javascript -> Python). Additionally, the library supports creating multiple report instances with the same creation datetime, voiding the assumption that a datetime uniquely identifies a report instance.

Fetching reports
----------------

The function :func:`.fetch_reports_by_name` fetches reports sorted wrt. a name of a report and allows filtering the names based on a prefix. The ``after_name`` and ``limit`` parameters allow implementing paging of reports.

Fetching tags
-------------

Tags assigned to report instances, belonging to a given report, can be fetched with the :meth:`.fetch_tags_sample` method. The method allows filtering the tag names based on a prefix.
