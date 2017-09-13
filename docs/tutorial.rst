.. _tutorial:

Tutorial
========

Basic concepts
--------------

Monique Dashboards drop the tradition of using raw metrics and represent dashboard data using **tables**. A table is a richer data structure - it usually contains labels for numeric values and a header. Multiple input formats, like SQL results or JSON documents, can be automatically converted to a tabular representation, while using metrics requires parsing the data manually and setting labels explicitly. Traditional metrics are still supported - by using single-cell tables.

A table created at a specific time forms a **report instance** and multiple report instances are grouped under a named **report**. For example, results of a single SQL query can form a single report instance and the results of executing the query every hour could be grouped under the report ``sql_results``.

Monique Dashboards don't define a model of a user and partition the data by **owner_id** - an explicitly passed UUID, usually identifying an account from an external system. UUIDs are widely used - whenever you see a name ending with ``id``, you can be sure it's a UUID.


.. _tutorial_report:

Creating a report and report instances
--------------------------------------
Creating a report is achieved by calling the :meth:`~mqe.reports.Report.insert` or the :meth:`~mqe.reports.Report.select_or_insert` method of the :class:`~mqe.reports.Report` class. After doing it, report instances can be created from a string input by calling :meth:`~mqe.reports.Report.process_input`. The method tries to automatically recognize an input format::

    import uuid
    from mqe.reports import Report

    owner_id = uuid.uuid4()

    simple_report = Report.insert(owner_id, 'simple')
    res = simple_report.process_input('10 20')
    print(res.report_instance.table)

    > Table(header_idxs=[], rows=[
    >    ['10', '20'],
    > ])

We can see that the input ``10 20`` has been parsed into a one-row, two-cell table containing the numbers. The :attr:`~mqetables.parsing.Table.header_idxs` attribute tells which row indexes form a header - in our case there is no header.

.. _tutorial_points_report:

We can try to create a more complex report instance::

    points_report = Report.insert(owner_id, 'points')
    input = """\
    user_name is_active points
    john      true      128
    monique   true      210
    """
    res = points_report.process_input(input)
    print(res.report_instance.table)

    > Table(header_idxs=[0], rows=[
    >     ['user_name', 'is_active', 'points'],
    >     ['john', 'true', '128'],
    >     ['monique', 'true', '210'],
    > ])

The whitespace-aligned table has been correctly parsed. The library recognizes multiple input formats, like JSON, ASCII tables or CSV files. A format can be also explicitly specified as the ``input_type`` parameter in the :meth:`~mqe.reports.Report.process_input` method.


.. _tutorial_dashboard:

Creating a dashboard and a tile
-------------------------------
To create a :class:`~mqe.dashboards.Dashboard` we use the :class:`~mqe.dashboards.OwnerDashboard` class which manages dashboards of a single owner::

    from mqe.dashboards import OwnerDashboards

    owner_dashboards = OwnerDashboards(owner_id)
    dashboard = owner_dashboards.insert_dashboard('My Dashboard')

A dashboard without tiles is not very useful. Tiles are specified using :data:`tile_config` - a dictionary/JSON object. To create a tile displaying users' points, we can define the following::

    from mqe.dataseries import SeriesSpec

    tile_config = {
        'tw_type': 'Range',
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'seconds_back': 86400,
            'tile_title': 'Points by user',
        }
    }

There are quite a few new things here. The :data:`~tile_options.tw_type` defines a type of a **tilewidget** - an object responsible for fetching the data to display. In our case the ``Range`` means that we want to display data from a time range of report instances.

The :class:`~mqe.dataseries.SeriesSpec` class defines a data series - a sequence of values extracted from a range of report instances. The first argument tells from which column we want to extract a value. The second specifies a *filtering column* - a column containing values used to find the wanted row. The third - a predicate applied to the filtering column. The first :class:`~mqe.dataseries.SeriesSpec` could be represented using the following pseudo-SQL::

    SELECT 2 FROM table WHERE 0 = 'john'

We could also cheat a bit and use the :func:`~mqe.dataseries.guess_series_spec` function that tries to guess a :class:`~mqe.dataseries.SeriesSpec` based on a cell we want to graph.

The :data:`tile_options` contains detailed options - in our case we tell that we want our tile to display data for the last day, and define a title.

Finally, we can create a tile displaying data from the ``points`` report::

    from mqe.tiles import Tile

    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)

.. _tutorial_tile_data:

We can already fetch data formatted for rendering a chart::

    print(tile.get_tile_data())

    > {'combined_colors': ['#4E99B2', '#8ED2AB'],
    >  'common_header': 'points',
    >  'fetched_from_dt': datetime.datetime(2017, 9, 2, 19, 55, 48, 806725),
    >  'fetched_to_dt': datetime.datetime(2017, 9, 3, 19, 55, 48, 806725),
    >  'generated_tile_title': 'points (john, monique)',
    >  'report_name': 'points',
    >  'series_data': [{'common_header': 'points',
    >                   'data_points': [DataPoint(rid=UUID('e1b988b2-90e1-11e7-bd69-bc5ff4d0b01f'), dt=datetime.datetime(2017, 9, 3, 19, 55, 48, 716357), value=128)],
    >                   'name': 'john',
    >                   'series_id': UUID('e1c04224-90e1-11e7-bd69-bc5ff4d0b01f')},
    >                  {'common_header': 'points',
    >                   'data_points': [DataPoint(rid=UUID('e1b988b2-90e1-11e7-bd69-bc5ff4d0b01f'), dt=datetime.datetime(2017, 9, 3, 19, 55, 48, 716357), value=210)],
    >                   'name': 'monique',
    >                   'series_id': UUID('e1c08888-90e1-11e7-bd69-bc5ff4d0b01f')}]}

We can see quite a few values helping with rendering a chart, like suggested colors or a ``common_header`` that could be set as a Y-axis title. The most important is ``series_data``, which contains data for each :class:`~mqe.dataseries.SeriesSpec` from :data:`tile_config`. The ``data_points`` key holds a list of values to graph, together with their creation datetimes and source report instance IDs.

The full description of the data returned by :meth:`~mqe.tiles.Tile.get_tile_data` can be found in the :data:`tile_data` documentation.


Placing a tile in a dashboard layout
------------------------------------

The tile is already created, but we must put it into a dashboard layout to assign a position and a size::

    from mqe.layouts import place_tile

    res = place_tile(tile)
    if not res:
        raise ValueError('Placing the tile unsuccessful')

The layouts are being defined for a grid of default width of 12 (the value can be changed in the configuration module) and are packed upwards, disallowing vertical space to exist between tiles.

The :func:`~mqe.layouts.place_tile` function searches for the first available area the tile will fit the tile and signals if the operation was successful. The operation could fail if multiple concurrent processes would try to update the layout. While there is a small chance it could happen in our example, it's a good idea to take advantage of the library's support for atomic layout updates.

To render a dashboard, we must fetch the full :class:`~mqe.layouts.Layout`::

    from mqe.layouts import Layout

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    print(layout.layout_dict)

    > {UUID('f4eaaab0-9145-11e7-a99d-bc5ff4d0b01f'): {'height': 4,
    >                                                 'width': 4,
    >                                                 'x': 0,
    >                                                 'y': 0}}

The :attr:`~mqe.layouts.Layout.layout_dict` defines the layout - it's a dictionary mapping a :attr:`~mqe.tiles.Tile.tile_id` to its :data:`visual_options` - the definition of a position and a size. The :attr:`~mqe.layouts.Layout.tile_dict` attribute represents the |layout_dict| as a dictionary mapping full |Tile| objects to :data:`visual_options`.

In the end, we can write a function rendering a dashboard::

    def render_dashboard(owner_id, dashboard):
        print('Rendering dashboard %r' % dashboard.dashboard_name)
        layout = Layout.select(owner_id, dashboard.dashboard_id)
        for tile, visual_options in layout.tile_dict.items():
            tile_data = tile.get_tile_data()
            print('Rendering tile %r at position %s/%s' % (
                tile_data['generated_tile_title'], visual_options['x'], visual_options['y']))
            # render tile_data['series_data']


Next steps
----------

The tutorial gives an overview of the library's API. Further chapters describe other important features:

* :ref:`auto-creation of tiles <guide_tpcreator>` by copying a *master tile*
* :ref:`auto-creation of data series <guide_sscreator>` contained in a tile
* :ref:`customizing data used for rendering a tile <guide_tilewidgets>`
* :ref:`managing layouts <guide_layouts>`

Source code presented in the tutorial and other chapters is available in the `examples directory of the Github repository <https://github.com/monique-dashboards/monique/examples>`_.

