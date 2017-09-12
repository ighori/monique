.. _guide_tilewidgets:

Rendering and configuring tiles
===============================

We could already see an example of specifying a |Tile| using :data:`tile_config` in the :ref:`tutorial <tutorial_dashboard>`. The library allows configuring a tile for rendering a chart or a text table, provides custom extension points and manages colors.


.. _guide_tile_config_and_tile_options:

Tile_config and tile_options
----------------------------

The library uses :data:`tile_options` - a dictionary that can be serialized to/from JSON - as a full description of a |Tile| - including its ``owner_id``, source |Report|, visualization type and data series definitions. If you look at the documentation, you will notice it requires quite a lot of parameters to be filled. It's why a simplified way of specifying a |Tile| is provided - using :data:`tile_config`, which requires less parameters, but still allows a subset of :data:`tile_options` to be included. A :data:`tile_config` is converted to full :data:`tile_options` when a |Tile| is inserted into the database.

The methods :meth:`~mqe.tiles.Tile.insert` and :meth:`~mqe.tiles.Tile.insert_similar` take a :data:`tile_config` as an argument::


    tile_config = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'tile_title': 'Points by user',
        }
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)
    print 'Full tile options:', tile.tile_options

    > Full tile options:
    >{u'drawer_type': u'ChartRangeDrawer',
    > u'owner_id': UUID('c81b2aa6-bdbe-48a4-999e-6ba60bcd18c8'),
    > u'report_id': UUID('ba89a310-92ff-11e7-9ac6-bc5ff4d0b01f'),
    > u'seconds_back': 604800,
    > u'series_configs': [{u'series_id': UUID('ba9248c6-92ff-11e7-9ac6-bc5ff4d0b01f'),
    >                      u'series_spec': <select 2 where 0 = john as john>},
    >                     {u'series_id': UUID('ba92943e-92ff-11e7-9ac6-bc5ff4d0b01f'),
    >                      u'series_spec': <select 2 where 0 = monique as monique>}],
    > u'tags': [],
    > u'tile_title': u'Points by user',
    > u'tw_type': u'Range'}

We can see that the printed :data:`tile_options` include attributes which were not explicitly specified in the :data:`tile_config`.

A |Tile| can be created using full :data:`tile_options` with the method :meth:`.insert_with_tile_options`::

    tile_options2 = tile.tile_options.copy()
    tile_options2['owner_id'] = uuid.uuid4()
    tile2 = Tile.insert_with_tile_options(dashboard.dashboard_id, tile_options2)

The example demonstrates a potential security risk - when the :data:`tile_options` are coming from an external source, like a web browser, they could be altered to point to another owner's data. That's another reason to use a :data:`tile_config` - the ``owner_id`` and ``report_id`` parameters are explicitly specified as the arguments to the :meth:`~mqe.tiles.Tile.insert` method, and if the parameters are present in the :data:`tile_config.tile_options`, they are ignored.


Updating tile's config
----------------------

Since tiles :ref:`are immutable <guide_layouts>`, updating tile's options involves creating a new tile and using it as a replacement for an old tile in a layout definition. The pattern is to retrieve an existing :data:`tile_config` using :meth:`.get_tile_config`, modify it, and create a new tile using the modified config:

.. code-block:: python
    :emphasize-lines: 6,7,8

    from mqe.layouts import Layout, replace_tiles

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    tile = layout.tile_dict.keys()[0]

    tile_config = tile.get_tile_config()
    tile_config['tile_options']['seconds_back'] = 3600
    repl_tile = tile.insert_similar(tile_config)

    replace_tiles({tile: repl_tile}, for_layout_id=layout.layout_id)

It's often easier to always create full :data:`tile_config` - for example, a web application could always send the full config describing a tile it want to update. That way the :meth:`.get_tile_config` doesn't need to be called and the differences between the versions of a tile don't need to be computed.


Formatting tile data - tilewidgets and drawers
----------------------------------------------

The method :meth:`~mqe.tiles.Tile.get_tile_data` of a |Tile| returns :data:`tile_data` - a dictionary containing data for rending a chart or a table (as shown in the :ref:`tutorial <tutorial_tile_data>`). What attributes the dictionary will contain is controlled by two settings - a higher-level setting of a **tilewidget type** - the :data:`tile_config.tw_type` attribute, and a lower-level setting of a **drawer type** - the :data:`tile_options.drawer_type` attribute.

A tilewidget type controls what range of report instances will be selected to compute data series points (the :data:`tile_data.series_data` attribute). When :data:`~tile_options.tw_type` is set to the string ``'Single'``, only the newest report instance will be selected (it's meant for tiles displaying only the current values of a report). When it is set to ``'Range'``, a range of report instances will be selected - up to the age specified in seconds as :data:`tile_options.seconds_back`.

A drawer type controls further postprocessing and setting additional attributes. Available drawers are listed in the :data:`tile_options.drawer_type` documentation. The drawers with a name starting with ``Chart`` are meant for rendering a chart and ensure the series data contains numbers only. The ``Text*`` drawers allow non-numeric series data, and additionally assure the returned colors (under the :data:`tile_data.combined_colors` attribute) are readable on a white background.

A sample :data:`tile_config` specifying a drawer could look like this::

    tile_config = {
        'tw_type': 'Range',
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'seconds_back': 3600,
            'tile_title': 'Points by user',
            'drawer_type': 'ChartRangeDrawer',
            'colors': ['red', 'blue'],
        }
    }


Updating tile_data
------------------

When a tile is already rendered and a new report instance is created, there's a need to fetch a part of :data:`tile_data` containing the new data. The method :meth:`~mqe.tiles.Tile.get_new_tile_data` does it - it returns partial :data:`tile_data` containing data coming from report instances created after the specified report instance ID. The ID should be equal to the last ID present in the full :data:`tile_data`. Sample code looks like this::

    # the full tile_data
    tile_data = tile.get_tile_data()

    # in the meantime, a new report instance is created
    points_report.process_input(input)

    # get the latest report_instance_id from the first series data
    last_report_instance_id = tile_data['series_data'][0]['data_points'][-1].rid

    # fetch the data coming after last_report_instance_id
    new_tile_data = tile.get_new_tile_data(last_report_instance_id)

    # the new_tile_data could be merged back into full tile_data


.. _guide_colors:

Managing colors
---------------

Colors assigned to each data series are returned as :data:`tile_data.combined_colors`. The attribute is computed from three sources.

The first are colors set explicitly as :data:`tile_options.colors`. The i-th color is being assigned to the i-th data series.

The second are colors coming from **default options** - options previously assigned to the specific data series, possibly for a different |Tile|.

The third are default colors defined in the config module as :attr:`mqe.mqeconfig.DEFAULT_COLORS`.

The first and the third source doesn't require further explanation, but how default options work? After a tile is created, we can tell that we want its :data:`tile_options` to be included in a pool of default options available for other tiles by calling :func:`.update_default_options`. After doing it and creating another tile that uses the same |SeriesSpec|, the assigned series' color will come from the default options (unless it's overriden by :data:`tile_options.colors`). The sample code shows the behaviour::

    from mqe.dataseries import update_default_options

    tile_config = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'colors': ['blue', 'red'],
        }
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)
    print tile.get_tile_data()['combined_colors']

    > ['blue', 'red']

    update_default_options(tile)

    tile_config_2 = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
    }
    tile_2 = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config_2)
    print tile_2.get_tile_data()['combined_colors']

    > ['red']

We can see that although ``tile_config_2`` doesn't specify any colors, the :data:`tile_data.combined_colors` contains a color set in ``tile_config``.


Data series names
-----------------

A |SeriesSpec| object has a name that is included in :data:`tile_data.series_data`. A name can be set explicitly by calling :meth:`~mqe.dataseries.SeriesSpec.set_name`. When the explicit name is not set, the :meth:`~mqe.dataseries.SeriesSpec.name` is computed automatically, based on its filtering expression. The sample code sets an explicit series' name::

    series_spec = SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']})
    series_spec.set_name("monique's points")
    tile_config = {
       'series_spec_list': [series_spec]
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)
    print tile.get_tile_data()['series_data'][0]['name']

    > monique's points


Creating custom tilewidgets and drawers
---------------------------------------

Custom tilewidgets and drawers can be registered by calling :func:`.register_tilewidget_class` and :func:`.register_drawer_class`. While creating a custom :class:`.Tilewidget` is a bit more complex, creating a custom :class:`.Drawer` is simple - it's just a custom postprocessor of :data:`tile_data`.

If we want out drawer to be called for both :meth:`~mqe.tiles.Tile.get_tile_data` and :meth:`~mqe.tiles.Tile.get_new_tile_data`, it's sufficient to implement the :meth:`.process_tile_data` method receiving :data:`tile_data` that we can modify. For example, here's a drawer that adds ``max_number`` attribute to :data:`tile_data` that is the maximal number from all data series::

    from mqe.tilewidgets import register_drawer_class, Drawer

    @register_drawer_class
    class MaxNumberDrawer(Drawer):

        drawer_type = 'MaxNumberDrawer'

        def process_tile_data(self, tile_data):
            max_number = 0
            for series_data in tile_data['series_data']:
                for point in series_data['data_points']:
                    if int(point.value) > max_number:
                        max_number = int(point.value)

            tile_data['max_number'] = max_number

The :func:`.register_drawer_class` function is used as a class decorator, and the :attr:`.Drawer.drawer_type` attribute defines a name that can be used in :data:`tile_config`. After computing the number inside :meth:`.process_tile_data`, we simply assign it to the :data:`tile_data`.

We can check if the new drawer is working::

    tile_config = {
        'series_spec_list': [
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
            SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
        ],
        'tile_options': {
            'drawer_type': 'MaxNumberDrawer',
        }
    }
    tile = Tile.insert(owner_id, points_report.report_id, dashboard.dashboard_id, tile_config)

    print tile.get_tile_data()['max_number']

    > 241

