Reference
=========

Dictionaries / JSON objects
---------------------------

Dictionaries described here can be converted to/from JSON using the provided :ref:`guide_serialization`.


tile_options
^^^^^^^^^^^^

.. data:: tile_options

Configuration of a dashboard tile that describes how to fetch the tile's data and how to format it. The configuration is usually created from :data:`tile_config`.

To create a tile using the specified :data:`tile_options`, use the
:meth:`~mqe.tiles.Tile.insert_with_tile_options` method.

Applications can store JSON-serializable values under keys not described here for their own purposes. In the example below, the key ``chart_type`` is application-specific.


Sample value::

  {
     'tw_type': 'Range',
     'drawer_type': 'ChartRangeDrawer',
     'report_id': UUID('0dbb0fd6-7458-11e7-9281-bc5ff4d0b01f'),
     'series_configs': [
        {
          'series_id': UUID('1584e976-7458-11e7-b2c8-bc5ff4d0b01f'),
          'series_spec': SeriesSpec(0, -1, {'op': 'eq', 'args': ['0']})
        },
        {
           'series_id': UUID('15886240-7458-11e7-b2c8-bc5ff4d0b01f'),
           'series_spec': SeriesSpec(1, -1, {'op': 'eq', 'args': ['0']})
        },
     ],
     'sscs': None,
     'tags': [],
     'tpcreator_uispec': None,
     'seconds_back': 604800,
     'colors': ['#429cf3', '#589b36'],
     'chart_type': 'line',
  }

.. _tile_options_attrs:


   .. attribute:: tile_options.tw_type

      **Required**. A type of a tilewidget that defines what range of data a tile should display. One of:

      * ``Single`` - fetches data from a newest report instance only
      * ``Range`` - fetches data from a time range of report instances



   .. attribute:: tile_options.drawer_type

      **Required**. A type of a drawer that defines postprocessing of the tile's data:

      * for the ``Single`` tilewidget it can be:

        * :class:`~mqe.tilewidgets.ChartSingleDrawer` - meant for rendering a chart. Assures the data series values are numbers. Computes the :data:`tile_data.extra_options` attribute.
        * :class:`~mqe.tilewidgets.TextSingleDrawer` - meant for displaying the data as text. Assures the colors are readable on a white background.

      * for the ``Range`` tilewidget it can be:

        * :class:`~mqe.tilewidgets.ChartRangeDrawer` - meant for rendering a chart. Assures the data series values are numbers. Computes the :data:`tile_data.extra_options` attribute.
        * :class:`~mqe.tilewidgets.TextTableDrawer` - meant for rendering a text table. Assures the colors are readable on a white background. Computes the :data:`tile_data.series_data_as_rows` attribute.


   .. attribute:: tile_options.owner_id

      **Required**. An owner ID the defines the owner of the tile.

   .. attribute:: tile_options.report_id

      **Required**. A report ID the defines the source of data.

   .. attribute:: tile_options.tags

      **Required**. A list of string tags that act as a filter for selecting source report instances.


   .. attribute:: tile_options.series_configs

      **Required**. Configuration of data series - a list of dictionaries with the following keys:

      * **required** ``series_id`` - an ID of a stored series (a value from the database column ``mqe.series_def.series_id``)
      * **required** ``series_spec`` - a :class:`~mqe.dataseries.SeriesSpec` object describing how to select data forming the series.


   .. attribute:: tile_options.tile_title

      A tile title. The library uses it for :ref:`guide_tpcreator`.



   .. attribute:: tile_options.colors

      A list of colors used for rendering data series. The i-th color is assigned to i-th data series. The values should be valid HTML color definitions (.e.g ``#AABBCC``).

      See also :attr:`tile_data.combined_colors`.



   .. attribute:: tile_options.seconds_back

      **Required for tw_type='Range'**. Defines the starting datetime from which report instances should be fetched (they are fetched from the current datetime minus the ``seconds_back`` seconds).



   .. attribute:: tile_options.sscs

      A :class:`~mqe.dataseries.SeriesSpec` that is used as a template for creating new series (see :ref:`guide_sscreator`).




   .. attribute:: tile_options.tpcreator_uispec

      A :data:`tpcreator_uispec` used for :ref:`guide_tpcreator`. This value is filled for a master tile only.



   .. attribute:: tile_options.tpcreator_data

      Filled for tpcreated tiles (see :ref:`guide_tpcreator`). It's a dictionary containing the following keys:

      * ``master_tile_id`` - the |tile_id| of the master tile from which the current tile was created
      * ``master_tpcreator_uispec`` - the :data:`tpcreator_uispec` of the master tile
      * ``tile_title_base`` - the title of the master tile with tag names stripped.



tile_config
^^^^^^^^^^^

.. data:: tile_config

Configuration of a dashboard tile from which full :data:`tile_options` can be produced. It requires less parameters than :data:`tile_options` and performs security checks.

See :ref:`guide_tile_config_and_tile_options` for more information.

Sample :data:`tile_config`::

  tile_config = {
      'tags': ['ip:192.168.1.1'],
      'tw_type': 'Range',
      'series_spec_list': [
          dataseries.SeriesSpec(1, 0, {'op': 'eq', 'args': ['val1']}),
          dataseries.SeriesSpec(1, 0, {'op': 'eq', 'args': ['val2']}),
      ],
      'tile_options': {
          'seconds_back': 86400,
          'tile_title': 'Users'
      }
  }

.. _tile_config_attrs:

    .. attribute:: tile_config.series_spec_list

       **Required**. A list of :class:`~mqe.dataseries.SeriesSpec` objects that describe data series.

    .. attribute:: tile_config.tw_type

       The tilewidget type. The same meaning as :attr:`tile_options.tw_type`. Default value: ``Range``.

    .. attribute:: tile_config.tags

       A list of report tags. The same meaning as :attr:`tile_options.tags`. Default value: ``[]``.


    .. attribute:: tile_config.tile_options

       An optional subset of :data:`tile_options` parameters that should be put into the final :data:`tile_options`.

       For security reasons the following keys contained in the subset are ignored:

       * :data:`~tile_options.owner_id` and :data:`~tile_options.report_id` - the parameters must be explicitly passed to the :meth:`~mqe.tiles.Tile.insert` or the :meth:`~mqe.tiles.Tile.insert_similar` method

       * :data:`~tile_options.series_configs` - the attribute is compiled from :attr:`tile_config.series_spec_list`





tile_data
^^^^^^^^^

.. data:: tile_data

The data of a tile formatted for rendering, returned by :meth:`~mqe.tilewidgets.Tilewidget.get_tile_data`.

Sample value::

  {'generated_tile_title': u'my_data (col. 0 (0), col. 1 (0))',
   'extra_options': {'y_axis_max': None, 'y_axis_min': 0},
   'fetched_from_dt': datetime.datetime(2017, 8, 2, 10, 3, 50, 253288),
   'fetched_to_dt': datetime.datetime(2017, 8, 3, 10, 3, 50, 253288),
   'latest_extra_ri_data': {},
   'report_name': u'my_data',
   'series_data': [{'common_header': None,
                    'data_points': [DataPoint(rid=UUID('0c3b3634-7833-11e7-b9a1-bc5ff4d0b01f'),   dt=datetime.datetime(2017, 8, 3, 10, 3, 50, 196178), value=10),
                             DataPoint(rid=UUID('0c3fa124-7833-11e7-b9a1-bc5ff4d0b01f'),   dt=datetime.datetime(2017, 8, 3, 10, 3, 50, 225130), value=12)],
                    'name': u'col. 0 (0)',
                    'series_id': UUID('0c42151c-7833-11e7-b9a1-bc5ff4d0b01f')},
                   {'common_header': None,
                    'data_points': [DataPoint(rid=UUID('0c3b3634-7833-11e7-b9a1-bc5ff4d0b01f'),   dt=datetime.datetime(2017, 8, 3, 10, 3, 50, 196178), value=20),
                             DataPoint(rid=UUID('0c3fa124-7833-11e7-b9a1-bc5ff4d0b01f'),   dt=datetime.datetime(2017, 8, 3, 10, 3, 50, 225130), value=22)],
                  'name': u'col. 1 (0)',
                  'series_id': UUID('0c4324c0-7833-11e7-b9a1-bc5ff4d0b01f')}],
   'combined_colors': ['#4E99B2', '#8ED2AB'],
   'common_header': None}

.. _tile_data_attrs:


    .. attribute:: tile_data.generated_tile_title

    A suggested chart (tile) title based on a report name and series names.

    .. attribute:: tile_data.generated_tile_title_postfix

    The postfix of the suggested chart title composed of tag names.

    .. attribute:: tile_data.extra_options

    Available for ``drawer_type = Chart*`` only. A dictionary containing suggested options that should be set for the rendered chart:

    * ``y_axis_min`` - the minimal value of the ``y`` axis. Set to ``0`` if all data series values are non-negative.
    * ``y_axis_max`` - the maximal value of the ``y`` axis. Set to ``1`` if all values are not greater than ``1`` (e.g. percents or booleans).

    .. note:: In the current version of the library the ``y_axis_min`` and ``y_axis_max`` values are only set for the special cases of ``0`` and ``1``. In the other cases, they are set to ``None``.

    .. attribute:: tile_data.fetched_from_dt

    .. attribute:: tile_data.fetched_to_dt

    Available for ``tw_type = Range`` only. The :class:`datetime.datetime` objects that define the time range for which the data was fetched.

    .. attribute:: tile_data.latest_extra_ri_data

    The value of ``extra_ri_data`` - custom data attached to the latest fetched report instance.

    .. attribute:: tile_data.report_name

    The name of the report associated with the tile.

    .. attribute:: tile_data.series_data

    Series data defined as a list of dictionaries for each data series (which are usually specified as :attr:`tile_config.series_spec_list`). Each dictionary has the following keys:

    * ``series_id`` - an ID of a :class:`~mqe.dataseries.SeriesDef` (an UUID)
    * ``name`` - a :meth:`~mqe.dataseries.SeriesSpec.name` of the series
    * ``data_points`` - series data as a list of :class:`~.DataPoint` objects
    * ``common_header`` - (optional, can be ``None``) the value of a header from the source report instances. Set only if all returned data series points have the same header.

    .. note:: The order of the :attr:`tile_data.series_data` dictionaries matches the order of the :attr:`tile_options.series_configs`, allowing using list indexes to identify data series in a context of a single :data:`tile_options`.

    .. attribute:: tile_data.combined_colors

    The list of suggested colors that should be assigned to the data series. See :ref:`guide_colors` for more information.

    .. attribute:: tile_data.common_header

    Set to the value of ``common_header`` of :attr:`tile_data.series_data` dicts if the value is the same for all the data series (it's ``None`` otherwise). The value can be usually set as a Y axis title of a rendered chart.

    .. attribute:: tile_data.series_data_as_rows

    Available only for ``drawer_type = TextTableDrawer``. A list of data series points (coming from all data series) formatted for rendering a text table. Each element of the list represents a table row - it's a tuple with the first element identifying a report instance (a tuple ``(report_instance_id, report_instance_creation_dt)``) and the second containing the series data for the report instance (a dictionary that maps a data series index to a data series value).



filtering_expr
^^^^^^^^^^^^^^

.. data:: filtering_expr

Defined for a |SeriesSpec|. Specifies a predicate which tells which table row contains the wanted value.

Sample value::

  { 'op': 'eq', 'args': ['user1', 'user2'] }

The sample will be compiled to a test::

  value = 'user1' or value = 'user2'

.. _filtering_expr_attrs:


   .. attribute:: filtering_expr.op

      What operation to use for testing a value:

      * ``eq`` - equality
      * ``contains`` - searches for a substring inside the tested value


   .. attribute:: filtering_expr.args

      A list of string arguments the tested value is checked against. If one of the tests succeeds, the whole test succeeds.

      .. note:: Each argument must be a string. If a value present in a table is not a string, it's converted to a string using :meth:`~mqe.pars.enrichment.EnrichedValue.to_string_key`.




tpcreator_uispec
^^^^^^^^^^^^^^^^

.. data:: tpcreator_uispec

Specifies how the Tag Prefix Creator should work (see :ref:`guide_tpcreator`). Defined inside :data:`tile_options` for a master tile.

Sample value::

  [{
    "tag": "t1:0",
    "prefix": "t1:0"
  }, {
    "tag": "t2:3",
    "prefix": "t2:"
  }]


It's a list of dictionaries with the following keys:

* ``tag`` - name of a tag for which the dict is specified
* ``prefix`` - prefix of a tag specified as ``tag`` that must be matched (it can be the full ``tag`` value, or an empty string)




visual_options
^^^^^^^^^^^^^^

.. data:: visual_options

Present in the |layout_dict| as a value. Defines placing and a size of a single dashboard tile. The values are defined for a grid having :data:`mqeconfig.DASHBOARD_COLS` columns (and an infinite number of rows).

Sample value::

  {
    "x": 9,
    "y": 0,
    "height": 4,
    "width": 3
  }


.. _visual_options_attrs:

   .. attribute:: visual_options.x

   .. attribute:: visual_options.y

   The horizontal and the vertical position of the top-left corner.


   .. attribute:: visual_options.height

   .. attribute:: visual_options.width

   The height and the width of the tile.




Module reference
----------------

Tiles, layouts, data series
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: mqe.dataseries
   :members:

.. automodule:: mqe.tiles
   :members:

.. automodule:: mqe.tilewidgets
   :members:

.. automodule:: mqe.layouts
    :members:


Dashboards
^^^^^^^^^^

.. automodule:: mqe.dashboards
    :members:


Reports and report instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: mqe.reports
   :members:



SSC, TPCreator
^^^^^^^^^^^^^^^

.. automodule:: mqe.sscreator
    :members:

.. automodule:: mqe.tpcreator
    :members:


Parsing data into tables
^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: mqe.pars.parseany
   :members:

.. automodule:: mqe.pars.parsing
   :members:

.. automodule:: mqe.pars.basicparsing
   :members:

.. automodule:: mqe.pars.asciiparsing
   :members:

.. automodule:: mqe.pars.enrichment
   :members:

Signals
^^^^^^^

.. automodule:: mqe.signals
    :members:


Configuration module
^^^^^^^^^^^^^^^^^^^^

.. automodule:: mqe.mqeconfig
   :members:


DAO classes
^^^^^^^^^^^

.. automodule:: mqe.dao.daobase
    :members:


Utilities
^^^^^^^^^

.. automodule:: mqe.dbutil
   :members:

.. automodule:: mqe.context
   :members:

.. automodule:: mqe
   :members:

.. automodule:: mqe.serialize
   :members:
