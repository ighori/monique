.. _guide_sscreator:

Auto-creating new data series
=============================


Recall our example from the :ref:`tutorial <tutorial_report>` where we have created a tile displaying users' points which included data series specification for users ``john`` and ``monique``. What will happen when data for a new user will appear in a table? ::

    input = """\
    user_name is_active points
    john      true      128
    monique   true      210
    alex      true      12
    """
    points_report.process_input(input)

    series_names = [series_data['name'] for series_data in tile.get_tile_data()['series_data']]
    print series_names

    > [u'john', u'monique']

We can see that although the table contained a new user ``alex``, the :data:`tile_data` didn't include a data series for ``alex``. The behaviour is correct - the :data:`tile_config` includes only two specific data series.

If we want a tile to show data series for all rows in a table, we must use Series Spec Creator. The algorithm, based on a sample |SeriesSpec| that should be copied, creates |SeriesSpec| objects for each table row which doesn't form a header. The sample |SeriesSpec| is called Series Spec Creator Spec (abbreviated to SSCS).

Let's create a new tile on a new dashboard that will use the Series Spec Creator:

.. code-block:: python
    :emphasize-lines: 12

        new_dashboard = owner_dashboards.insert_dashboard('Points')

        tile_config = {
            'tw_type': 'Range',
            'series_spec_list': [
                SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']}),
                SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']}),
            ],
            'tile_options': {
                'seconds_back': 86400,
                'tile_title': 'Points by user',
                'sscs': SeriesSpec(2, 0, {'op': 'eq', 'args': ['john']})
            }
        }
        new_tile = Tile.insert(owner_id, points_report.report_id, new_dashboard.dashboard_id, tile_config)
        place_tile(new_tile)

    The new :data:`tile_config` includes the :attr:`tile_options.sscs` attribute which is just the first regular |SeriesSpec|.

Series Spec Creator works by replacing old tiles with new tiles containing new series (the library uses a model of immutable tiles and layouts - see :ref:`guide_layouts`). We can check if the Creator does its job by creating a new report instance and checking the series of the tile present in the layout::

    input = """\
    user_name is_active points
    john      true      133
    monique   true      220
    alex      true      18
    andrew    true      6
    """
    points_report.process_input(input)

    layout = Layout.select(owner_id, new_dashboard.dashboard_id)
    tile_id = layout.layout_dict.keys()[0]
    tile = Tile.select(new_dashboard.dashboard_id, tile_id)

    series_names = [series_data['name'] for series_data in tile.get_tile_data()['series_data']]
    print series_names

    > [u'john', u'monique', u'alex', u'andrew']


Subscribing to a signal
-----------------------

If you need to know when the Creator replaces a tile by creating new series, you can subscribe to a signal :data:`~mqe.signals.layout_modified` which receives information about the replaced tiles as the |lmr| object::

    from mqe.signals import layout_modified

    @layout_modified.connect
    def on_layout_modified(c, layout_modification_result, reason, **kwargs):
        if reason == 'ssc':
            old_tiles = layout_modification_result.tile_replacement.keys()
            new_tiles = layout_modification_result.tile_replacement.values()
            print 'SSC replaced tiles %s with tiles %s' % (old_tiles, new_tiles)

A lower-level interface
-----------------------

The default behaviour is to call the Series Spec Creator for each report instance created by the |pi| method. For a more fine-grained control, ``handle_ssc=False`` can be passed to the method and the Creator can be invoked manually by calling :meth:`~mqe.sscreator.handle_sscreator` or :meth:`~mqe.sscreator.create_new_series`.

