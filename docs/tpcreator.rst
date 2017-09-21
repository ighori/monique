.. _guide_tpcreator:


Auto-creating new tiles
=======================

If you have a single server and want to graph its CPU load, the job is simple - create a dashboard tile displaying the metrics. The task is more complex when you have multiple servers, especially when they are dynamically created and destroyed. Placing all the metrics inside a single tile results in an unreadable chart.

Monique Dashboards solve the issue by using a concept of a **master tile** which can be automatically copied. In our example a master tile should be created for a sample server and its metrics. When metrics for a new server are received, the master tile is copied. The new tile will display metrics for the new server.

The concept isn't limited to servers and is useful whenever multiple instances of the same entity need to be placed on a dashboard: stock market prices, blog posts stats, microservices instances' metrics etc.


Using tags for identifying entities
------------------------------------

Tags are custom string labels attached to report instances that can be passed to the :meth:`.process_input` method. For example, when sending CPU usage metrics, we could attach a tag identifying the source server::

    from mqe.reports import Report

    cpu_report = Report.select_or_insert(owner_id, 'cpu_usage')
    metrics = [
        ('user', 42.3),
        ('system', 13.4),
        ('io', 8.4),
    ]
    cpu_report.process_input(json.dumps(metrics), tags=['ip:192.168.1.18'])

When the report will be sent from multiple servers, the prefix ``ip:`` will be shared among tag values, while postfixes will be different. The library supports creating a tile for each distinct postfix value. The algorithm is called Tag Prefix Creator (abbreviated to TPCreator).

While including the ``:`` character in a tag name is not enforced, it's a useful convention. The part until the character can be treated as a *property name*, and the part after the character as a *property value*. In our example the property ``ip`` has the value ``192.168.1.18``.
When the report will be sent from multiple servers, the property ``ip`` will have different values and TPCreator will create a tile for each distinct property value.


Creating a master tile
----------------------

A master tile is a sample tile from a group of tiles that we want to create. It defines data series, visualization type and other |tile_options| that will be shared among the group. It must also define a :attr:`~tile_options.tpcreator_uispec`, a description of how the TPCreator should work:

.. code-block:: python
    :emphasize-lines: 3,10

        master_tile_config = {
            'tw_type': 'Range',
            'tags': ['ip:192.168.1.18'],
            'series_spec_list': [
                SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
                SeriesSpec(1, 0, {'op': 'eq', 'args': ['system']}),
            ],
            'tile_options': {
                'tile_title': 'CPU usage',
                'tpcreator_uispec': [{'tag': 'ip:192.168.1.18', 'prefix': 'ip:'}]
            }
        }
        master_tile = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id,
                                  master_tile_config)
        place_tile(master_tile)


The :attr:`~tile_options.tags` attribute is a regular attribute that tells that the tile should display the data coming from report instances having the specified tags.

The :data:`tpcreator_uispec` tells what prefix of each tag must be matched in order to create a new tile. The example tells that when a report instance with a tag having the prefix ``ip:`` is received, a new tile should be possibly created by copying the master tile and assigning the new tag to it.

If the convention of including the ``:`` character in a tag name is used, a helper function :func:`.suggested_tpcreator_uispec` can be used::

                'tpcreator_uispec': suggested_tpcreator_uispec(['ip:192.168.1.18'])


Creating tiles from a master tile
---------------------------------

When the master tile is placed in a layout, a regular creation of a report instance will trigger TPCreator::

    cpu_report.process_input(metrics, tags=['ip:192.168.2.51'])
    cpu_report.process_input(metrics, tags=['ip:192.168.1.30'])
    cpu_report.process_input(metrics, tags=['ip:192.168.2.51'])

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    for tile in layout.tile_dict:
        print tile.tags

    > [u'ip:192.168.1.18']
    > [u'ip:192.168.1.30']
    > [u'ip:192.168.2.51']

We can see that two new tiles, having the expected tags, were *tpcreated* (the word means tile creation done by TPCreator). A report instance for the tag ``ip:192.168.2.52`` was sent twice, but the creation is triggered only for tags for which a tpcreated tile doesn't exist yet.

The master of a tpcreated tile can be identified by calling :meth:`.get_master_tile_id`. Additionally some master's options are included in the tpcreated tile's options under :data:`tile_options.tpcreator_data`, including the title of the master tile, allowing using it to automatically assign a title to a tpcreated tile.

The tpcreated tiles are sorted by tag value and grouped together - when multiple master tiles are present in a layout or regular tiles are placed in the same layout as regular tiles, the tpcreated tiles will always be near their master. If you have changed a layout dict manually and want to ensure the sorting is preserved, the :func:`.repack` function should be called (which is also available as a layout mod :func:`.repack_mod`).


Note also that TPCreator works well together with :ref:`Series Spec Creator <guide_sscreator>`. When a master tile contains :data:`tile_options.sscs`, tpcreated tiles will inherit it and new series will be added to the tpcreated tiles dynamically (a set of created series specifications is not necessarily shared among the tpcreated tiles).


Synchronizing options of tpcreated tiles
----------------------------------------

The master tile should act as a template even after some tiles were tpcreated from it. When its :data:`tile_options` are updated, the change should be reflected in the tpcreated tiles. The behaviour is achieved when the :func:`.replace_tiles` function is used to update the master tile. For example, if we would like to add a new data series to all tpcreated tiles, it's sufficient to do it for the master tile:

.. code-block:: python
    :emphasize-lines: 7,16

    new_master_tile_config = {
        'tw_type': 'Range',
        'tags': ['ip:192.168.1.18'],
        'series_spec_list': [
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['user']}),
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['system']}),
            SeriesSpec(1, 0, {'op': 'eq', 'args': ['io']}),
        ],
        'tile_options': {
            'tile_title': 'CPU usage',
            'tpcreator_uispec': [{'tag': 'ip:192.168.1.18', 'prefix': 'ip:'}]
        }
    }
    new_master_tile = Tile.insert(owner_id, cpu_report.report_id, dashboard.dashboard_id,
                                  new_master_tile_config)
    replace_tiles({master_tile: new_master_tile})

    for tile in layout.tile_dict:
        print len(tile.get_tile_data()['series_data'])

    > 3
    > 3
    > 3

We can see that the :attr:`tile_data.series_data` includes data for three series for each tile.

When the size of a master tile changes, the sizes of tpcreated tiles can be synchronized by calling :func:`.synchronize_sizes_of_tpcreated` (which is also available as a layout mod :func:`.synchronize_sizes_of_tpcreated_mod`).


Expiring tiles and promoting new masters
----------------------------------------

When TPCreator is used to create tiles for ephemeral servers, the tiles displaying reports from destroyed servers stop receiving data and should be deleted. The task could be achieved by a regular call to :func:`.detach_tiles`. The effect will be as one would expect if the list of tiles includes tpcreated tiles only. But if we detach a master tile, TPCreator will no longer work.

The good news is that the library provides a ready function :func:`.expire_tiles_without_data` that detaches tiles that received no data for a given time period. The problem of detaching a master tile is solved by *promoting a new master* - choosing one of the tpcreated tiles as the new master inheriting all the tpcreated tiles. If there are no tpcreated tiles, the master tile is not being detached even if it doesn't have data.

The promotion of a new master can be a useful operation in other cases. It can be implemented by calling :func:`.make_master_from_tpcreated` and :func:`.replace_tiles`:

.. code-block:: python
    :emphasize-lines: 5,6

    old_master = [tile for tile in layout.tile_dict if tile.is_master_tile()][0]
    new_chosen_master = [tile for tile in layout.tile_dict if tile.tags == ['ip:192.168.2.51']][0]
    assert not new_chosen_master.is_master_tile()

    new_master = make_master_from_tpcreated(old_master, new_chosen_master)
    replace_tiles({old_master: new_master, new_chosen_master: None}, for_layout_id=None)

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    tile = [tile for tile in layout.tile_dict if tile.tags == ['ip:192.168.2.51']][0]
    print tile.is_master_tile()

    > True

In the example the ``new_chosen_master`` is selected explicitly, by searching for a tile having the given tags. If a new master must be chosen automatically, the function :func:`.select_tpcreated_tile_ids` can be used to get a list of tpcreated tile IDs.


Using multiple tags
-------------------

When multiple tags are being assigned to report instances, TPCreator allows specifying which tags' prefixes should be processed. For example, we could add a tag identifying a data center::

    cpu_report.process_input(metrics, tags=['ip:192.168.2.51', 'dc:dc-west'])

If we specify the following :data:`tpcreator_uispec`::

    'tpcreator_uispec': [{'tag': 'ip:192.168.2.51', 'prefix': 'ip:'},
                         {'tag': 'dc:dc-west', 'prefix': 'dc:dc-west'}]

a new tile will be created only for tags including the full tag ``dc:dc-west``. If we want to create a tile for each combination of ``ip`` and ``dc`` properties, the following :data:`tpcreator_uispec` should be used::

    'tpcreator_uispec': [{'tag': 'ip:192.168.2.51', 'prefix': 'ip:'},
                         {'tag': 'dc:dc-west', 'prefix': 'dc:'}]

Note that the default limit of a number of tags that can be attached to a report instance is three (it's a limitation of the database model).


Subscribing to a signal
-----------------------

When TPCreator creates a new tile from a master tile, it issues a signal :data:`~mqe.signals.layout_modified` containing the newly created tile::

    from mqe.signals import layout_modified

    @layout_modified.connect
    def on_layout_modified(c, layout_modification_result, reason, **kwargs):
        if reason == 'tpcreator':
            new_tile = layout_modification_result.new_tiles.keys()[0]
            print 'TPCreator created a new tile:', new_tile

Note also that when a call to :func:`.replace_tiles` involves master tiles, the actual number of replaced tiles can be higher than the number of passed tiles. It's because when a master tile changes, its tpcreated tiles are also changed. For example, we could print the number of tiles changed in the call from the previous paragraph::

    res = replace_tiles({old_master: new_master}, for_layout_id=None)
    print 'replaced %d tiles' % len(res.tile_replacement)

    > replaced 3 tiles

The :attr:`~.LayoutModificationResult.tile_replacement` attribute is a dictionary mapping old tiles to new tiles, allowing tracking the "identity" of tiles.


A lower-level interface
-----------------------

The default behaviour is to call the TPCreator for each report instance created by the |pi| method. For a more fine-grained control, ``handle_tpcreator=False`` can be passed to the method and the TPCreator can be invoked manually by calling :meth:`~mqe.tpcreator.handle_tpcreator`.
