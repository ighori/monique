.. _guide_layouts:

Layouts and tiles
=================

Monique Dashboards use a model of immutable layouts and tiles. While it makes some basic operations not straightforward, like updating tile's options, other, traditionally difficult tasks, are easy: concurrent-safe updates, synchronizing dashboard content between multiple devices or caching.

The immutability means that once a |Tile| with a given |tile_id| is created, its |tile_options| cannot be modified. It can only be deleted and detached from a layout.

For a |Layout| the immutability means that once a |Layout| is set for a given dashboard and assigned a |layout_id|, its |layout_dict| cannot be modified. The |Layout| can only be replaced by a new |Layout| set for the same dashboard, having a different |layout_id|.


Placing, detaching, replacing tiles
-----------------------------------

The high-level functions that modify a layout accept a parameter ``for_layout_id``. If the parameter is set to ``None``, it means that the operation can be performed for any |layout_id|, possibly with multiple tries. Otherwise, the parameter tells for which specific |layout_id| the operation should be made. The two modes have their uses - for example, when :ref:`TPCreator <guide_tpcreator>` creates a new tile, it doesn't care for which |layout_id| the tile is placed. But when a user wants to place a new tile in a layout displayed in a web browser, we should ensure that the operation will be performed for the version of the layout the user sees.

Sample code for applying the :func:`~mqe.layouts.place_tile` operation to a specific layout could look like this::

    from mqe.layouts import Layout, place_tile

    layout = Layout.select(owner_id, dashboard.dashboard_id)

    # we decided that new_tile should be put in the current layout

    res = place_tile(new_tile, for_layout_id=layout.layout_id)
    if not res:
        raise ValueError('Placing the tile unsuccessful')
    else:
        print 'New tile placed with visual_options', res.new_tiles[new_tile]

    > New tile placed with visual_options {'width': 4, 'height': 4, 'x': 4, 'y': 0}

The sample also shows a usage of the |lmr| object - a result of modifying a layout returned by many functions. The object lists new, detached and replaced tiles.

Detaching a tile from a layout is done in a similar way to placing, by calling :func:`.detach_tile`.

The function for replacing tiles, :func:`.replace_tiles`, is very useful. Since a tile is immutable, we cannot simply modify it. We must create a new tile and set it as the replacement. The example shows how we could "modify" a tile's title::

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    tile = layout.tile_dict.keys()[0]

    # retrieve and modify the existing tile_config
    tile_config = tile.get_tile_config()
    tile_config['tile_options']['tile_title'] = 'New Title'

    # replace tiles
    repl_tile = tile.insert_similar(tile_config)
    res = replace_tiles({tile: repl_tile}, for_layout_id=layout.layout_id)
    if not res:
        raise ValueError('Replacement of tiles unsuccessful')
    else:
        print 'Tiles replaces:', res.tile_replacement

    > Tiles replaced: {Tile({'tile_id': UUID('e5ed5d64-919a-11e7-b953-bc5ff4d0b01f')}): Tile({'tile_id': UUID('e5f31b46-919a-11e7-b953-bc5ff4d0b01f')})}

The example shows a usage of the :meth:`.get_tile_config` method to retrieve a :data:`tile_config` from an existing tile, and the :meth:`.insert_similar` method that inserts a new tile for the same owner ID, report ID and dashboard ID as the source tile.


.. _guide_layouts_custom:

Setting a custom layout
-----------------------

A new |layout_dict| can be set explicitly by using the :meth:`~mqe.layouts.Layout.set` method of the |Layout| class. In that case we take responsibility for creating a correct layout (ie., without intersecting tiles). For example, if we would like to increase the height of each tile by one, and add a new tile, we could do the following::

    layout = Layout.select(owner_id, dashboard.dashboard_id)

    for visual_options in layout.layout_dict.values():
        visual_options['height'] += 1
    layout.layout_dict[new_tile.tile_id] = {'width': 6, 'height': 3, 'x': 10, 'y': 0}

    new_layout_id = layout.set()
    if not new_layout_id:
        raise ValueError('Updating the layout failed')

The default behaviour of the :meth:`~mqe.layouts.Layout.set` method is to update the layout with |layout_id| equal to the one read from the database when the layout was selected, ensuring the operation is concurrent-safe.


Layout mods
-----------
**Layout mods** offer a more structured way of making custom layout modifications. They support retries and composing multiple operations into one layout update.

If we need to replace a tile and place two new tiles, we could achieve it by multiple calls of the :func:`.replace_tile` and the :func:`.place_tile` functions. But the whole operation will not be atomic (the layout could be altered between the calls) and the performance will suffer. The alternative is to compose a single operation from the layout mod functions::

    from mqe.layouts import replace_tiles_mod, place_tile_mod, apply_mods

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    mods = [
        replace_tiles_mod({tile: tile1}),
        place_tile_mod(tile2),
        place_tile_mod(tile3),
    ]
    res = apply_mods(mods, owner_id, dashboard.dashboard_id, for_layout_id=layout.layout_id)
    if not res:
        raise ValueError('Operation failed')
    else:
        print res

    > LayoutModificationResult(
    >     new_tiles={Tile({'tile_id': UUID('fc84fd06-9218-11e7-a66b-bc5ff4d0b01f')}): {'width': 4, 'height': 4, 'x': 8, 'y': 0}, Tile({'tile_id': UUID('fc8537f8-9218-11e7-a66b-bc5ff4d0b01f')}): {'width': 4, 'height': 4, 'x': 8, 'y': 4}},
    >     detached_tiles=[],
    >     tile_replacement={Tile({'tile_id': UUID('fc825790-9218-11e7-a66b-bc5ff4d0b01f')}): Tile({'tile_id': UUID('fc84ca16-9218-11e7-a66b-bc5ff4d0b01f')})})

First, we declare a list of layout mods to apply. By convention, functions returning mods have a name ending with ``_mod``. We use the :func:`.replace_tiles_mod` and the :func:`.place_tile_mod` functions which are counterparts of the already known functions. Next, we apply the mods by calling the :func:`.apply_mods` function. We set the ``for_layout_id`` parameter assuring the operation is performed for the specific layout instance. If the parameter would be set to ``None``, the operation would be performed for any layout instance, possibly with multiple tries.

Other functions returning mods are: :func:`.pack_upwards_mod`, :func:`.pack_leftwards_mod`, :func:`.repack_mod`. There is no mod for detaching since the :func:`.replace_tiles_mod` interprets a mapping of a |Tile| to ``None`` as detachment.


Writing a layout mod
^^^^^^^^^^^^^^^^^^^^

A layout mod is a function that receives a :class:`.LayoutModification` object. The function should modify the ``layout.layout_dict`` attribute of the object and express the modification by putting the modified tiles into :attr:`~.LayoutModification.tile_replacement`, :attr:`~.LayoutModification.new_tiles` and :attr:`~.LayoutModification.detached_tiles` attributes of the object. A mod function can also raise the exception :exc:`.LayoutModificationImpossible` which signals that the operation cannot be performed.

For example, here's a mod that deletes tiles placed in the first row of a layout::

    from mqe.layouts import LayoutModificationImpossible

    def detach_last_tile_mod():

        def do(layout_mod):
            tile_ids = [tile_id for tile_id, visual_options in layout_mod.layout.layout_dict.items()
                        if visual_options['y'] == 0]
            if not tile_ids:
                raise LayoutModificationImpossible()
            for tile_id in tile_ids:
                del layout_mod.layout.layout_dict[tile_id]
                layout_mod.detached_tiles.append(Tile.select(layout_mod.layout.dashboard_id, tile_id))

        return do

    res = apply_mods([detach_last_tile_mod()], owner_id, dashboard.dashboard_id, None)
    if not res:
        raise ValueError('Operation failed')
    else:
        print res

The thing to notice is that we have created the inner function ``do()`` and returned it as a result of the ``detach_last_tile_mod()``. The real layout mod function is the ``do()`` function and we could define it as a normal outer function, but the example fulfills the convention of having a function named ``*_mod`` that returns a layout function. That design allows adding parameters to the ``*_mod`` function without breaking the API.


Deleting unneeded tiles
-----------------------

One thing we didn't discuss is the cleanup of tiles that no longer belong to a layout. For example, when you replace a tile with a new tile, the old tile should be deleted. And when the layout modification fails, the replacement tile should be deleted since it couldn't be put into a layout.

The good news is that when you use layout mods or the high-level level functions like :func:`.place_tile`, the cleanup is done automatically. The manual deletion of tiles (by calling :meth:`~mqe.tiles.Tile.delete`) is necessary when :ref:`a whole layout is replaced <guide_layouts_custom>`.

The other thing to notice is that leaving undeleted tiles is not a critical error and the situation can happen in case of programming errors or killing a process in a middle of an operation. The tiles will occupy a small amount of space in a database table, but the thing that matters is the definition of a layout, and the library ensures that the replacements of layouts are done atomically.
