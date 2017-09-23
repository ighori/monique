import tutorial
from tutorial import SECTION


def main():
    vars = tutorial.main()
    points_report = vars['points_report']
    tile = vars['tile']
    owner_id = vars['owner_id']
    owner_dashboards = vars['owner_dashboards']
    dashboard = vars['dashboard']


    SECTION('Placing, detaching, replacing tiles')


    from mqe.layouts import Layout, place_tile

    layout = Layout.select(owner_id, dashboard.dashboard_id)

    new_tile = tile.copy(dashboard.dashboard_id)
    # we decided that new_tile should be put in the current layout
    res = place_tile(new_tile, for_layout_id=layout.layout_id)
    if not res:
        raise ValueError('Placing the tile unsuccessful')
    else:
        print 'New tile placed with visual_options', res.new_tiles[new_tile]


    from mqe.layouts import replace_tiles
    from mqe.tiles import Tile

    layout = Layout.select(owner_id, dashboard.dashboard_id)
    tile = Tile.select(dashboard.dashboard_id, layout.layout_dict.keys()[0])
    tile_config = tile.get_tile_config()
    tile_config['tile_options']['tile_title'] = 'New Title'
    repl_tile = tile.insert_similar(tile_config)
    res = replace_tiles({tile: repl_tile}, for_layout_id=layout.layout_id)
    if not res:
        raise ValueError('Replacement of tiles unsuccessful')
    else:
        print 'Tiles replaced:', res.tile_replacement


    SECTION('Setting a custom layout')


    layout = Layout.select(owner_id, dashboard.dashboard_id)
    for visual_options in layout.layout_dict.values():
        visual_options['height'] += 1
    new_layout_id = layout.set()
    if not new_layout_id:
        raise ValueError('Updating the layout failed')


    SECTION('Layout mods')


    from mqe.layouts import replace_tiles_mod, place_tile_mod, apply_mods
    tile = repl_tile
    tile1 = tile.copy(dashboard.dashboard_id)
    tile2 = tile.copy(dashboard.dashboard_id)
    tile3 = tile.copy(dashboard.dashboard_id)

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


    from mqe.layouts import LayoutModificationImpossible

    def detach_top_tiles_mod():

        def do(layout_mod):
            tile_ids = [tile_id for tile_id, visual_options in layout_mod.layout.layout_dict.items()
                        if visual_options['y'] == 0]
            if not tile_ids:
                raise LayoutModificationImpossible()
            for tile_id in tile_ids:
                del layout_mod.layout.layout_dict[tile_id]
                layout_mod.detached_tiles.append(Tile.select(layout_mod.layout.dashboard_id, tile_id))

        return do

    res = apply_mods([detach_top_tiles_mod()], owner_id, dashboard.dashboard_id, None)
    if not res:
        raise ValueError('Operation failed')
    else:
        print res


    def detach_top_tiles_using_replacement_mod():

        def do(layout_mod):
            tiles = [tile for tile, visual_options in layout_mod.layout.tile_dict.items()
                     if visual_options['y'] == 0]
            if not tiles:
                raise LayoutModificationImpossible()
            replace_tiles_mod({tile: None for tile in tiles})(layout_mod)

        return do






if __name__ == '__main__':
    main()
