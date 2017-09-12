CREATE TABLE mqe.dashboard (
    owner_id uuid,
    dashboard_id uuid,
    dashboard_name text,
    dashboard_options text,
    PRIMARY KEY(owner_id, dashboard_id)
);

CREATE TABLE mqe.dashboard_layout_def (
    owner_id uuid,
    dashboard_id uuid,
    layout_id timeuuid,
    layout_def text,
    layout_props text,
    PRIMARY KEY(owner_id, dashboard_id)
);

CREATE TABLE mqe.all_dashboards_properties (
    owner_id uuid,
    dashboard_id_ordering text,
    PRIMARY KEY(owner_id)
);

CREATE TABLE mqe.tile (
    dashboard_id uuid,
    tile_id timeuuid,
    tile_options text,
    PRIMARY KEY(dashboard_id, tile_id)
);

CREATE TABLE mqe.tile_count (
    owner_id uuid,
    count counter,
    PRIMARY KEY(owner_id)
);
    
CREATE TABLE mqe.layout_by_report (
    owner_id uuid,
    report_id timeuuid,
    label text,
    tags_repr text,
    dashboard_id uuid,
    layout_id timeuuid,
    PRIMARY KEY(owner_id, report_id, label, tags_repr, dashboard_id)
);

