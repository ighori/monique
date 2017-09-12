CREATE TABLE mqe.report (
    report_id timeuuid,
    report_name text,
    owner_id uuid,
    PRIMARY KEY(report_id)
);

CREATE TABLE mqe.report_by_owner_id_report_name (
    owner_id uuid,
    report_name text,
    report_id timeuuid,
    PRIMARY KEY(owner_id, report_name)
);

CREATE TABLE mqe.report_name_by_owner_id_report_name_prefix (
    owner_id uuid,
    report_name_prefix text,
    report_name text,
    PRIMARY KEY(owner_id, report_name_prefix, report_name)
);

CREATE TABLE mqe.report_instance (
    report_id timeuuid,
    day text,
    tags_repr text,
    report_instance_id timeuuid,
    ri_data text,
    input_string text,
    all_tags_repr text,
    PRIMARY KEY((report_id, day, tags_repr), report_instance_id)
)
WITH CLUSTERING ORDER BY (report_instance_id DESC);

CREATE TABLE mqe.report_instance_metadata (
    report_id timeuuid,
    day text,
    report_instance_id timeuuid,
    all_tags_repr text,
    inserted timestamp,
    extra_ri_data text,
    PRIMARY KEY((report_id, day), report_instance_id)
)
WITH CLUSTERING ORDER BY (report_instance_id DESC);

CREATE TABLE mqe.report_instance_day (
    report_id timeuuid,
    tags_repr text,
    day text,
    PRIMARY KEY((report_id, tags_repr), day)
)
WITH caching = {'keys':'ALL', 'rows_per_partition':'ALL'};

CREATE TABLE mqe.report_tag (
    report_id timeuuid,
    tag_prefix text,
    tag text,
    PRIMARY KEY(report_id, tag_prefix, tag)
);



CREATE TABLE mqe.series_def (
    report_id timeuuid,
    tags_repr text,
    series_id timeuuid,
    series_spec text,
    from_rid timeuuid,
    to_rid timeuuid,
    PRIMARY KEY(report_id, tags_repr, series_id)
);

CREATE TABLE mqe.series_def_by_series_spec (
    report_id timeuuid,
    tags_repr text,
    series_spec text,
    series_id timeuuid,
    PRIMARY KEY(report_id, tags_repr, series_spec)
);

CREATE TABLE mqe.series_value (
    series_id timeuuid,
    report_instance_id timeuuid,
    json_value text,
    header text,
    PRIMARY KEY(series_id, report_instance_id)
);

CREATE TABLE mqe.series_spec_default_options (
    report_id timeuuid,
    series_spec text,
    default_options text,
    PRIMARY KEY(report_id, series_spec)
);



CREATE TABLE mqe.report_instance_count (
    report_id timeuuid,
    tags_repr text,
    count counter,
    PRIMARY KEY(report_id, tags_repr)
);

CREATE TABLE mqe.report_instance_count_for_owner (
    owner_id uuid,
    count counter,
    PRIMARY KEY(owner_id)
);

CREATE TABLE mqe.report_instance_diskspace (
    report_id timeuuid,
    tags_repr text,
    bytes counter,
    PRIMARY KEY(report_id, tags_repr)
);

CREATE TABLE mqe.report_instance_diskspace_for_owner (
    owner_id uuid,
    bytes counter,
    PRIMARY KEY(owner_id)
);


