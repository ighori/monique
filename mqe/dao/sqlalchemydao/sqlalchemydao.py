import uuid

from sqlalchemy import Table, Column, BigInteger, Unicode, MetaData, Index, Date
from sqlalchemy import types


class TimeUUID(types.TypeDecorator):

    impl = types.Unicode

    def process_bind_param(self, value, dialect):
        assert value.version == 1
        return u'%x;%s' % (value.time, value.hex.decode('ascii'))

    def process_result_value(self, value, dialect):
        assert ';' in value
        time, hex = value.split(';')
        return uuid.UUID(hex)


class UUID(types.TypeDecorator):

    impl = types.Unicode

    def process_bind_param(self, value, dialect):
        return value.hex.decode('ascii')

    def process_result_value(self, value, dialect):
        return uuid.UUID(value)


class StringList(types.TypeDecorator):

    impl = types.Unicode

    def process_bind_param(self, value, dialect):
        assert isinstance(value, list)
        return u','.join(value)

    def process_result_value(self, value, dialect):
        if not value:
            return []
        return value.split(u',')


metadata = MetaData()

report = Table(
    'report', metadata,
    Column('report_id', TimeUUID, primary_key=True),
    Column('report_name', Unicode),
    Column('owner_id', UUID),
    Column('report_instance_count', BigInteger, default=0),
    Column('report_instance_diskspace', BigInteger, default=0),

    Index('idx__report__owner_id__report_name', 'owner_id', 'report_name'),
)

report_instance = Table(
    'report_instance', metadata,
    Column('report_id', TimeUUID, primary_key=True),
    Column('tags', StringList, primary_key=True),
    Column('report_instance_id', TimeUUID, primary_key=True),
    Column('ri_data', Unicode),
    Column('input_string', Unicode),
    Column('all_tags', StringList),
    Column('extra_ri_data', Unicode),
)

report_instance_day = Table(
    'report_instance_day', metadata,
    Column('report_id', TimeUUID, primary_key=True),
    Column('tags', StringList, primary_key=True),
    Column('day', Date, primary_key=True),
)

report_tag = Table(
    'report_tag', metadata,
    Column('report_id', TimeUUID, primary_key=True),
    Column('tag', Unicode, primary_key=True),
)

report_data_for_owner = Table(
    'report_data_for_owner', metadata,
    Column('owner_id', UUID, primary_key=True),
    Column('report_instance_count', BigInteger, default=0),
    Column('report_instance_diskspace', BigInteger, default=0),
)

series_def = Table(
    'series_def', metadata,
    Column('report_id', TimeUUID, primary_key=True),
    Column('tags', StringList, primary_key=True),
    Column('series_id', TimeUUID, primary_key=True),
    Column('series_spec', Unicode, index=True),
    Column('from_rid', TimeUUID),
    Column('to_rid', TimeUUID),
)

series_value = Table(
    'series_value', metadata,
    Column('series_id', TimeUUID, primary_key=True),
    Column('report_instance_id', TimeUUID, primary_key=True),
    Column('json_value', Unicode),
    Column('header', Unicode),
)

options = Table(
    'options', metadata,
    Column('report_id', TimeUUID, primary_key=True),
    Column('kind', Unicode, primary_key=True),
    Column('options_key', Unicode, primary_key=True),
    Column('options_value', Unicode),
)

dashboard = Table(
    'dashboard', metadata,
    Column('owner_id', UUID, primary_key=True),
    Column('dashboard_id', UUID, primary_key=True),
    Column('dashboard_name', Unicode),
    Column('dashboard_options', Unicode),
)

dashboard_layout = Table(
    'dashboard_layout', metadata,
    Column('owner_id', UUID, primary_key=True),
    Column('dashboard_id', UUID, primary_key=True),
    Column('layout_def', Unicode),
    Column('layout_props', Unicode),
    Column('layout_id', TimeUUID),
)

all_dashboards_properties = Table(
    'all_dashboards_properties', metadata,
    Column('owner_id', UUID, primary_key=True),
    Column('dashboard_id_ordering', Unicode),
)

layout_by_report = Table(
    'layout_by_report', metadata,
    Column('owner_id', UUID, primary_key=True),
    Column('report_id', TimeUUID, primary_key=True),
    Column('label', Unicode, primary_key=True),
    Column('tags', StringList, primary_key=True),
    Column('dashboard_id', UUID, primary_key=True),
    Column('layout_id', TimeUUID)
)

tile = Table(
    'tile', metadata,
    Column('dashboard_id', UUID, primary_key=True),
    Column('tile_id', TimeUUID, primary_key=True),
    Column('tile_options', Unicode),
)