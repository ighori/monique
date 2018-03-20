import sys
import uuid

from sqlalchemy import Table, Column, BigInteger, Unicode, MetaData, Index, Date, create_engine
from sqlalchemy import types
from sqlalchemy.sql import select, and_, or_, insert

from mqe.dao.daobase import *
from mqe import c
from mqe.dbutil import gen_uuid
from mqe import mqeconfig


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


def init_engine():
    if hasattr(c, 'sqlalchemy_engine'):
        return
    if not getattr(mqeconfig, 'SQLALCHEMY_ENGINE', None):
        raise ValueError('No SQLALCHEMY_ENGINE defined in mqeconfig')
    c.sqlalchemy_engine = create_engine(mqeconfig.SQLALCHEMY_ENGINE)

def connection():
    init_engine()
    return c.sqlalchemy_engine.connect()


class result(object):

    def __init__(self, *args, **kwargs):
        self.connection = connection()
        self.result_proxy = self.connection.execute(*args, **kwargs)

    def __enter__(self):
        return self.result_proxy

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.result_proxy.close()
        self.connection.close()


def execute(*args, **kwargs):
    conn = connection()
    res = conn.execute(*args, **kwargs)
    res.close()
    conn.close()


class SqlalchemyDashboardDAO(DashboardDAO):

    def select(self, owner_id, dashboard_id):
        q = select([dashboard]).\
            where(and_(dashboard.c.owner_id==owner_id, dashboard.c.dashboard_id==dashboard_id))
        with result(q) as res:
            return res.fetchone()

    def select_all(self, owner_id):
        with cursor() as cur:
            cur.execute("""SELECT * FROM dashboard WHERE owner_id=?""",
                        [owner_id])
            return cur.fetchall()


    def insert(self, owner_id, dashboard_name, dashboard_options):
        row = dict(owner_id=owner_id,
                   dashboard_id=gen_uuid(),
                   dashboard_name=dashboard_name,
                   dashboard_options=dashboard_options)
        q = dashboard.insert().values(**row)
        execute(q)
        return row


    def update(self, owner_id, dashboard_id, dashboard_name, dashboard_options):
        with cursor() as cur:
            if dashboard_name is not None:
                cur.execute("""UPDATE dashboard SET dashboard_name=?
                               WHERE owner_id=? AND dashboard_id=?""",
                            [dashboard_name, owner_id, dashboard_id])
            if dashboard_options is not None:
                cur.execute("""UPDATE dashboard SET dashboard_options=?
                               WHERE owner_id=? AND dashboard_id=?""",
                            [dashboard_options, owner_id, dashboard_id])

    def delete(self, owner_id, dashboard_id):
        with cursor() as cur:
            cur.execute("""DELETE FROM dashboard WHERE owner_id=? AND dashboard_id=?""",
                        [owner_id, dashboard_id])


    def select_tile_ids(self, dashboard_id):
        with cursor() as cur:
            cur.execute("""SELECT tile_id FROM tile WHERE dashboard_id=?""", [dashboard_id])
            return [r['tile_id'] for r in cur.fetchall()]


    def select_all_dashboards_ordering(self, owner_id):
        with cursor() as cur:
            cur.execute("""SELECT dashboard_id_ordering
                           FROM all_dashboards_properties
                           WHERE owner_id=?""",
                        [owner_id])
            row = cur.fetchone()
            if row:
                return serialize.json_loads(row['dashboard_id_ordering'])
            return None

    def set_all_dashboards_ordering(self, owner_id, dashboard_id_list):
        with cursor() as cur:
            cur.execute(*replace('all_dashboards_properties', dict(owner_id=owner_id,
                                                                   dashboard_id_ordering=serialize.mjson(dashboard_id_list))))


def create_tables():
    init_engine()
    metadata.create_all(c.sqlalchemy_engine)


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        command = sys.argv[1]
        if command == 'create_tables':
            create_tables()
        else:
            print 'Unknown command %r' % command
