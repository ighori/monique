import sys
import uuid

from sqlalchemy import Table, Column, BigInteger, Unicode, MetaData, Index, Date, create_engine
from sqlalchemy import types
from sqlalchemy.sql import select, and_, or_, insert, bindparam
from sqlalchemy import exc

from mqe.dao.daobase import *
from mqe import c, serialize
from mqe.dbutil import gen_uuid, gen_timeuuid
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
        value = value or []
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
    c.sqlalchemy_engine = create_engine(mqeconfig.SQLALCHEMY_ENGINE, echo=mqeconfig.DEBUG_QUERIES)

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
        q = select([dashboard]).where(and_(dashboard.c.owner_id==owner_id,
                                           dashboard.c.dashboard_id==dashboard_id))
        with result(q) as res:
            return res.fetchone()


    def select_all(self, owner_id):
        q = select([dashboard]).where(dashboard.c.owner_id==owner_id)
        with result(q) as res:
            return res.fetchall()


    def insert(self, owner_id, dashboard_name, dashboard_options):
        row = dict(owner_id=owner_id,
                   dashboard_id=gen_uuid(),
                   dashboard_name=dashboard_name,
                   dashboard_options=dashboard_options)
        q = dashboard.insert().values(**row)
        execute(q)
        return row


    def update(self, owner_id, dashboard_id, dashboard_name, dashboard_options):
        q = dashboard.update().where(and_(dashboard.c.owner_id==owner_id,
                                          dashboard.c.dashboard_id==dashboard_id))
        values = {}
        if dashboard_name is not None:
            values['dashboard_name'] = dashboard_name
        if dashboard_options is not None:
            values['dashboard_options'] = dashboard_options
        execute(q.values(**values))


    def delete(self, owner_id, dashboard_id):
        q = dashboard.delete().where(and_(dashboard.c.owner_id==owner_id,
                                          dashboard.c.dashboard_id==dashboard_id))
        execute(q)


    def select_tile_ids(self, dashboard_id):
        q = select([tile.c.tile_id]).where(tile.c.dashboard_id==dashboard_id)
        with result(q) as res:
            return [r['tile_id'] for r in res.fetchall()]


    def select_all_dashboards_ordering(self, owner_id):
        q = select([all_dashboards_properties.c.dashboard_id_ordering]).\
            where(all_dashboards_properties.c.owner_id==owner_id)
        with result(q) as res:
            row = res.fetchone()
            if row:
                return serialize.json_loads(row['dashboard_id_ordering'])
            return None

    def set_all_dashboards_ordering(self, owner_id, dashboard_id_list):
        q_update = all_dashboards_properties.update().where(
            all_dashboards_properties.c.owner_id==owner_id)
        values = {'dashboard_id_ordering': serialize.mjson(dashboard_id_list),
                  'owner_id': owner_id}
        with result(q_update.values(**values)) as res:
            if res.rowcount == 0:
                q_insert = all_dashboards_properties.insert()
                execute(q_insert.values(**values))



class SqlalchemyLayoutDAO(LayoutDAO):

    def select(self, owner_id, dashboard_id,
               columns=('layout_id', 'layout_def', 'layout_props')):
        q = select([getattr(dashboard_layout.c, col) for col in columns]).\
                where(and_(dashboard_layout.c.owner_id==owner_id,
                           dashboard_layout.c.dashboard_id==dashboard_id))
        with result(q) as res:
            return res.fetchone()


    def select_multi(self, owner_id, dashboard_id_list,
                     columns=('layout_id', 'layout_def', 'layout_props')):
        if 'dashboard_id' not in columns:
            columns += 'dashboard_id',
        q = select([getattr(dashboard_layout.c, col) for col in columns]).\
                where(and_(dashboard_layout.c.owner_id==owner_id,
                           dashboard_layout.c.dashboard_id.in_(dashboard_id_list)))
        with result(q) as res:
            return res.fetchall()

    def set(self, owner_id, dashboard_id, old_layout_id, new_layout_id,
            new_layout_def, new_layout_props):
        if old_layout_id is None:
            q = dashboard_layout.insert()
            values = dict(
                owner_id=owner_id,
                dashboard_id=dashboard_id,
                layout_def=new_layout_def,
                layout_props=new_layout_props,
                layout_id=new_layout_id,
            )
            try:
                execute(q.values(**values))
            except exc.IntegrityError:
                return False
            else:
                return True

        q = dashboard_layout.update().where(and_(
            dashboard_layout.c.owner_id==owner_id,
            dashboard_layout.c.dashboard_id==dashboard_id,
            dashboard_layout.c.layout_id==old_layout_id
        ))
        values = dict(
            layout_id=new_layout_id,
            layout_def=new_layout_def,
            layout_props=new_layout_props,
        )
        with result(q.values(**values)) as res:
            return res.rowcount == 1


    def delete(self, owner_id, dashboard_id):
        q = dashboard_layout.delete().where(and_(
            dashboard_layout.c.owner_id==owner_id,
            dashboard_layout.c.dashboard_id==dashboard_id,
        ))
        execute(q)


    def insert_layout_by_report_multi(self, owner_id, report_id_list, tags, label, dashboard_id,
                                      layout_id):
        for report_id in report_id_list:
            update_q = layout_by_report.update().where(and_(
                layout_by_report.c.owner_id==owner_id,
                layout_by_report.c.report_id==report_id,
                layout_by_report.c.label==label,
                layout_by_report.c.tags==tags,
                layout_by_report.c.dashboard_id==dashboard_id,
            ))
            with result(update_q) as res:
                if res.rowcount == 0:
                    insert_q = layout_by_report.insert()
                    values = dict(
                        owner_id=owner_id,
                        report_id=report_id,
                        tags=tags,
                        label=label,
                        dashboard_id=dashboard_id,
                        layout_id=layout_id,
                    )
                    execute(insert_q.values(**values))


    def delete_layout_by_report(self, owner_id, report_id, tags, label, dashboard_id,
                                layout_id):
        q = layout_by_report.delete().where(and_(
            layout_by_report.c.owner_id==owner_id,
            layout_by_report.c.report_id==report_id,
            layout_by_report.c.tags==tags,
            layout_by_report.c.label==label,
            layout_by_report.c.dashboard_id==dashboard_id,
            layout_by_report.c.layout_id==layout_id,
        ))
        execute(q)


    def select_layout_by_report_multi(self, owner_id, report_id, tags, label, limit):
        q = select([layout_by_report]).where(and_(
            layout_by_report.c.owner_id==owner_id,
            layout_by_report.c.report_id==report_id,
            layout_by_report.c.tags==tags,
            layout_by_report.c.label==label,
        )).limit(limit)
        with result(q) as res:
            return res.fetchall()



class SqlalchemyTileDAO(TileDAO):

    def select_multi(self, dashboard_id, tile_id_list):
        q = select([tile]).where(and_(
            tile.c.dashboard_id==dashboard_id,
            tile.c.tile_id.in_(tile_id_list),
        ))
        with result(q) as res:
            return res.fetchall()


    def insert_multi(self, owner_id, dashboard_id, tile_options_list):
        if not tile_options_list:
            return
        q = tile.insert()
        rows = []
        for tile_options in tile_options_list:
            row = dict(dashboard_id=dashboard_id,
                       tile_id=gen_timeuuid(),
                       tile_options=tile_options)
            rows.append(row)
        execute(q, rows)
        return rows

    def delete_multi(self, tile_list):
        if not tile_list:
            return
        q = tile.delete().where(and_(
            tile.c.dashboard_id==bindparam('dashboard_id'),
            tile.c.tile_id==bindparam('tile_id'),
        ))
        params_list = [{'dashboard_id': t.dashboard_id,
                        'tile_id': t.tile_id} for t in tile_list]
        execute(q, params_list)


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
