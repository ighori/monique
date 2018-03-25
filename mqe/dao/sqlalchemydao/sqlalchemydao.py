import sys
import uuid

from sqlalchemy import Table, Column, BigInteger, Unicode, MetaData, Index, UniqueConstraint, Date, create_engine
from sqlalchemy import types
from sqlalchemy.sql import select, and_, or_, insert, bindparam, literal_column, text
from sqlalchemy import exc

from mqe.dao.daobase import *
from mqe import c, serialize, util
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

    UniqueConstraint('report_name', 'owner_id'),
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
    conn = None
    res = None
    try:
        conn = connection()
        res = conn.execute(*args, **kwargs)
    finally:
        if res is not None:
            res.close()
        if conn is not None:
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
            with result(update_q, layout_id=layout_id) as res:
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



class SqlalchemyReportDAO(ReportDAO):


    def select(self, report_id):
        q = select([report]).where(report.c.report_id==report_id)
        with result(q) as res:
            return res.fetchone()


    def select_multi(self, owner_id, report_id_list):
        q = select([report]).where(report.c.report_id.in_(report_id_list))
        with result(q) as res:
            return res.fetchall()

    def select_or_insert(self, owner_id, report_name):
        row = self.select_by_name(owner_id, report_name)
        if row:
            return False, row

        q = report.insert()
        values = dict(
            report_id=gen_timeuuid(),
            report_name=report_name,
            owner_id=owner_id,
        )
        try:
            execute(q.values(**values))
            return True, values
        except exc.IntegrityError:
            return False, self.select_by_name(owner_id, report_name)


    def select_by_name(self, owner_id, report_name):
        q = select([report]).where(and_(
            report.c.owner_id==owner_id,
            report.c.report_name==report_name,
        ))
        with result(q) as res:
            return res.fetchone()


    def select_ids_by_name_prefix_multi(self, owner_id, name_prefix, after_name, limit):
        if name_prefix is None:
            name_prefix = ''
        and_list = [
            report.c.owner_id==owner_id,
            report.c.report_name.like('%s%%' % name_prefix),
        ]
        if after_name is not None:
            and_list.append(report.c.report_name > after_name)
        q = select([report.c.report_id, report.c.report_name]).\
            where(and_(*and_list)).\
            order_by(report.c.report_name).\
            limit(limit)
        with result(q) as res:
            return [row['report_id'] for row in res.fetchall()]


    def insert(self, owner_id, report_name):
        inserted, row = self.select_or_insert(owner_id, report_name)
        if not inserted:
            return None
        return row


    def delete(self, owner_id, report_id):
        q = report.delete().where(report.c.report_id==report_id)
        execute(q)
        q = report_tag.delete.where(report_tag.c.report_id==report_id)
        execute(q)


    def select_report_instance_count(self, owner_id, report_id):
        q = select([report.c.report_instance_count]).where(and_(
            report.c.owner_id==owner_id,
            report.c.report_id==report_id,
        ))
        with result(q) as res:
            row = res.fetchone()
            if not row:
                return 0
            return row['report_instance_count']


    def select_report_instance_diskspace(self, owner_id, report_id):
        q = select([report.c.report_instance_diskspace]).where(and_(
            report.c.owner_id==owner_id,
            report.c.report_id==report_id,
            ))
        with result(q) as res:
            row = res.fetchone()
            if not row:
                return 0
            return row['report_instance_diskspace']


    def select_report_instance_days(self, report_id, tags):
        q = select([report_instance_day.day]).where(and_(
            report_instance_day.c.report_id==report_id,
            report_instance_day.c.tags==tags,
        ))
        with result(q) as res:
            dates = [row['day'] for row in res.fetchall()]
            return [util.datetime_from_date(d) for d in dates]


    def select_tags_sample(self, report_id, tag_prefix, limit, after_tag):
        q = select([report_tag.c.tag]).where(and_(
            report_tag.c.report_id==report_id,
            report_tag.c.tag > after_tag,
            report_tag.c.tag.like('%s%%' % tag_prefix),
        )).limit(limit)
        with result(q) as res:
            return [r['tag'] for r in res.fetchall()]



class Sqlite3ReportInstanceDAO(ReportInstanceDAO):

    def _compute_ri_diskspace(self, row):
        if not row['input_string']:
            return 0
        return len(row['input_string'])


    def insert(self, owner_id, report_id, report_instance_id, tags, ri_data, input_string,
               extra_ri_data, custom_created):
        created = util.datetime_from_uuid1(report_instance_id)

        tags_powerset = util.powerset(tags[:mqeconfig.MAX_TAGS])
        all_rows = []
        for tags_subset in tags_powerset:
            all_rows.append(dict(report_id=report_id,
                                 tags=tags_subset,
                                 report_instance_id=report_instance_id,
                                 ri_data=ri_data,
                                 input_string=input_string,
                                 all_tags=tags,
                                 extra_ri_data=extra_ri_data))
        q = report_instance.insert()
        execute(q, all_rows)

        q = report_instance_day.insert()
        for tags_subset in tags_powerset:
            try:
                execute(q, report_id=report_id, tags=tags_subset, day=created.date())
            except (exc.IntegrityError, exc.ProgrammingError):
                pass

        q = report.update().where(report.c.report_id==report_id)

        execute(q.values(report_instance_count=(report.c.report_instance_count + 1)))

        diskspace = self._compute_ri_diskspace(all_rows[0])
        execute(q.values(report_instance_diskspace=(report.c.report_instance_diskspace + diskspace)))

        # owner counts
        q = report_data_for_owner.update().where(report_data_for_owner.c.owner_id==owner_id)

        with result(q.values(report_instance_count=(report_data_for_owner.c.report_instance_count + 1))) as res:
            if res.rowcount == 0:
                try:
                    execute(report_data_for_owner.insert(), owner_id=owner_id)
                except (exc.IntegrityError, exc.ProgrammingError):
                    execute(q.values(report_instance_count=(report_data_for_owner.c.report_instance_count + 1)))

        execute(q.values(report_instance_diskspace=report_data_for_owner.c.report_instance_diskspace + diskspace))

        # tags
        q = report_tag.insert()
        for tag in tags:
            try:
                execute(q, report_id=report_id, tag=tag)
            except (exc.IntegrityError, exc.ProgrammingError):
                pass

        return all_rows[0]


    def select_extra_ri_data(self, report_id, report_instance_id):
        q = select([report_instance.c.extra_ri_data]).where(and_(
            report_instance.c.report_id==report_id,
            report_instance.c.tags==[],
            report_instance.c.report_instance_id==report_instance_id,
        ))
        with result(q) as res:
            row = res.fetchone()
            return row['extra_ri_data'] if row else None


    def select(self, report_id, report_instance_id, tags):
        tags = tags or []
        with cursor() as cur:
            cur.execute("""SELECT * FROM report_instance
                           WHERE report_id=? AND tags=? AND report_instance_id=?""",
                        [report_id, tags, report_instance_id])
            return postprocess_tags(cur.fetchone())


    def select_multi(self, report_id, tags, min_report_instance_id, max_report_instance_id,
                     columns, order, limit):
        tags = tags or []
        what = ', '.join(columns) if columns else '*'

        with cursor() as cur:
            cur.execute("""SELECT {what} FROM report_instance
                           WHERE report_id=? AND tags=?
                           AND report_instance_id > ? AND report_instance_id < ?
                           ORDER BY report_instance_id {order} LIMIT ?""". \
                        format(what=what, order=order),
                        [report_id, tags, min_report_instance_id, max_report_instance_id,
                         limit])
            return map(postprocess_tags, cur.fetchall())


    def select_latest_id(self, report_id, tags):
        tags = tags or []
        with cursor() as cur:
            cur.execute("""SELECT report_instance_id FROM report_instance
                           WHERE report_id=? AND tags=?
                           ORDER BY report_instance_id DESC LIMIT 1""",
                        [report_id, tags])
            row = cur.fetchone()
            return row['report_instance_id'] if row else None

    def delete(self, owner_id, report_id, report_instance_id, update_counters):
        ri = self.select(report_id, report_instance_id, [])
        if not ri:
            return 0, []
        return self._delete_ris(owner_id, report_id, ri['all_tags'], [ri], update_counters)

    def delete_multi(self, owner_id, report_id, tags, min_report_instance_id, max_report_instance_id,
                     limit, update_counters, use_insertion_datetime):
        if use_insertion_datetime:
            raise NotImplementedError('Sqlite3ReportInstanceDAO doesn\'t support the use_insertion_datetime flag')
        ris = self.select_multi(report_id, tags, min_report_instance_id, max_report_instance_id,
                                ['report_instance_id', 'all_tags', 'input_string'], 'asc', limit)
        log.info('Selected %d report instances for deletion', len(ris))
        return self._delete_ris(owner_id, report_id, tags, ris, update_counters)

    def _delete_ris(self, owner_id, report_id, tags, ris, update_counters):
        qs = []
        tags_days = set()
        all_tags_subsets = set()

        with cursor() as cur:
            for ri in ris:
                tags_powerset = util.powerset(ri['all_tags'])
                cur.execute("""DELETE FROM report_instance WHERE report_id=?
                               AND tags IN {in_p} AND report_instance_id=?""".format(in_p=in_params(tags_powerset)),
                            [report_id] + tags_powerset + [ri['report_instance_id']])
                day = util.datetime_from_uuid1(ri['report_instance_id']).date()
                for tags_subset in tags_powerset:
                    tags_days.add((tuple(tags_subset), day))
                    all_tags_subsets.add(tuple(tags_subset))

            if update_counters:
                total_diskspace = sum(self._compute_ri_diskspace(ri) for ri in ris)
                cur.execute("""UPDATE report
                               SET report_instance_count = report_instance_count - ?
                               WHERE report_id=?""",
                            [len(ris), report_id])
                cur.execute("""UPDATE report
                               SET report_instance_diskspace = report_instance_diskspace - ?
                               WHERE report_id=?""",
                            [total_diskspace, report_id])
                cur.execute("""UPDATE report_data_for_owner
                               SET report_instance_count=report_instance_count - ?
                               WHERE owner_id=?""",
                            [len(ris), owner_id])
                cur.execute("""UPDATE report_data_for_owner
                               SET report_instance_diskspace=report_instance_diskspace - ?
                               WHERE owner_id=?""",
                            [total_diskspace, owner_id])


            ### Delete days for which report instances no longer exist

            for day_tags, day in tags_days:
                cur.execute("""SELECT report_instance_id FROM report_instance
                               WHERE report_id=? AND tags=? AND 
                               report_instance_id > ? AND report_instance_id < ?
                               LIMIT 1""",
                            [report_id, list(day_tags),
                             util.min_uuid_with_dt(datetime.datetime.combine(day,
                                                                             datetime.datetime.min.time())),
                             util.max_uuid_with_dt(datetime.datetime.combine(day,
                                                                             datetime.datetime.max.time()))])
                if not cur.fetchall():
                    cur.execute("""DELETE FROM report_instance_day
                                   WHERE report_id=? AND tags=? AND day=?""",
                                [report_id, list(day_tags), day])


            ### Delete tags for which report instances no longer exist

            tags_present = set()
            for tags, _ in tags_days:
                for tag in tags:
                    tags_present.add(tag)

            for tag in tags_present:
                cur.execute("""SELECT report_id FROM report_instance_day
                               WHERE report_id=? AND tags=?
                               LIMIT 1""",
                            [report_id, [tag]])
                if cur.fetchall():
                    continue
                cur.execute("""DELETE FROM report_tag
                               WHERE report_id=? AND tag=?""",
                            [report_id, tag])


            return len(ris), [list(ts) for ts in all_tags_subsets]


    def select_report_instance_count_for_owner(self, owner_id):
        with cursor() as cur:
            cur.execute("""SELECT report_instance_count FROM report_data_for_owner
                           WHERE owner_id=?""", [owner_id])
            row = cur.fetchone()
            return row['report_instance_count'] if row else 0

    def select_report_instance_diskspace_for_owner(self, owner_id):
        with cursor() as cur:
            cur.execute("""SELECT report_instance_diskspace FROM report_data_for_owner
                           WHERE owner_id=?""", [owner_id])
            row = cur.fetchone()
            return row['report_instance_diskspace'] if row else 0



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
