import logging
from collections import defaultdict

import cassandra.util
import datetime
from cassandra import ConsistencyLevel

from mqe import c
from mqe import serialize
from mqe.dao.cassandradb.cassandrautil import insert, execute_lwt, day_text, bind, dt_from_day_text
from mqe.dao.daobase import *
from mqe.dbutil import gen_uuid, gen_timeuuid
from mqe import util
from mqe.util import undefined
from mqe import mqeconfig

log = logging.getLogger('mqe.dao.cassandra')


REPORT_INSERT_TRIES = 20
REPORT_REFETCH_TRIES = 40


COLUMN_RENAMES = defaultdict(dict)


def initialize():
    from mqe.dao.cassandradb.cassandrautil import Cassandra
    c.cass = Cassandra()

def tags_repr_from_tags(tags):
    if tags is None:
        return ''
    assert isinstance(tags, list)
    return ','.join(sorted(tags))

def tags_from_tags_repr(tags_repr):
    if not tags_repr:
        return []
    return sorted(tags_repr.split(','))

def postprocess_tags(row):
    if row:
        if 'all_tags_repr' in row:
            row['all_tags'] = tags_from_tags_repr(row['all_tags_repr'])
            del row['all_tags_repr']
        if 'tags_repr' in row:
            row['tags'] = tags_from_tags_repr(row['tags_repr'])
            del row['tags_repr']
    return row

def postprocess_col_renames(renames, row):
    for k, v in renames.items():
        if v in row:
            row[k] = row[v]
            del row[v]

def what_from_columns(columns, table):
    if not columns:
        return '*'
    colnames = [COLUMN_RENAMES[table].get(col, col) for col in columns]
    return ', '.join(colnames)


def invalidate_precomputed_instances_data(report_id, tags_powerset, first_valid_dt):
    log.debug('Invalidating precomputed %s %s %s', report_id, tags_powerset, first_valid_dt)
    all_tags_reprs = [tags_repr_from_tags(ts) for ts in tags_powerset]
    series_rows = c.cass.execute("""SELECT * FROM mqe.series_def
                                  WHERE report_id=? AND tags_repr IN ?""",
                                 [report_id, all_tags_reprs])

    qs = []
    for row in series_rows:
        if row['from_rid'] is None:
            continue
        if cassandra.util.datetime_from_uuid1(row['from_rid']) < first_valid_dt and \
                        cassandra.util.datetime_from_uuid1(row['to_rid']) < first_valid_dt:
            log.debug('Clearing series_id=%s', row['series_id'])
            new_from_rid = None
            new_to_rid = None
        elif cassandra.util.datetime_from_uuid1(row['from_rid']) >= first_valid_dt and \
                        cassandra.util.datetime_from_uuid1(row['to_rid']) >= first_valid_dt:
            continue
        else:
            log.debug('Updating from_rid_dt=%s', first_valid_dt)
            new_from_rid = cassandra.util.min_uuid_from_time(first_valid_dt)
            new_to_rid = row['to_rid']

        qs.append(bind("""UPDATE mqe.series_def SET from_rid=?, to_rid=?
                          WHERE report_id=? AND tags_repr=? AND series_id=?""",
                       [new_from_rid, new_to_rid, report_id, row['tags_repr'], row['series_id']]))
        if new_from_rid is None:
            qs.append(bind("""DELETE FROM mqe.series_value WHERE series_id=?""",
                           [row['series_id']]))
        else:
            qs.append(bind("""DELETE FROM mqe.series_value WHERE series_id=? AND report_instance_id < ?""",
                           [row['series_id'], new_from_rid]))
    c.cass.execute_parallel(qs)




class CassReportDAO(ReportDAO):

    def select(self, report_id):
        rows = c.cass.execute("""SELECT * FROM mqe.report WHERE report_id=?""", [report_id])
        return rows[0] if rows else None

    def select_multi(self, owner_id, report_id_list):
        return c.cass.execute("""SELECT * FROM mqe.report
                                 WHERE report_id IN ?""", [report_id_list])

    def select_or_insert(self, owner_id, report_name):
        row = self.select_by_name(owner_id, report_name)
        if row:
            return False, row

        for i in xrange(REPORT_INSERT_TRIES):
            row = self.insert(owner_id, report_name)
            if row:
                return True, row

        for i in xrange(REPORT_REFETCH_TRIES):
            log.warn('No existing report and could not create, retrying fetching')
            row = self.select_by_name(owner_id, report_name)
            if row:
                return False, row

        log.warn('select_or_insert failed')
        return False, None

    def select_by_name(self, owner_id, report_name):
        report_id = c.cass.execute_fst("""SELECT report_id FROM mqe.report_by_owner_id_report_name
                                          WHERE owner_id=? AND report_name=?""",
                                       [owner_id, report_name])['report_id']
        if report_id is None:
            return None
        return self.select(report_id)


    def select_ids_by_name_prefix_multi(self, owner_id, name_prefix, after_name, limit):
        if name_prefix is None:
            name_prefix = ''
        q = """SELECT report_name FROM mqe.report_name_by_owner_id_report_name_prefix
               WHERE owner_id=? AND report_name_prefix=?"""
        args = [owner_id, name_prefix] # type: list
        if after_name is not None:
            q += """ AND report_name > ?"""
            args.append(after_name)
        q += """ ORDER BY report_name_prefix, report_name LIMIT ?"""
        args.append(limit)
        report_names = [r['report_name'] for r in c.cass.execute(q, args)]

        id_rows = c.cass.execute("""SELECT report_id, report_name FROM mqe.report_by_owner_id_report_name
                                  WHERE owner_id=? AND report_name IN ?""",
                                 [owner_id, report_names])
        id_by_name = {r['report_name']: r['report_id'] for r in id_rows}
        ids = [id_by_name[rn] for rn in report_names]
        return ids


    def insert(self, owner_id, report_name):
        log.debug('Trying to insert new report %r', report_name)
        report_id = gen_timeuuid()
        def insert_report_name():
            return c.cass.execute("""INSERT INTO mqe.report_by_owner_id_report_name (owner_id, report_name, report_id)
                                   VALUES (?, ?, ?)
                                   IF NOT EXISTS""",
                                  [owner_id, report_name, report_id])
        lwt_res = execute_lwt(insert_report_name)
        if lwt_res == False:
            log.info('Race condition in creating a new report %r', report_name)
            return None
        elif lwt_res == None:
            rows = c.cass.execute("""SELECT report_id FROM mqe.report_by_owner_id_report_name
                                   WHERE owner_id=? AND report_name=? /* SERIAL */""",
                                  [owner_id, report_name],
                                  ConsistencyLevel.SERIAL)
            if not rows or rows[0]['report_id'] != report_id:
                log.info('Race condition in creating a new report when lwt_res==None: %r',
                         report_name)
                return None


        row =  {'report_id': report_id, 'report_name': report_name, 'owner_id': owner_id}
        c.cass.execute(insert('mqe.report', row))

        log.info('Inserted new report report_id=%s report_name=%r', report_id, report_name)

        qs = []
        for prefix in util.iter_prefixes(report_name, include_empty=True):
            qs.append(insert('mqe.report_name_by_owner_id_report_name_prefix',
                             dict(owner_id=owner_id,
                                  report_name_prefix=prefix,
                                  report_name=report_name)))
        c.cass.execute_parallel(qs)

        return row

    def delete(self, owner_id, report_id):
        row = self.select(report_id)
        if not row:
            log.warn('No report row %s', report_id)
            return

        qs = []

        qs.append(bind("""DELETE FROM mqe.report WHERE report_id=?""", [report_id]))
        qs.append(bind("""DELETE FROM mqe.report_by_owner_id_report_name
                          WHERE owner_id=? AND report_name=?""", [owner_id, row['report_name']]))
        for prefix in util.iter_prefixes(row['report_name'], include_empty=True):
            qs.append(bind("""DELETE FROM mqe.report_name_by_owner_id_report_name_prefix
                              WHERE owner_id=? AND report_name_prefix=? AND report_name=?""",
                           [owner_id, prefix, row['report_name']]))
        qs.append(bind("""DELETE FROM mqe.report_tag WHERE report_id=?""", [report_id]))

        c.cass.execute_parallel(qs)

    def select_report_instance_count(self, owner_id, report_id):
        return c.cass.execute_fst("""SELECT count FROM mqe.report_instance_count
                                     WHERE report_id=? AND tags_repr=''""",
                                  [report_id])['count'] or 0

    def select_report_instance_diskspace(self, owner_id, report_id):
        return c.cass.execute_fst("""SELECT bytes FROM mqe.report_instance_diskspace
                                     WHERE report_id=? AND tags_repr=''""",
                                  [report_id])['bytes'] or 0

    def select_report_instance_days(self, report_id, tags):
        rows = c.cass.execute("""SELECT day FROM mqe.report_instance_day
                               WHERE report_id=? AND tags_repr=?""",
                              [report_id, tags_repr_from_tags(tags)])
        return [dt_from_day_text(r['day']) for r in rows]

    def select_tags_sample(self, report_id, tag_prefix, limit):
        rows = c.cass.execute("""SELECT tag FROM mqe.report_tag
                                 WHERE report_id=? AND tag_prefix=? LIMIT ?""",
                              [report_id, tag_prefix, limit])
        return [r['tag'] for r in rows]




class CassReportInstanceDAO(ReportInstanceDAO):

    def _compute_ri_diskspace(self, row):
        if not row['input_string']:
            return 0
        return len(row['input_string'])


    def insert(self, owner_id, report_id, report_instance_id, tags, ri_data, input_string,
               extra_ri_data, custom_created):
        created = util.datetime_from_uuid1(report_instance_id)
        day = day_text(created)
        all_tags_repr = tags_repr_from_tags(tags)

        qs = []

        metadata_row = dict(
            report_id=report_id,
            day=day,
            report_instance_id=report_instance_id,
            all_tags_repr=all_tags_repr,
            inserted=datetime.datetime.utcnow(),
        )
        if extra_ri_data:
            metadata_row['extra_ri_data'] = extra_ri_data
        qs.append(insert('mqe.report_instance_metadata', metadata_row))

        first_row = None
        tags_powerset = util.powerset(tags[:mqeconfig.MAX_TAGS])
        for tags_subset in tags_powerset:
            tags_repr = tags_repr_from_tags(tags_subset)
            row = dict(report_id=report_id,
                       day=day,
                       tags_repr=tags_repr,
                       report_instance_id=report_instance_id,
                       ri_data=ri_data,
                       input_string=input_string,
                       all_tags_repr=all_tags_repr)
            if first_row is None:
                first_row = row
            qs.append(insert('mqe.report_instance', row, COLUMN_RENAMES['report_instance']))

            if not c.cass.execute("""SELECT day FROM mqe.report_instance_day
                                   WHERE report_id=? AND tags_repr=? AND day=?""",
                                  [report_id, tags_repr, day]):
                qs.append(insert('mqe.report_instance_day',
                                 dict(report_id=report_id,
                                      tags_repr=tags_repr,
                                      day=day)))

            qs.append(bind("""UPDATE mqe.report_instance_count SET count=count+1
                              WHERE report_id=? AND tags_repr=?""",
                           [report_id, tags_repr]))

            diskspace = self._compute_ri_diskspace(row)
            qs.append(bind("""UPDATE mqe.report_instance_diskspace SET bytes=bytes+?
                              WHERE report_id=? AND tags_repr=?""",
                           [diskspace, report_id, tags_repr]))

        ### queries for all tags
        qs.append(bind("""UPDATE mqe.report_instance_count_for_owner SET count=count+1
                          WHERE owner_id=?""",
                       [owner_id]))
        if first_row:
            diskspace = self._compute_ri_diskspace(first_row)
            qs.append(bind("""UPDATE mqe.report_instance_diskspace_for_owner SET bytes=bytes+?
                              WHERE owner_id=?""",
                           [diskspace, owner_id]))

        # avoid reinserting the same tag multiple times
        tag_rows = c.cass.execute("""SELECT tag FROM mqe.report_tag
                                     WHERE report_id=? AND tag_prefix='' AND tag IN ?""",
                                  [report_id, tags])
        tags_from_rows = {row['tag'] for row in tag_rows}
        for tag in tags:
            if tag in tags_from_rows:
                continue
            for p in util.iter_prefixes(tag, include_empty=True):
                qs.append(insert('mqe.report_tag',
                                 dict(report_id=report_id,
                                      tag_prefix=p,
                                      tag=tag)))

        c.cass.execute_parallel(qs)

        return postprocess_tags(first_row)


    def select_extra_ri_data(self, report_id, report_instance_id):
        return c.cass.execute_fst("""SELECT extra_ri_data FROM mqe.report_instance_metadata
                                     WHERE report_id=? AND day=? AND report_instance_id=?""",
              [report_id, day_text(report_instance_id), report_instance_id])['extra_ri_data']


    def select(self, report_id, report_instance_id, tags):
        rows = c.cass.execute("""SELECT * FROM mqe.report_instance
                                 WHERE report_id=? AND day=? AND tags_repr=?
                                 AND report_instance_id=?""",
                              [report_id, day_text(report_instance_id),
                               tags_repr_from_tags(tags), report_instance_id])
        if not rows:
            return None
        postprocess_tags(rows[0])
        postprocess_col_renames(COLUMN_RENAMES['report_instance'], rows[0])
        return rows[0]


    def select_multi(self, report_id, tags, min_report_instance_id, max_report_instance_id,
                     columns, order, limit):
        tags_repr = tags_repr_from_tags(tags)

        what = what_from_columns(columns, 'report_instance')

        q_tpl = """SELECT day FROM mqe.report_instance_day
                   WHERE report_id=? AND tags_repr=? {filter_min} {filter_max}"""
        params = [report_id, tags_repr]

        if min_report_instance_id is not None:
            filter_min = 'AND day >= ?'
            params.append(day_text(min_report_instance_id))
        else:
            filter_min = ''

        if max_report_instance_id is not None:
            filter_max = 'AND day <= ?'
            params.append(day_text(max_report_instance_id))
        else:
            filter_max = ''

        day_rows = c.cass.execute(q_tpl.format(filter_min=filter_min, filter_max=filter_max),
                                  params)
        days = [row['day'] for row in day_rows]
        if order == 'desc':
            days = list(reversed(days))

        res = []
        for day in days:
            day_instances = list(c.cass.execute(
                """SELECT {what} FROM mqe.report_instance
                   WHERE report_id=? AND day=? AND tags_repr=?
                   AND report_instance_id > ? AND report_instance_id < ?
                   ORDER BY report_instance_id {order} LIMIT ?""". \
                    format(what=what, order=order),
                [report_id, day, tags_repr,
                 min_report_instance_id, max_report_instance_id,
                 limit]
            ))
            res.extend(day_instances)
            if len(res) >= limit:
                break

        res = res[:limit]
        for row in res:
            postprocess_tags(row)
            postprocess_col_renames(COLUMN_RENAMES['report_instance'], row)
        return res


    def select_latest_id(self, report_id, tags):
        latest_day = c.cass.execute_fst("""SELECT day FROM mqe.report_instance_day
            WHERE report_id=? AND tags_repr=? ORDER BY day DESC LIMIT 1""",
                                        [report_id, tags_repr_from_tags(tags)])['day']
        if not latest_day:
            return None
        return c.cass.execute_fst("""SELECT report_instance_id FROM mqe.report_instance
                                     WHERE report_id=? AND day=? AND tags_repr=?
                                     ORDER BY report_instance_id DESC LIMIT 1""",
                 [report_id, latest_day, tags_repr_from_tags(tags)])['report_instance_id']

    def delete(self, owner_id, report_id, report_instance_id, update_counters):
        ri = self.select(report_id, report_instance_id, [])
        if not ri:
            return 0, []
        return self._delete_ris(owner_id, report_id, ri['all_tags'], [ri], update_counters)

    def delete_multi(self, owner_id, report_id, tags, min_report_instance_id,
                     max_report_instance_id, limit, update_counters, use_insertion_datetime):
        columns = ['report_instance_id', 'all_tags_repr', 'day']
        if update_counters:
            columns.append('input_string')
        ris = self.select_multi(report_id, tags, min_report_instance_id, max_report_instance_id,
                                columns, 'asc', limit)
        log.info('Selected %d report instances to delete', len(ris))

        if use_insertion_datetime:
            days = util.uniq_sameorder(ri['day'] for ri in ris)
            rim_rows = c.cass.execute("""SELECT report_instance_id, inserted
                                     FROM mqe.report_instance_metadata
                                     WHERE report_id=? AND day IN ?
                                     AND report_instance_id > ? AND report_instance_id < ?""",
                                      [report_id, days, min_report_instance_id,
                                       max_report_instance_id])
            assert len(rim_rows) == len(ris)

            ri_by_rid = {ri['report_instance_id']: ri for ri in ris}
            from_dt = util.datetime_from_uuid1(min_report_instance_id)
            to_dt = util.datetime_from_uuid1(max_report_instance_id)
            for rim in rim_rows:
                if not (from_dt <= rim['inserted'] <= to_dt):
                    del ri_by_rid[rim['report_instance_id']]
            ris = ri_by_rid.values()

        return self._delete_ris(owner_id, report_id, tags, ris, update_counters)

    def _delete_ris(self, owner_id, report_id, tags, ris, update_counters):

        qs = []
        count_by_tags_repr = defaultdict(int)
        diskspace_by_tags_repr = defaultdict(int)
        tags_reprs_days = set()
        for ri in ris:
            qs.append(bind("""DELETE FROM mqe.report_instance_metadata
                              WHERE report_id=? AND day=? AND report_instance_id=?""",
                           [report_id, ri['day'], ri['report_instance_id']]))
            for tags_subset in util.powerset(ri['all_tags']):
                tags_repr = tags_repr_from_tags(tags_subset)
                qs.append(bind("""DELETE FROM mqe.report_instance
                                  WHERE report_id=? AND day=? AND tags_repr=?
                                  AND report_instance_id=?""",
                               [report_id, ri['day'], tags_repr, ri['report_instance_id']]))
                count_by_tags_repr[tags_repr] += 1
                if update_counters:
                    diskspace_by_tags_repr[tags_repr] += self._compute_ri_diskspace(ri)
                tags_reprs_days.add((tags_repr, ri['day']))

        if update_counters:
            qs.append(bind("""UPDATE mqe.report_instance_count_for_owner
                              SET count=count-?
                              WHERE owner_id=?""",
                           [count_by_tags_repr[''], owner_id]))
            qs.append(bind("""UPDATE mqe.report_instance_diskspace_for_owner
                              SET bytes=bytes-?
                              WHERE owner_id=?""",
                           [diskspace_by_tags_repr[''], owner_id]))

            for tags_repr, count in count_by_tags_repr.iteritems():
                qs.append(bind("""UPDATE mqe.report_instance_count
                                  SET count=count-?
                                  WHERE report_id=? AND tags_repr=?""",
                               [count, report_id, tags_repr]))
            for tags_repr, bytes in diskspace_by_tags_repr.iteritems():
                qs.append(bind("""UPDATE mqe.report_instance_diskspace
                                  SET bytes=bytes-?
                                  WHERE report_id=? AND tags_repr=?""",
                               [bytes, report_id, tags_repr]))

        c.cass.execute_parallel(qs)

        ### Delete days for which report instances no longer exist

        days_qs = {}
        for tags_repr, day in tags_reprs_days:
            days_qs[(tags_repr, day)] = bind("""SELECT report_instance_id FROM mqe.report_instance
                                                WHERE report_id=? AND day=? AND tags_repr=?
                                                LIMIT 1""",
                                             [report_id, day, tags_repr])
        days_res = c.cass.execute_parallel(days_qs)

        qs = []
        for (tags_repr, day), rows in days_res.iteritems():
            if rows:
                continue
            qs.append(bind("""DELETE FROM mqe.report_instance_day
                              WHERE report_id=? AND tags_repr=? AND day=?""",
                           [report_id, tags_repr, day]))
        log.info('Deleting %s days', len(qs))
        c.cass.execute_parallel(qs)

        return len(ris), [tags_from_tags_repr(tr) for tr in count_by_tags_repr]


    def select_report_instance_count_for_owner(self, owner_id):
        return c.cass.execute_fst("""SELECT count FROM mqe.report_instance_count_for_owner
                                     WHERE owner_id=?""",
                                  [owner_id])['count'] or 0

    def select_report_instance_diskspace_for_owner(self, owner_id):
        return c.cass.execute_fst("""SELECT bytes FROM mqe.report_instance_diskspace_for_owner
                                     WHERE owner_id=?""",
                              [owner_id])['bytes'] or 0



class CassDashboardDAO(DashboardDAO):

    def select(self, owner_id, dashboard_id):
        rows = c.cass.execute("""SELECT * FROM mqe.dashboard
                                 WHERE owner_id=? AND dashboard_id=?""",
                              [owner_id, dashboard_id])
        return rows[0] if rows else None


    def select_all(self, owner_id):
        return c.cass.execute("""SELECT * FROM mqe.dashboard WHERE owner_id=?""",
                              [owner_id])


    def insert(self, owner_id, dashboard_name, dashboard_options):
        dashboard_id = gen_uuid()
        row = dict(owner_id=owner_id,
            dashboard_id=dashboard_id,
            dashboard_name=dashboard_name,
            dashboard_options=dashboard_options)
        c.cass.execute(insert('mqe.dashboard', row))
        return row


    def update(self, owner_id, dashboard_id, dashboard_name, dashboard_options):
        if dashboard_name is not None:
            c.cass.execute("""UPDATE mqe.dashboard SET dashboard_name=?
                              WHERE owner_id=? AND dashboard_id=?""",
                           [dashboard_name, owner_id, dashboard_id])
        if dashboard_options is not None:
            c.cass.execute("""UPDATE mqe.dashboard SET dashboard_options=?
                              WHERE owner_id=? AND dashboard_id=?""",
                           [dashboard_options, owner_id, dashboard_id])

    def delete(self, owner_id, dashboard_id):
        c.cass.execute("""DELETE FROM mqe.dashboard WHERE owner_id=? AND dashboard_id=?""",
                       [owner_id, dashboard_id])

    def select_tile_ids(self, dashboard_id):
        rows = c.cass.execute("""SELECT tile_id FROM mqe.tile WHERE dashboard_id=?""", [dashboard_id])
        return [r['tile_id'] for r in rows]

    def select_all_dashboards_ordering(self, owner_id):
        current_ordering_raw = c.cass.execute_fst("""SELECT dashboard_id_ordering
                                                     FROM mqe.all_dashboards_properties
                                                     WHERE owner_id=?""",
                                                  [owner_id])['dashboard_id_ordering']
        if current_ordering_raw:
            return serialize.json_loads(current_ordering_raw)
        return None

    def set_all_dashboards_ordering(self, owner_id, dashboard_id_list):
        c.cass.execute("""UPDATE mqe.all_dashboards_properties
                          SET dashboard_id_ordering=?
                          WHERE owner_id=?""",
                   [serialize.mjson(dashboard_id_list), owner_id])



class CassSeriesDefDAO(SeriesDefDAO):

    def select_multi(self, report_id, tags_series_id_list):
        qs = []
        for tags, series_id in tags_series_id_list:
            qs.append(bind("""SELECT * FROM mqe.series_def
                              WHERE report_id=? AND tags_repr=? AND series_id=?""",
                           [report_id, tags_repr_from_tags(tags), series_id]))
        res = c.cass.execute_parallel(qs)
        res = [postprocess_tags(rows[0]) if rows else None for rows in res]
        return res

    def select_id_or_insert_multi(self, report_id, tags_series_spec_list):
        select_qs = []
        for (tags, series_spec) in tags_series_spec_list:
            select_qs.append(
                bind("""SELECT series_id FROM mqe.series_def_by_series_spec
                        WHERE report_id=? AND tags_repr=? AND series_spec=?""",
                     [report_id, tags_repr_from_tags(tags),
                      serialize.mjson(series_spec)]))

        select_qs_res = c.cass.execute_parallel(select_qs)

        res = [rows[0]['series_id'] if rows else None for rows in select_qs_res]

        to_insert_idxs = [i for i in xrange(len(tags_series_spec_list))
                          if res[i] is None]
        to_insert_data = [tags_series_spec_list[i] for i in to_insert_idxs]
        insert_res = self.insert_multi(report_id, to_insert_data)
        for i in xrange(len(to_insert_idxs)):
            res[to_insert_idxs[i]] = insert_res[i]

        return res

    def insert_multi(self, report_id, tags_series_spec_list):
        if not tags_series_spec_list:
            return []
        qs = []
        res = []
        for tags, series_spec in tags_series_spec_list:
            series_id = gen_timeuuid()
            qs.append(insert('mqe.series_def', dict(
                report_id=report_id,
                tags_repr=tags_repr_from_tags(tags),
                series_id=series_id,
                series_spec=serialize.mjson(series_spec))))
            qs.append(insert('mqe.series_def_by_series_spec', dict(
                report_id=report_id,
                tags_repr=tags_repr_from_tags(tags),
                series_spec=serialize.mjson(series_spec),
                series_id=series_id)))
            res.append(series_id)
        c.cass.execute_parallel(qs)
        return res

    def update_from_rid_to_rid(self, report_id, series_id, tags, from_rid=undefined,
                               to_rid=undefined):
        q = """UPDATE mqe.series_def
               SET {from_rid_clause} {to_rid_clause}
              WHERE report_id=? AND tags_repr=? AND series_id=?"""
        params = []
        fmt = {}

        if from_rid is not undefined:
            fmt['from_rid_clause'] = 'from_rid=?'
            params.append(from_rid)
        else:
            fmt['from_rid_clause'] = ''

        if to_rid is not undefined:
            fmt['to_rid_clause'] = 'to_rid=?'
            params.append(to_rid)
        else:
            fmt['to_rid_clause'] = ''

        params += [report_id, tags_repr_from_tags(tags), series_id]

        c.cass.execute(q.format(**fmt), params)

    def clear_all_series_defs(self, report_id, tags_powerset):
        all_tags_reprs = [tags_repr_from_tags(ts) for ts in tags_powerset]
        series_rows = c.cass.execute("""SELECT series_id, from_rid, to_rid, tags_repr
                                      FROM mqe.series_def
                                      WHERE report_id=? AND tags_repr IN ?""",
                                     [report_id, all_tags_reprs])
        qs = []
        for row in series_rows:
            if row['from_rid'] is not None or row['to_rid'] is not None:
                qs.append(bind("""UPDATE mqe.series_def SET from_rid=NULL, to_rid=NULL
                                  WHERE report_id=? AND tags_repr=? AND series_id=?""",
                               [report_id, row['tags_repr'], row['series_id']]))
            qs.append(bind("""DELETE FROM mqe.series_value WHERE series_id=?""",
                           [row['series_id']]))
        c.cass.execute_parallel(qs)


class CassSeriesValueDAO(SeriesValueDAO):

    def insert_multi(self, series_id, data):
        qs = []
        for row in data:
            row['series_id'] = series_id
            qs.append(insert('mqe.series_value', row))
        c.cass.execute_parallel(qs)

    def select_multi(self, series_id, min_report_instance_id, max_report_instance_id, limit):
        q = """SELECT report_instance_id, json_value, header
                                 FROM mqe.series_value
                                 WHERE series_id=?
                                 {min_clause}
                                 {max_clause}
                                 ORDER BY report_instance_id DESC
                                 LIMIT ?"""
        params = [series_id]
        fmt = {}

        if min_report_instance_id is not None:
            fmt['min_clause'] = 'AND report_instance_id > ?'
            params.append(min_report_instance_id)
        else:
            fmt['min_clause'] = ''

        if max_report_instance_id is not None:
            fmt['max_clause'] = 'AND report_instance_id < ?'
            params.append(max_report_instance_id)
        else:
            fmt['max_clause'] = ''

        params.append(limit)

        return c.cass.execute(q.format(**fmt), params)


class CassOptionsDAO(OptionsDAO):

    def select_multi(self, report_id, kind, key_list):
        assert kind == 'SeriesSpec'

        rows = c.cass.execute("""SELECT series_spec, default_options
                                 FROM mqe.series_spec_default_options
                                 WHERE report_id=? AND series_spec IN ?""",
                              [report_id, key_list])
        return [(row['series_spec'], row['default_options']) for row in rows]

    def set_multi(self, report_id, kind, key_value_list):
        assert kind == 'SeriesSpec'

        qs = []
        for (key, value) in key_value_list:
            qs.append(insert('mqe.series_spec_default_options', dict(
                report_id=report_id,
                series_spec=key,
                default_options=value,
            )))
        c.cass.execute_parallel(qs)


class CassLayoutDAO(LayoutDAO):

    def select(self, owner_id, dashboard_id,
               columns=('layout_id', 'layout_def', 'layout_props')):
        what = what_from_columns(columns, 'dashboard_layout_def')
        rows = c.cass.execute("""SELECT {what}
                                 FROM mqe.dashboard_layout_def
                                 WHERE owner_id=? AND dashboard_id=?""".format(what=what),
                              [owner_id, dashboard_id])
        if not rows:
            return None
        postprocess_col_renames(COLUMN_RENAMES['dashboard_layout_def'], rows[0])
        return rows[0]

    def set(self, owner_id, dashboard_id, old_layout_id, new_layout_id,
            new_layout_def, new_layout_props):
        layout_id_colname = COLUMN_RENAMES['dashboard_layout_def'].get('layout_id', 'layout_id')

        def do_set_layout():
            if not old_layout_id:
                update_rows = c.cass.execute(insert('mqe.dashboard_layout_def',
                                             dict(owner_id=owner_id,
                                                  dashboard_id=dashboard_id,
                                                  layout_id=new_layout_id,
                                                  layout_def=new_layout_def,
                                                  layout_props=new_layout_props),
                                             COLUMN_RENAMES['dashboard_layout_def'],
                                             if_not_exists=True))
            else:
                update_rows = c.cass.execute(
                    """UPDATE mqe.dashboard_layout_def
                       SET {layout_id_colname}=?, layout_def=?, layout_props=?
                       WHERE owner_id=? AND dashboard_id=?
                       IF {layout_id_colname}=?""".format(layout_id_colname=layout_id_colname),
                    [new_layout_id, new_layout_def, new_layout_props,
                     owner_id, dashboard_id, old_layout_id])
            return update_rows

        lwt_res = execute_lwt(do_set_layout)
        if lwt_res == False:
            log.info('Setting new layout failed on LWT transaction')
            return False
        if lwt_res == None:
            log.info('Setting new layout resulted in unknown LWT result')
            saved_layout_id = c.cass.execute_fst(
                    """SELECT {layout_id_colname} FROM mqe.dashboard_layout_def
                       WHERE owner_id=? AND dashboard_id=? /* SERIAL */""".format(
                            layout_id_colname=layout_id_colname),
                    [owner_id, dashboard_id], ConsistencyLevel.SERIAL)[layout_id_colname]
            if saved_layout_id != new_layout_id:
                log.info('The unknown LWT result resolved to failure')
                return False
            log.info('The unknown LWT result resolved to success')
        return True

    def delete(self, owner_id, dashboard_id):
        c.cass.execute("""DELETE FROM mqe.dashboard_layout_def WHERE owner_id=? AND dashboard_id=?""",
                       [owner_id, dashboard_id])

    def insert_layout_by_report_multi(self, owner_id, report_id_list, tags, label, dashboard_id,
                                layout_id):
        qs = []
        for report_id in report_id_list:
            qs.append(insert('mqe.layout_by_report', dict(
                owner_id=owner_id,
                report_id=report_id,
                tags_repr=tags_repr_from_tags(tags),
                label=label,
                dashboard_id=dashboard_id,
                layout_id=layout_id,
            ), COLUMN_RENAMES['layout_by_report']))
        c.cass.execute_parallel(qs)

    def delete_layout_by_report(self, owner_id, report_id, tags, label, dashboard_id,
                                layout_id):
        layout_id_colname = COLUMN_RENAMES['dashboard_layout_def'].get('layout_id', 'layout_id')

        def do_delete_layout_by_report():
            return c.cass.execute("""DELETE FROM mqe.layout_by_report
                                 WHERE owner_id=? AND report_id=? AND tags_repr=?
                                 AND label=? AND dashboard_id=?
                                 IF {layout_id_colname}=?""".format(layout_id_colname=layout_id_colname),
                                  [owner_id, report_id, tags_repr_from_tags(tags),
                                   label, dashboard_id, layout_id])

        execute_lwt(do_delete_layout_by_report)

    def select_layout_by_report_multi(self, owner_id, report_id, tags, label, limit):
        rows = c.cass.execute("""SELECT * FROM mqe.layout_by_report
                                 WHERE owner_id=? AND report_id=? AND tags_repr=?
                                 AND label=?
                                 LIMIT ?""",
                               [owner_id, report_id, tags_repr_from_tags(tags),
                                label, limit])
        rows = list(rows)
        for row in rows:
            postprocess_tags(row)
            postprocess_col_renames(COLUMN_RENAMES['layout_by_report'], row)
        return rows


class CassTileDAO(TileDAO):

    def select_multi(self, dashboard_id, tile_id_list):
        rows = c.cass.execute("""SELECT * FROM mqe.tile WHERE dashboard_id=?
                                 AND tile_id IN ?""", [dashboard_id, tile_id_list])
        return rows

    def insert_multi(self, owner_id, dashboard_id, tile_options_list):
        qs = []
        res = []
        for tile_options in tile_options_list:
            row = dict(dashboard_id=dashboard_id,
                       tile_id=gen_timeuuid(),
                       tile_options=tile_options)
            qs.append(insert('mqe.tile', row))
            qs.append(bind("""UPDATE mqe.tile_count SET count=count+1 WHERE owner_id=?""",
                           [owner_id]))
            res.append(row)
        c.cass.execute_parallel(qs)
        return res

    def delete_multi(self, tile_list):
        qs = []
        for tile in tile_list:
            if not tile:
                continue
            qs.append(bind("""DELETE FROM mqe.tile WHERE dashboard_id=? AND tile_id=?""",
                           [tile.dashboard_id, tile.tile_id]))
            qs.append(bind("""UPDATE mqe.tile_count SET count=count-1 WHERE owner_id=?""",
                           [tile.owner_id]))
        c.cass.execute_parallel(qs)
