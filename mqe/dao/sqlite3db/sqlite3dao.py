import logging
import sqlite3
import datetime

from mqe import serialize
from mqe import util
from mqe.dao.daobase import *
from mqe.dao.sqlite3db.sqlite3util import connect, closing_cursor as cursor, insert, replace, in_params
from mqe.dbutil import gen_timeuuid, gen_uuid
from mqe import mqeconfig


log = logging.getLogger('mqe.dao.sqlite3dao')


def initialize():
    connect()


def postprocess_tags(row):
    if row:
        if 'tags' in row and row['tags'] is None:
            row['tags'] = []
        if 'all_tags' in row and row['all_tags'] is None:
            row['all_tags'] = []
    return row


class Sqlite3DashboardDAO(DashboardDAO):

    def select(self, owner_id, dashboard_id):
        with cursor() as cur:
            cur.execute("""SELECT * FROM dashboard
                           WHERE owner_id=? AND dashboard_id=?""",
                        [owner_id, dashboard_id])
            return cur.fetchone()


    def select_all(self, owner_id):
        with cursor() as cur:
            cur.execute("""SELECT * FROM dashboard WHERE owner_id=?""",
                        [owner_id])
            return cur.fetchall()


    def insert(self, owner_id, dashboard_name, dashboard_options):
        dashboard_id = gen_uuid()
        row = dict(owner_id=owner_id,
                   dashboard_id=dashboard_id,
                   dashboard_name=dashboard_name,
                   dashboard_options=dashboard_options)
        with cursor() as cur:
            cur.execute(*insert('dashboard', row))
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


class Sqlite3LayoutDAO(LayoutDAO):

    def select(self, owner_id, dashboard_id,
               columns=('layout_id', 'layout_def', 'layout_props')):
        what = ', '.join(columns)
        with cursor() as cur:
            cur.execute("""SELECT {what}
                           FROM dashboard_layout
                           WHERE owner_id=? AND dashboard_id=?""".format(what=what),
                        [owner_id, dashboard_id])
            return cur.fetchone()

    def select_multi(self, owner_id, dashboard_id_list,
               columns=('layout_id', 'layout_def', 'layout_props')):
        if 'dashboard_id' not in columns:
            columns += 'dashboard_id',
        what = ', '.join(columns)

        with cursor() as cur:
            cur.execute("""SELECT {what}
                           FROM dashboard_layout
                           WHERE owner_id=? AND dashboard_id IN {in_p}"""\
                        .format(what=what, in_p=in_params(dashboard_id_list)),
                        [owner_id] + dashboard_id_list)
            return cur.fetchall()

    def set(self, owner_id, dashboard_id, old_layout_id, new_layout_id,
            new_layout_def, new_layout_props):
        with cursor() as cur:
            if old_layout_id is None:
                try:
                    cur.execute(*insert('dashboard_layout', dict(
                        owner_id=owner_id,
                        dashboard_id=dashboard_id,
                        layout_def=new_layout_def,
                        layout_props=new_layout_props,
                        layout_id=new_layout_id,
                    )))
                except sqlite3.IntegrityError:
                    return False
                else:
                    return True

            cur.execute("""UPDATE dashboard_layout
                   SET layout_id=?, layout_def=?, layout_props=?
                   WHERE owner_id=? AND dashboard_id=? AND layout_id=?""",
                        [new_layout_id, new_layout_def, new_layout_props,
                         owner_id, dashboard_id, old_layout_id])
            return cur.rowcount == 1


    def delete(self, owner_id, dashboard_id):
        with cursor() as cur:
            cur.execute("""DELETE FROM dashboard_layout
                           WHERE owner_id=? AND dashboard_id=?""",
                        [owner_id, dashboard_id])


    def insert_layout_by_report_multi(self, owner_id, report_id_list, tags, label, dashboard_id,
                                      layout_id):
        tags = tags or []
        with cursor() as cur:
            qs = """REPLACE INTO layout_by_report (owner_id, report_id, tags, label, dashboard_id, layout_id) VALUES (?, ?, ?, ?, ?, ?)"""
            params_list = []
            for report_id in report_id_list:
                params_list.append([owner_id, report_id, tags, label, dashboard_id, layout_id])
            cur.executemany(qs, params_list)


    def delete_layout_by_report(self, owner_id, report_id, tags, label, dashboard_id,
                                layout_id):
        tags = tags or []
        with cursor() as cur:
            cur.execute("""DELETE FROM layout_by_report
                           WHERE owner_id=? AND report_id=? AND tags=?
                           AND label=? AND dashboard_id=?
                           AND layout_id=?""",
                        [owner_id, report_id, tags,
                         label, dashboard_id, layout_id])


    def select_layout_by_report_multi(self, owner_id, report_id, tags, label, limit):
        with cursor() as cur:
            cur.execute("""SELECT * FROM layout_by_report
                           WHERE owner_id=? AND report_id=? AND tags=?
                           AND label=? LIMIT ?""",
                          [owner_id, report_id, tags, label, limit])
            return cur.fetchall()



class Sqlite3TileDAO(TileDAO):

    def select_multi(self, dashboard_id, tile_id_list):
        with cursor() as cur:
            cur.execute("""SELECT * FROM tile WHERE dashboard_id=?
                           AND tile_id IN {in_p}""".format(in_p=in_params(tile_id_list)),
                        [dashboard_id] + tile_id_list)
            return cur.fetchall()


    def insert_multi(self, owner_id, dashboard_id, tile_options_list):
        qs = """INSERT INTO tile (dashboard_id, tile_id, tile_options)
                VALUES (?, ?, ?)"""
        params_list = []
        res = []
        for tile_options in tile_options_list:
            row = dict(dashboard_id=dashboard_id,
                       tile_id=gen_timeuuid(),
                       tile_options=tile_options)
            params_list.append([row['dashboard_id'], row['tile_id'], row['tile_options']])
            res.append(row)
        with cursor() as cur:
            cur.executemany(qs, params_list)
        return res

    def delete_multi(self, tile_list):
        qs = """DELETE FROM tile WHERE dashboard_id=? AND tile_id=?"""
        params_list = [[tile.dashboard_id, tile.tile_id] for tile in tile_list]
        with cursor() as cur:
            cur.executemany(qs, params_list)


class Sqlite3ReportDAO(ReportDAO):

    def select(self, report_id):
        with cursor() as cur:
            cur.execute("""SELECT * FROM report WHERE report_id=?""", [report_id])
            return cur.fetchone()


    def select_multi(self, owner_id, report_id_list):
        with cursor() as cur:
            cur.execute("""SELECT * FROM report
                           WHERE report_id IN {in_p}""".format(in_p=in_params(report_id_list)),
                        report_id_list)
            return cur.fetchall()


    def select_or_insert(self, owner_id, report_name):
        row = self.select_by_name(owner_id, report_name)
        if row:
            return False, row

        with cursor() as cur:
            report_id = gen_timeuuid()
            cur.execute("""INSERT INTO report (report_id, report_name, owner_id)
                           SELECT ?, ?, ? WHERE NOT EXISTS
                           (SELECT 1 FROM report WHERE owner_id=? AND report_name=?)""",
                        [report_id, report_name, owner_id, owner_id, report_name])
            if cur.lastrowid:
                return True, {'owner_id': owner_id, 'report_name': report_name,
                              'report_id': report_id}
            return False, self.select_by_name(owner_id, report_name)

    def select_by_name(self, owner_id, report_name):
        with cursor() as cur:
            cur.execute("""SELECT * FROM report WHERE owner_id=? AND report_name=?""",
                        [owner_id, report_name])
            return cur.fetchone()


    def select_ids_by_name_prefix_multi(self, owner_id, name_prefix, after_name, limit):
        if name_prefix is None:
            name_prefix = ''
        q = """SELECT report_id, report_name FROM report
               WHERE owner_id=? AND report_name LIKE ?"""
        args = [owner_id, '%s%%' % name_prefix]
        if after_name is not None:
            q += """ AND report_name > ?"""
            args.append(after_name)
        q += """ ORDER BY report_name LIMIT ?"""
        args.append(limit)
        with cursor() as cur:
            cur.execute(q, args)
            return [row['report_id'] for row in cur.fetchall()]


    def insert(self, owner_id, report_name):
        inserted, row = self.select_or_insert(owner_id, report_name)
        if not inserted:
            return None
        return row

    def delete(self, owner_id, report_id):
        row = self.select(report_id)
        if not row:
            log.warn('No report row %s', report_id)
            return

        with cursor() as cur:
            cur.execute("""DELETE FROM report WHERE report_id=?""", [report_id])
            cur.execute("""DELETE FROM report_tag WHERE report_id=?""", [report_id])


    def select_report_instance_count(self, owner_id, report_id):
        with cursor() as cur:
            cur.execute("""SELECT report_instance_count FROM report
                           WHERE owner_id=? AND report_id=?""",
                        [owner_id, report_id])
            return cur.fetchone()['report_instance_count']


    def select_report_instance_diskspace(self, owner_id, report_id):
        with cursor() as cur:
            cur.execute("""SELECT report_instance_diskspace FROM report
                           WHERE owner_id=? AND report_id=?""",
                        [owner_id, report_id])
            return cur.fetchone()['report_instance_diskspace']


    def select_report_instance_days(self, report_id, tags):
        with cursor() as cur:
            cur.execute("""SELECT day FROM report_instance_day
                               WHERE report_id=? AND tags=?""",
                              [report_id, tags])
            dates = [row['day'] for row in cur.fetchall()]
            return [util.datetime_from_date(d) for d in dates]


    def select_tags_sample(self, report_id, tag_prefix, limit, after_tag):
        with cursor() as cur:
            cur.execute("""SELECT tag FROM report_tag
                           WHERE report_id=? AND tag > ? AND tag LIKE ? LIMIT ?""",
                        [report_id, after_tag, '%s%%' % tag_prefix, limit])
            return [r['tag'] for r in cur.fetchall()]



class Sqlite3ReportInstanceDAO(ReportInstanceDAO):

    def _compute_ri_diskspace(self, row):
        if not row['input_string']:
            return 0
        return len(row['input_string'])


    def insert(self, owner_id, report_id, report_instance_id, tags, ri_data, input_string,
               extra_ri_data, custom_created):
        created = util.datetime_from_uuid1(report_instance_id)

        with cursor() as cur:
            first_row = None
            tags_powerset = util.powerset(tags[:mqeconfig.MAX_TAGS])
            for tags_subset in tags_powerset:
                row = dict(report_id=report_id,
                           tags=tags_subset,
                           report_instance_id=report_instance_id,
                           ri_data=ri_data,
                           input_string=input_string,
                           all_tags=tags,
                           extra_ri_data=extra_ri_data)
                if first_row is None:
                    first_row = row
                cur.execute(*insert('report_instance', row))

                cur.execute("""INSERT OR IGNORE INTO report_instance_day (report_id, tags, day)
                               VALUES (?, ?, ?)""",
                            [report_id, tags_subset, created.date()])

            if first_row:
                # report counts

                cur.execute("""UPDATE report SET
                               report_instance_count = report_instance_count + 1
                               WHERE report_id=?""", [report_id])

                diskspace = self._compute_ri_diskspace(first_row)
                cur.execute("""UPDATE report SET
                               report_instance_diskspace = report_instance_diskspace + ?
                               WHERE report_id=?""", [diskspace, report_id])

                # owner counts
                cur.execute("""SELECT 1 FROM report_data_for_owner WHERE owner_id=?""",
                            [owner_id])
                if not cur.fetchone():
                    try:
                        cur.execute("""INSERT INTO report_data_for_owner (owner_id)
                                       VALUES (?)""",
                                    [owner_id])
                    except sqlite3.IntegrityError:
                        pass

                cur.execute("""UPDATE report_data_for_owner
                               SET report_instance_count=report_instance_count+1
                               WHERE owner_id=?""",
                            [owner_id])

                cur.execute("""UPDATE report_data_for_owner
                               SET report_instance_diskspace=report_instance_diskspace+?
                               WHERE owner_id=?""",
                            [diskspace, owner_id])

                for tag in tags:
                    cur.execute("""INSERT OR IGNORE INTO report_tag (report_id, tag)
                                   VALUES (?, ?)""",
                                [report_id, tag])

            return first_row


    def select_extra_ri_data(self, report_id, report_instance_id):
        with cursor() as cur:
            cur.execute("""SELECT extra_ri_data FROM report_instance
                           WHERE report_id=? AND tags=? AND report_instance_id=?""",
                        [report_id, [], report_instance_id])
            row = cur.fetchone()
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
                           ORDER BY report_instance_id {order} LIMIT ?""".\
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



class Sqlite3SeriesDefDAO(SeriesDefDAO):

    def select_multi(self, report_id, tags_series_id_list):
        res = []
        with cursor() as cur:
            for tags, series_id in tags_series_id_list:
                cur.execute("""SELECT * FROM series_def
                               WHERE report_id=? AND tags=? AND series_id=?""",
                            [report_id, tags, series_id])
                res.append(postprocess_tags(cur.fetchone()))
        return res

    def select_id_or_insert_multi(self, report_id, tags_series_spec_list):
        res = []
        with cursor() as cur:
            for (tags, series_spec) in tags_series_spec_list:
                cur.execute("""SELECT series_id FROM series_def
                               WHERE series_spec=? AND report_id=? AND tags=?""",
                            [serialize.mjson(series_spec), report_id, tags])
                row = cur.fetchone()
                if row:
                    res.append(row['series_id'])
                    continue

                series_id = gen_timeuuid()
                cur.execute("""INSERT INTO series_def (report_id, tags, series_id, series_spec, from_rid, to_rid)
                               SELECT ?, ?, ?, ?, ?, ? WHERE NOT EXISTS
                               (SELECT 1 FROM series_def
                                WHERE report_id=? AND tags=? AND series_id=?)""",
                            [report_id, tags, series_id, serialize.mjson(series_spec),
                             None, None, report_id, tags, series_id])
                if cur.lastrowid:
                    res.append(series_id)
                    continue

                cur.execute("""SELECT series_id FROM series_def
                               WHERE series_spec=? AND report_id=? AND tags=?""",
                            [serialize.mjson(series_spec), report_id, tags])
                res.append(cur.fetchone()['series_id'])
            return res

    def insert_multi(self, report_id, tags_series_spec_list):
        res = []
        with cursor() as cur:
            for tags, series_spec in tags_series_spec_list:
                series_id = gen_timeuuid()
                cur.execute(*insert('series_def', dict(
                    report_id=report_id,
                    tags=tags,
                    series_id=series_id,
                    series_spec=serialize.mjson(series_spec))))
                res.append(series_id)
            return res

    def update_from_rid_to_rid(self, report_id, series_id, tags, from_rid=undefined,
                               to_rid=undefined):
        q = """UPDATE series_def
               SET {from_rid_clause} {to_rid_clause}
              WHERE report_id=? AND tags=? AND series_id=?"""
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

        params += [report_id, tags, series_id]

        with cursor() as cur:
            cur.execute(q.format(**fmt), params)


    def clear_all_series_defs(self, report_id, tags_powerset):
        with cursor() as cur:
            cur.execute("""UPDATE series_def SET from_rid=NULL, to_rid=NULL
                           WHERE report_id=? AND tags IN {in_p}""".format(in_p=in_params(tags_powerset)),
                        [report_id] + tags_powerset)

            rows = cur.execute("""SELECT series_id FROM series_def
                                  WHERE report_id=? AND tags IN {in_p}""".format(
                                        in_p=in_params(tags_powerset)),
                               [report_id] + tags_powerset)
            for row in rows:
                cur.execute("""DELETE FROM series_value WHERE series_id=?""", [row['series_id']])


class Sqlite3SeriesValueDAO(SeriesValueDAO):

    def insert_multi(self, series_id, data_it):
        q = """INSERT OR IGNORE INTO series_value (series_id, report_instance_id, json_value, header) VALUES (?, ?, ?, ?)"""
        params_list = []
        for d in data_it:
            params_list.append([series_id, d['report_instance_id'], d['json_value'],
                               d.get('header')])
        with cursor() as cur:
            cur.executemany(q, params_list)

    def select_multi(self, series_id, min_report_instance_id, max_report_instance_id, limit):
        q = """SELECT report_instance_id, json_value, header
                                 FROM series_value
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

        with cursor() as cur:
            cur.execute(q.format(**fmt), params)
            return cur.fetchall()

    def select_latest_id(self, series_id):
        with cursor() as cur:
            cur.execute("""SELECT report_instance_id
                           FROM series_value
                           WHERE series_id=?
                           ORDER BY report_instance_id DESC
                           LIMIT 1""")
            row = cur.fetchone()
            return row['report_instance_id'] if row else None


class Sqlite3OptionsDAO(OptionsDAO):

    def select_multi(self, report_id, kind, key_list):
        with cursor() as cur:
            q = """SELECT options_key, options_value FROM options
                           WHERE report_id=? AND kind=? AND options_key IN {in_p}""".\
                format(in_p=in_params(key_list))
            cur.execute(q, [report_id, kind] + key_list)
            return [(row['options_key'], row['options_value']) for row in cur.fetchall()]

    def set_multi(self, report_id, kind, key_value_list):
        with cursor() as cur:
            for (key, value) in key_value_list:
                cur.execute(*replace('options', dict(
                    report_id=report_id,
                    kind=kind,
                    options_key=key,
                    options_value=value
                )))
