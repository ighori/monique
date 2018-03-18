import logging

import itertools

import datetime

from mqe import mqeconfig, serialize, util, c

log = logging.getLogger('mqe.materializers')


class Materializer(object):

    def insert_series_values(self, series_def, report, from_dt, to_dt,
                             after=None, limit=None, latest_instance_id=None):
        raise NotImplementedError()



class DefaultMaterializer(object):

    def insert_series_values(self, series_def, report, from_dt, to_dt, after=None, limit=None,
                             latest_instance_id=None):
        assert after or (from_dt is not None and to_dt is not None)

        log.debug('insert_series_values report_id=%s sd.from_dt=%s sd.to_dt=%s from_dt=%s'
                  'to_dt=%s after=%s limit=%s', report.report_id, series_def.from_dt,
                  series_def.to_dt, from_dt, to_dt, after, limit)

        instances_it = report.fetch_instances_iter(after=after,
                                                   from_dt=from_dt if not after else None,
                                                   to_dt=to_dt if not after else None,
                                                   limit=limit or mqeconfig.MAX_SERIES_POINTS,
                                                   tags=series_def.tags,
                                                   columns=['report_instance_id', 'ri_data'])
        info = dict(oldest_rid_fetched=None,
                    newest_rid_fetched=None,
                    count=0)

        def rows_it():
            for ri in instances_it:
                if info['oldest_rid_fetched'] is None:
                    info['oldest_rid_fetched'] = ri.report_instance_id
                info['newest_rid_fetched'] = ri.report_instance_id
                info['count'] += 1

                cell = series_def.series_spec.get_cell(ri)
                if cell:
                    row = dict(report_instance_id=ri.report_instance_id,
                               json_value=serialize.mjson(cell.value))
                    header = ri.table.header(cell.colno)
                    if header:
                        row['header'] = header
                    yield row

        c.dao.SeriesValueDAO.insert_multi(series_def.series_id, rows_it())

        if info['count'] == 0:
            return

        log.info('Inserted %d series values report_name=%r series_id=%s',
                 info['count'], report.report_name, series_def.series_id)


        # from_rid stores minimal uuid from dt for which we fetched instances,
        # while to_rid stores an actual latest report_instance_id in the series.
        # However, generally it's not expected to_rid can always be a real report_instance_id
        if from_dt is not None:
            oldest_rid_stored = util.min_uuid_with_dt(from_dt)
        else:
            oldest_rid_stored = info['oldest_rid_fetched']

        if series_def.from_rid is None or \
                util.uuid_lt(oldest_rid_stored, series_def.from_rid):
            log.debug('Updating series_def_id=%s from_rid_dt=%s', series_def.series_id,
                      util.datetime_from_uuid1(oldest_rid_stored))
            series_def.update_from_rid(oldest_rid_stored)

        if series_def.to_rid is None or \
                util.uuid_lt(series_def.to_rid, info['newest_rid_fetched']):
            log.debug('Updating series_def_id=%s to_rid_dt=%s', series_def.series_id,
                      util.datetime_from_uuid1(info['newest_rid_fetched']))
            series_def.update_to_rid(info['newest_rid_fetched'])


class WindowedMaterializer(Materializer):

    def __init__(self, window_length_seconds):
        self.window_length_seconds = window_length_seconds

    def _window_start(self, dt):
        ts = util.datetime_to_timestamp(dt)
        window_start_ts = (ts // self.window_length_seconds) * self.window_length_seconds
        return util.datetime_from_timestamp(window_start_ts)

    def compute_value_header_for_group(self, series_def, instances_it):
        raise NotImplementedError()

    def insert_series_values(self, series_def, report, from_dt, to_dt, after=None, limit=None,
                             latest_instance_id=None):
        assert after or (from_dt is not None and to_dt is not None)

        log.debug('insert_series_values report_id=%s sd.from_dt=%s sd.to_dt=%s from_dt=%s'
                  'to_dt=%s after=%s limit=%s', report.report_id, series_def.from_dt,
                  series_def.to_dt, from_dt, to_dt, after, limit)

        if after is not None:
            effective_from_dt = util.datetime_from_uuid1(after)
            effective_to_dt = datetime.datetime.utcnow()
        else:
            effective_from_dt = from_dt
            effective_to_dt = to_dt

        instances_it = report.fetch_instances_iter(after=after,
                                                   from_dt=from_dt if not after else None,
                                                   to_dt=to_dt if not after else None,
                                                   limit=limit or mqeconfig.MAX_SERIES_POINTS,
                                                   tags=series_def.tags,
                                                   columns=['report_instance_id', 'ri_data'])
        first_ri = util.first(instances_it)
        if not first_ri:
            return

        instances_it = itertools.chain([first_ri], instances_it)
        oldest_rid_fetched = first_ri.report_instance_id


        groups = itertools.groupby(instances_it,
                                   key=lambda ri: self._window_start(ri.created))
        all_but_last_groups, last_group_getter = util.all_but_last(groups)

        def rows_it(groups_it):
            for window_start, group_iter in groups_it:
                group_value, group_header = self.compute_value_header_for_group(series_def, group_iter)
                if group_value is None:
                    continue
                row = dict(report_instance_id=util.uuid_with_dt(window_start),
                           json_value=serialize.mjson(group_value))
                if group_header:
                    row['header'] = group_header
                yield row

        c.dao.SeriesValueDAO.insert_multi(series_def.series_id, rows_it(all_but_last_groups))

        # check if last group should be also processed
        seconds_range = (effective_to_dt - effective_from_dt).total_seconds()

        last_group_window_start, last_group_iter = last_group_getter()
        next_window_start = last_group_window_start + \
                            datetime.timedelta(seconds=self.window_length_seconds)

        if (seconds_range > self.window_length_seconds) and \
           (next_window_start < effective_to_dt):
            c.dao.SeriesValueDAO.insert_multi(series_def.series_id, rows_it(last_group_iter))


        log.info('Inserted series values report_name=%r series_id=%s',
                 report.report_name, series_def.series_id)


        # from_rid stores minimal uuid from dt for which we fetched instances,
        # while to_rid stores an actual latest report_instance_id in the series.
        # However, generally it's not expected to_rid can always be a real report_instance_id
        if from_dt is not None:
            oldest_rid_stored = util.min_uuid_with_dt(from_dt)
        else:
            oldest_rid_stored = oldest_rid_fetched

        if series_def.from_rid is None or \
                util.uuid_lt(oldest_rid_stored, series_def.from_rid):
            log.debug('Updating series_def_id=%s from_rid_dt=%s', series_def.series_id,
                      util.datetime_from_uuid1(oldest_rid_stored))
            series_def.update_from_rid(oldest_rid_stored)

        if series_def.to_rid is None or \
                util.uuid_lt(series_def.to_rid, info['newest_rid_fetched']):
            log.debug('Updating series_def_id=%s to_rid_dt=%s', series_def.series_id,
                      util.datetime_from_uuid1(info['newest_rid_fetched']))
            series_def.update_to_rid(info['newest_rid_fetched'])

