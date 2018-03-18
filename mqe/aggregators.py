from __future__ import division

import logging

from mqe import serialize, mqeconfig, util, c


log = logging.getLogger('mqe.aggregators')


@serialize.json_type('AggregatorSpec')
class AggregatorSpec(object):

    def __init__(self, agg_name, window_seconds):
        self.agg_name = agg_name
        self.window_seconds = window_seconds

    def for_json(self):
        return {'agg_name': self.agg_name, 'window_seconds': self.window_seconds}

    @staticmethod
    def from_rawjson(obj):
        return AggregatorSpec(obj['agg_name'], obj['window_seconds'])


class Aggregator(object):
    agg_name = None

    def __init__(self, window_seconds):
        self.window_seconds = window_seconds

    def insert_series_values(self, series_def, report, from_dt, to_dt, after=None, limit=None,
                             latest_instance_id=None):
        raise NotImplementedError()


class NopAggregator(Aggregator):
    agg_name = 'nop'

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


class WindowedAggregator(Aggregator):

    def _window_start(self, dt):
        ts = util.datetime_to_timestamp(dt)
        window_start_ts = (ts // self.window_seconds) * self.window_seconds
        return util.datetime_from_timestamp(window_start_ts)

    def insert_series_values(self, series_def, report, from_dt, to_dt, after=None, limit=None,
                             latest_instance_id=None):
        series_def.to_rid



AGGREGATOR_CLASSES = [NopAggregator]

_AGGREGATOR_CLASS_BY_AGG_NAME = {cls.agg_name: cls for cls in AGGREGATOR_CLASSES}

def create_aggregator(agg_spec):
    if not agg_spec:
        return NopAggregator(0)
    if agg_spec.agg_name not in _AGGREGATOR_CLASS_BY_AGG_NAME:
        raise ValueError('No aggregator %r found' % agg_spec.agg_name)
    return _AGGREGATOR_CLASS_BY_AGG_NAME[agg_spec.agg_name](agg_spec.window_seconds)