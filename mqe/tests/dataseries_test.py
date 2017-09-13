import unittest
import json
import datetime
from datetime import timedelta
import uuid

from mqe.dataseries import SeriesSpec, update_default_options, select_default_series_spec_options
from mqe import reports
from mqetables.enrichment import EnrichedValue
from mqe import dataseries
from mqe.dataseries import guess_series_spec
from mqe.util import dictwithout, MIN_UUID

from mqe.tests.tutil import report_data, CustomData, call
from mqe.tests import tiles_test

utcnow = datetime.datetime.utcnow


class SeriesSpecTest(unittest.TestCase):

    def test_get_cell_nopromote(self):
        rd = report_data('points')
        ss = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        self.assertEqual(2, ss.actual_data_colno(rd.instances[0]))
        self.assertEqual(0, ss.actual_filtering_colno(rd.instances[0]))

        self.assertEqual(210, ss.get_cell(rd.instances[0]).value)
        self.assertEqual(220, ss.get_cell(rd.instances[1]).value)
        self.assertEqual(265, ss.get_cell(rd.instances[2]).value)
        self.assertIsNone(ss.get_cell(rd.instances[3]))

    def test_get_cell_promote(self):
        rd = report_data('points')
        ss = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        ss.promote_colnos_to_headers(rd.instances[0])
        self.assertEqual(2, ss.actual_data_colno(rd.instances[0]))
        self.assertEqual(0, ss.actual_filtering_colno(rd.instances[0]))

        self.assertEqual(210, ss.get_cell(rd.instances[0]).value)
        self.assertEqual(220, ss.get_cell(rd.instances[1]).value)
        self.assertEqual(265, ss.get_cell(rd.instances[2]).value)
        self.assertIsNone(ss.get_cell(rd.instances[3]))

        rep2 = reports.Report.insert(report_data('points').owner_id, 'points_colchange')
        res2 = rep2.process_input(json.dumps([
            dict(points=300, user_name='john'),
            dict(points=400, user_name='monique'),
        ]))
        self.assertEqual(0, ss.actual_data_colno(res2.report_instance))
        self.assertEqual(1, ss.actual_filtering_colno(res2.report_instance))
        self.assertEqual(400, ss.get_cell(res2.report_instance).value)

        ss = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        self.assertIsNone(ss.get_cell(res2.report_instance))

    def test_name(self):
        ss = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        self.assertEqual('monique', ss.name())
        ss.set_name('xxx')
        self.assertEqual('xxx', ss.name())
        ss.params['static_name'] = 'yyy'
        self.assertEqual('xxx', ss.name())

        ss = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        ss.params['static_name'] = 'yyy'
        self.assertEqual('yyy', ss.name())

        ss = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        ss.set_name('xxx')
        self.assertEqual('xxx', ss.name(True))
        ss.set_name('')
        self.assertEqual('monique', ss.name(True))

    def test_name_virtual(self):
        ri = report_data('points').instances[-1]
        ss = SeriesSpec(2, -1, dict(op='eq', args=['1']))
        self.assertEqual('col. 2 (1)', ss.name())

        ss.promote_colnos_to_headers(ri)
        self.assertEqual('points (1)', ss.name())


class GuessSeriesSpecTest(unittest.TestCase):
    maxDiff = None


    def test_label_score(self):
        ls = dataseries._label_score
        self.assertEqual(0, ls(EnrichedValue('3')))
        self.assertEqual(0, ls(EnrichedValue('4%')))
        self.assertEqual(0, ls(EnrichedValue('.32')))
        self.assertEqual(0, ls(EnrichedValue('0')))
        self.assertEqual(0, ls(EnrichedValue('')))
        self.assertEqual(0, ls(EnrichedValue(' ')))
        self.assertEqual(0, ls(EnrichedValue('\n')))
        self.assertEqual(0, ls(EnrichedValue('23 GB')))
        self.assertEqual(0, ls(EnrichedValue('23GB')))
        self.assertEqual(1, ls(EnrichedValue('dfd')))
        self.assertEqual(0.9, ls(EnrichedValue('1000 kg')))
        self.assertEqual(0.5, ls(EnrichedValue('2012-02-01')))

    def __test_label_score_2(self):
        def p(val):
            ev = EnrichedValue(val)
            score = dataseries._label_score(ev)
            print '%s got %s' % (val, score)

        p('796 KB/64 MB')
        p('796KB/64MB')
        p('796 KB / 64MB ')

        p('redis1')
        p('redis 11')
        p('r/1000')
        p('r /1000')
        p('r / 1000')
        p('1000/r')
        p('1000 /r')
        p('1000 / r')

    def series_specs_equal(self, ss1, ss2):
        def params(ss):
            return dictwithout(ss.params, 'data_column_header_for_name', 'static_name')
        self.assertDictEqual(params(ss1), params(ss2))

    def test_guess_series_spec(self):
        ss = guess_series_spec(report_data('points').report, report_data('points').instances[0], 2, 2)
        ss_expected = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        ss_expected.promote_colnos_to_headers(report_data('points').instances[0])
        self.series_specs_equal(ss_expected, ss)

        ss = guess_series_spec(report_data('points').report, report_data('points').instances[0], 2, 0)
        ss_expected = SeriesSpec(0, -1, dict(op='eq', args=['2']))
        ss_expected.promote_colnos_to_headers(report_data('points').instances[0])
        self.series_specs_equal(ss_expected, ss)

        cd = CustomData([10, 20])
        ss = guess_series_spec(cd.report, cd.instances[-1], 0, 0)
        ss_expected = SeriesSpec(0, -1, dict(op='eq', args=['0']))
        ss_expected.promote_colnos_to_headers(cd.instances[-1])
        self.series_specs_equal(ss_expected, ss)

        cd = CustomData([ [1,2,3], [4,5,6] ])
        ss = guess_series_spec(cd.report, cd.instances[-1], 2, 0)
        ss_expected = SeriesSpec(0, -1, dict(op='eq', args=['2']))
        ss_expected.promote_colnos_to_headers(cd.instances[-1])
        self.series_specs_equal(ss_expected, ss)

        cd = CustomData([ [["aaa", 10], ["bbb", 20]] ])
        ss = guess_series_spec(cd.report, cd.instances[-1], 1, 1)
        ss_expected = SeriesSpec(1, 0, dict(op='eq', args=['bbb']))
        ss_expected.promote_colnos_to_headers(cd.instances[-1])
        self.series_specs_equal(ss_expected, ss)

        cd = CustomData([ [["aaa", 10]] ])
        ss = guess_series_spec(cd.report, cd.instances[-1], 0, 1)
        ss_expected = SeriesSpec(1, 0, dict(op='eq', args=['aaa']))
        ss_expected.promote_colnos_to_headers(cd.instances[-1])
        self.series_specs_equal(ss_expected, ss)


class GetSeriesValuesTest(unittest.TestCase):

    def test_series_def_select_or_insert(self):
        ss = SeriesSpec(2, 0, dict(op='eq', args=['monique']))
        ss.promote_colnos_to_headers(report_data('points').instances[-1])
        sd_id = dataseries.SeriesDef.select_id_or_insert(report_data('points').report.report_id, [], ss)
        sd = dataseries.SeriesDef.select(report_data('points').report.report_id, [], sd_id)
        self.assertEqual([], sd.tags)

        sd2_id = dataseries.SeriesDef.select_id_or_insert(report_data('points').report.report_id, [], ss)
        sd2 = dataseries.SeriesDef.select(report_data('points').report.report_id, [], sd2_id)
        self.assertEqual(sd, sd2)
        return sd

    def test_get_series_values(self):
        sd = self.test_series_def_select_or_insert()
        res = dataseries.get_series_values(sd, report_data('points').report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual([210, 220, 265], [sv.value for sv in res])
        self.assertEqual(['points', 'points', 'points'], [sv.header for sv in res])

        res = dataseries.get_series_values(sd, report_data('points').report, datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(seconds=1), 1000)
        self.assertEqual([], res)

        res = dataseries.get_series_values(sd, report_data('points').report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 2)
        self.assertEqual([220, 265], [sv.value for sv in res])
        self.assertEqual(['points', 'points'], [sv.header for sv in res])

    def test_get_series_values_multiple_inserts(self):
        cd = CustomData(range(20))
        sd_id = dataseries.SeriesDef.select_id_or_insert(cd.report.report_id, [], dataseries.guess_series_spec(cd.report, cd.instances[0], 0, 0))
        sd = dataseries.SeriesDef.select(cd.report.report_id, [], sd_id)
        res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual(range(20), [sv.value for sv in res])

        cd.report.process_input('200')
        sd = dataseries.SeriesDef.select(cd.report.report_id, [], sd_id)
        res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual(range(20) + [200], [sv.value for sv in res])

        cd.report.process_input('200')
        sd = dataseries.SeriesDef.select(cd.report.report_id, [], sd_id)
        res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual(range(20) + [200, 200], [sv.value for sv in res])

        cd.report.process_input('80', created=datetime.datetime.utcnow() - datetime.timedelta(seconds=10))
        sd = dataseries.SeriesDef.select(cd.report.report_id, [], sd_id)
        res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual([80] + range(20) + [200, 200], [sv.value for sv in res])

        ri = cd.report.process_input('200').report_instance
        sd = dataseries.SeriesDef.select(cd.report.report_id, [], sd_id)
        res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual([80] + range(20) + [200, 200, 200], [sv.value for sv in res])

        cd.report.process_input('300', created=ri.created - datetime.timedelta(milliseconds=1))
        sd = dataseries.SeriesDef.select(cd.report.report_id, [], sd_id)
        res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual([80] + range(20) + [200, 200, 300, 200], [sv.value for sv in res])

        # tags
        cd.report.process_input('400', created=ri.created - datetime.timedelta(milliseconds=2), tags=['aaa'])
        sd = dataseries.SeriesDef.select(cd.report.report_id, [], sd_id)
        res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
        self.assertEqual([80] + range(20) + [200, 200, 400, 300, 200], [sv.value for sv in res])

    def test_creating_ri_reversed_and_fetching_series_values_in_the_middle(self):
        owner_id = uuid.uuid1()
        r = reports.Report.insert(owner_id, 'nums')
        sd_id = dataseries.SeriesDef.insert(r.report_id, [],
                                dataseries.SeriesSpec(0, -1, {'op': 'eq', 'args': ['0']}))
        sd = dataseries.SeriesDef.select(r.report_id, [], sd_id)

        for i in reversed(xrange(10, 20)):
            r.process_input(str(i), created=utcnow()-timedelta(days=i))
        expected = list(reversed(range(10, 20)))
        values = [sv.value for sv in dataseries.get_series_values(
            sd, r, utcnow()-timedelta(days=100), utcnow())]
        self.assertEqual(expected, values)

        for i in reversed(xrange(0, 10)):
            r.process_input(str(i), created=utcnow()-timedelta(days=i))

        sd = dataseries.SeriesDef.select(r.report_id, [], sd_id)
        expected = list(reversed(range(0, 20)))
        values = [sv.value for sv in dataseries.get_series_values(
            sd, r, utcnow()-timedelta(days=100), utcnow())]
        self.assertEqual(expected, values)

    def test_get_series_values_tags(self):
        cd = CustomData(range(20), tags=['t1', 't2'])

        def values(tags):
            sd_id = dataseries.SeriesDef.select_id_or_insert(cd.report.report_id, tags, dataseries.guess_series_spec(cd.report, cd.instances[0], 0, 0))
            sd = dataseries.SeriesDef.select(cd.report.report_id, tags, sd_id)
            res = dataseries.get_series_values(sd, cd.report, datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow(), 1000)
            return [sv.value for sv in res]

        for tags in ([], ['t1'], ['t2'], ['t1', 't2']):
            self.assertEqual(range(20), values(tags))

        self.assertEqual([], values(['t1', 't2', 't3']))

        ri = cd.report.process_input('9', tags=['t2']).report_instance

        self.assertEqual(range(20) + [9], values([]))
        self.assertEqual(range(20), values(['t1']))
        self.assertEqual(range(20) + [9], values(['t2']))
        self.assertEqual(range(20), values(['t1', 't2']))
        self.assertEqual([], values(['t1', 't2', 't3']))

        cd.report.process_input('15', created=ri.created - datetime.timedelta(milliseconds=1), tags=['t1'])

        self.assertEqual(range(20) + [15, 9], values([]))
        self.assertEqual(range(20) + [15], values(['t1']))
        self.assertEqual(range(20) + [9], values(['t2']))
        self.assertEqual(range(20), values(['t1', 't2']))
        self.assertEqual([], values(['t1', 't2', 't3']))

        cd.report.process_input('99', created=cd.instances[10].created - datetime.timedelta(microseconds=1), tags=['t1', 't2'])
        self.assertEqual(range(10) + [99] + range(10, 20), values(['t1', 't2']))
        self.assertEqual(range(10) + [99] + range(10, 20) + [15], values(['t1']))

    def test_get_series_values_after(self):
        cd = CustomData(range(20), tags=['t1'])

        def values(tags, after):
            sd_id = dataseries.SeriesDef.select_id_or_insert(cd.report.report_id, tags, dataseries.guess_series_spec(cd.report, cd.instances[0], 0, 0))
            sd = dataseries.SeriesDef.select(cd.report.report_id, tags, sd_id)
            res = dataseries.get_series_values_after(sd, cd.report, after)
            return [sv.value for sv in res]

        self.assertEqual(range(20), values(['t1'], MIN_UUID))
        self.assertEqual(range(20), values([], MIN_UUID))
        self.assertEqual([19], values([], cd.instances[-2].report_instance_id))
        self.assertEqual([], values(['t2'], cd.instances[-2].report_instance_id))
        self.assertEqual([19], values(['t1'], cd.instances[-2].report_instance_id))
        self.assertEqual([], values([], cd.instances[-1].report_instance_id))

        cd.report.process_input('50', tags=['t1'], created=cd.instances[10].created - datetime.timedelta(microseconds=1))
        self.assertEqual(range(10) + [50] + range(10, 20), values([], MIN_UUID))
        self.assertEqual(range(10) + [50] + range(10, 20), values(['t1'], MIN_UUID))
        self.assertEqual([], values(['t1', 't2', 't3'], MIN_UUID))

    def test_same_dt(self):
        owner_id = uuid.uuid1()
        r = reports.Report.insert(owner_id, 'rname')

        sd_id = dataseries.SeriesDef.select_id_or_insert(r.report_id, [],
                        dataseries.SeriesSpec(0, -1, {'op': 'eq', 'args': ['0']}))
        sd = dataseries.SeriesDef.select(r.report_id, [], sd_id)

        for i in range(10):
            r.process_input(str(i), created=datetime.datetime(2017, 5, 5))

        values = dataseries.get_series_values(sd, r, datetime.datetime(2017, 5, 1), utcnow())
        self.assertEqual(10, len(values))
        self.assertEqual(set(range(10)), set([v.value for v in values]))

        values = dataseries.get_series_values(sd, r, datetime.datetime(2017, 5, 5), utcnow())
        self.assertEqual(10, len(values))
        self.assertEqual(set(range(10)), set([v.value for v in values]))

        values = dataseries.get_series_values(sd, r, datetime.datetime(2017, 5, 5),
                                                     datetime.datetime(2017, 5, 5))
        self.assertEqual(10, len(values))
        self.assertEqual(set(range(10)), set([v.value for v in values]))

        values = dataseries.get_series_values(sd, r, datetime.datetime(2017, 5, 5, 0, 0, 0, 1),
                                                     datetime.datetime(2017, 5, 5, 0, 0, 0, 1))
        self.assertEqual(0, len(values))


class DefaultOptionsTest(unittest.TestCase):

    def test_hashing_ss(self):
        ss_list = [SeriesSpec(i, 0, {'op': 'eq', 'args': [str(i)]}) for i in range(10)]
        for i, ss in enumerate(ss_list):
            ss.set_name('name_%d' % i)
        d = {ss: i for i, ss in enumerate(ss_list)}
        for i, ss in enumerate(ss_list):
            self.assertEqual(i, d[ss])

    def test_colors(self):
        tile = call(tiles_test.TileTest.test_insert, tile_options_ext={'colors': ['red', 'blue']})
        update_default_options(tile)
        default_options = select_default_series_spec_options(tile.report_id, tile.series_specs())
        self.assertEqual([{'color': 'red'}, {'color': 'blue'}], default_options)

        tile2 = call(tiles_test.TileTest.test_insert)
        default_options = select_default_series_spec_options(tile2.report_id, tile2.series_specs())
        self.assertEqual([{'color': 'red'}, {'color': 'blue'}], default_options)

        tile3 = call(tiles_test.TileTest.test_insert, tile_options_ext={'colors': ['pink']})
        update_default_options(tile3)

        default_options = select_default_series_spec_options(tile3.report_id, tile3.series_specs())
        self.assertEqual([{'color': 'pink'}, {'color': 'blue'}], default_options)

        tile4 = call(tiles_test.TileTest.test_insert)
        update_default_options(tile4)
        default_options = select_default_series_spec_options(tile4.report_id, tile4.series_specs())
        self.assertEqual([{'color': 'pink'}, {'color': 'blue'}], default_options)

    def test_names(self):
        tile = call(tiles_test.TileTest.test_insert)
        self.assertNotIn('name', select_default_series_spec_options(tile.report_id, tile.series_specs())[0])

        tile_config = tile.get_tile_config()
        for i, ss in enumerate(tile_config['series_spec_list']):
            ss.set_name('serie%d' % i)
        tile = tile.insert_similar(tile_config)
        update_default_options(tile)
        self.assertEqual('serie0', select_default_series_spec_options(tile.report_id, tile.series_specs())[0]['name'])
        self.assertEqual('serie1', select_default_series_spec_options(tile.report_id, tile.series_specs())[1]['name'])

        tile_config = tile.get_tile_config()
        for i, ss in enumerate(tile_config['series_spec_list']):
            ss.set_name('s%d' % i)
        tile = tile.insert_similar(tile_config)
        update_default_options(tile)
        self.assertEqual('s0', select_default_series_spec_options(tile.report_id, tile.series_specs())[0]['name'])
        self.assertEqual('s1', select_default_series_spec_options(tile.report_id, tile.series_specs())[1]['name'])

