import tutorial
from tutorial import SECTION
from mqe.dataseries import SeriesSpec
from mqe.reports import Report

def main():
    vars = tutorial.main()
    owner_id = vars['owner_id']
    owner_dashboards = vars['owner_dashboards']
    dashboard = vars['dashboard']
    points_report = vars['points_report']


    SECTION('Label-less report instances')


    numbers = Report.select_or_insert(owner_id, 'numbers')
    input = """\
    10 20 30
    40 50 60
    """
    res = numbers.process_input(input)

    series_spec = SeriesSpec(1, -1, {'op': 'eq', 'args': ['1']})
    print series_spec.get_cell(res.report_instance)


    metric = Report.select_or_insert(owner_id, 'metric')
    res = metric.process_input('32.4')

    series_spec = SeriesSpec(0, -1, {'op': 'eq', 'args': ['0']})
    print series_spec.get_cell(res.report_instance)


    SECTION('Handling reordering of columns')


    input = """\
    user_name is_active points
    john      true      128
    monique   true      210
    """
    res = points_report.process_input(input, force_header=[0])

    series_spec = SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']})
    print series_spec.get_cell(res.report_instance)

    input_2 = """\
    user_name points is_active
    john      128    true     
    monique   210    true     
    """
    res_2 = points_report.process_input(input_2, force_header=[0])
    print series_spec.get_cell(res_2.report_instance)

    series_spec.promote_colnos_to_headers(res.report_instance)
    print series_spec.get_cell(res_2.report_instance)


    SECTION('A lower-level interface to data series')


    from mqe.dataseries import SeriesDef, get_series_values
    from datetime import datetime

    series_spec = SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']})
    series_id = SeriesDef.insert(points_report.report_id, [], series_spec)
    series_def = SeriesDef.select(points_report.report_id, [], series_id)
    series_values = get_series_values(series_def, points_report, from_dt=datetime(2017, 1, 1),
                                                                 to_dt=datetime(2018, 1, 1))
    point_values = [sv.value for sv in series_values]


if __name__ == '__main__':
    main()
