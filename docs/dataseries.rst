Data series
===========

While the examples shown in the :ref:`tutorial <tutorial>` and other chapters show how |SeriesSpec| objects describing data series can be created and their data retrieved by calling :meth:`~.Tile.get_tile_data`, the library also offers a lower-level interface not involving tiles and allows customizing the |SeriesSpec| objects.


Label-less report instances
---------------------------
When a report instance contains natural labels for numeric values, like in the :ref:`points_report <tutorial_points_report>` where the ``user_name`` column contains labels for the ``points`` column, it's easy to specify the column numbers in the |SeriesSpec| constructor. But what if we create a report instance without any labels? For example::

    numbers = Report.select_or_insert(owner_id, 'numbers')
    input = """\
    10 20 30
    40 50 60
    """
    res = numbers.process_input(numbers)

A |SeriesSpec| supports passing ``-1`` as the ``filtering_colno`` argument to its constructor which signifies a virtual column containing row indexes. The ``filtering_expr`` can then specify the wanted row index. In our case, if we wanted to extract the number ``50``, we should write::

    series_spec = SeriesSpec(1, -1, {'op': 'eq', 'args': ['1']})

    print series_spec.get_cell(res.report_instance)

    > Cell(rowno=1, colno=1, value=u'50')

Worth noting is that the ``args`` key of the ``filtering_expr`` must always be a list of *strings*.

The example also demonstrates usage of the :meth:`.get_cell` method, which can be used for manually extracting a value from a report instance using a |SeriesSpec|.

The special case of single-cell tables, which can be thought of as traditional metrics, should be also handled by using the virtual column::

    metric = Report.select_or_insert(owner_id, 'metric')
    res = metric.process_input('32.4')

    series_spec = SeriesSpec(0, -1, {'op': 'eq', 'args': ['0']})
    print series_spec.get_cell(res.report_instance)

    > Cell(rowno=0, colno=0, value=32.4)


Handling reordering of columns
------------------------------
In the default setup a |SeriesSpec| uses column *numbers* as its specification. A problem will arise when the columns will be reordered or new columns will be inserted in the middle. For example, we can extract a value from the regular ``points_reports`` instance in the old way::

    input = """\
    user_name is_active points
    john      true      128
    monique   true      210
    """
    res = points_report.process_input(input, force_header=[0])

    series_spec = SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']})
    print series_spec.get_cell(res.report_instance)

    > Cell(rowno=2, colno=2, value=u'210')

But when the ``user_name`` and the ``points`` columns will be replaced, the meant value will not be extracted::

    input_2 = """\
    user_name points is_active
    john      128    true
    monique   210    true
    """
    res_2 = points_report.process_input(input_2, force_header=[0])
    print series_spec.get_cell(res_2.report_instance)

    > Cell(rowno=2, colno=2, value=u'true')

To overcome the problem, a |SeriesSpec| can be converted to use column *headers* instead of column numbers by calling :meth:`.promote_colnos_to_headers`::

    series_spec.promote_colnos_to_headers(res.report_instance)

    print series_spec.get_cell(res_2.report_instance)

    > Cell(rowno=2, colno=1, value=u'210')

The method, based on headers present in the :class:`.ReportInstance` object passed as an argument, promotes the originally specified column numbers to the headers.

One thing that should be noted is that we have passed the ``force_header`` argument to the :meth:`.process_input` method. The argument marks the given indexes as header rows. And although the automatic detection of a header has worked for our inputs, the :meth:`.promote_colnos_to_headers` method will do the promotion only if it's sure the header is correctly set. For whitespace-separated tables the header detection can be wrong, and it's why we needed to specify the header explicitly. For many other inputs, like JSON and even ASCII tables, the algorithm detecting a header can specify a sure header on its own.


A lower-level interface to data series
--------------------------------------

When data series are managed through the :data:`tile_config` mechanism, the job of creating series definitions in the database and inserting series values is done automatically. Sometimes it's useful to access the data series directly.

The class :class:`.SeriesDef` represents a |SeriesSpec| with a set of :class:`.SeriesValue` objects available for a range of report instances (defined by the attributes :attr:`.from_rid` and :attr:`.to_rid`). We can create a :class:`.SeriesDef` and fetch its values in the following way::

    from mqe.dataseries import SeriesDef, get_series_values
    from datetime import datetime

    series_spec = SeriesSpec(2, 0, {'op': 'eq', 'args': ['monique']})
    series_id = SeriesDef.insert(points_report.report_id, [], series_spec)
    series_def = SeriesDef.select(points_report.report_id, [], series_id)
    series_values = get_series_values(series_def, points_report, from_dt=datetime(2017, 1, 1),
                                                                 to_dt=datetime(2018, 1, 1))
    point_values = [sv.value for for sv in series_values]

If the selection of series values should be based on report instance IDs instead of datetimes, the function :func:`.get_series_values_after` should be used instead of :func:`.get_series_values`.

The :attr:`.SeriesDef.series_id` attribute is available in the full :data:`tile_options` under :data:`tile_options.series_configs` attribute.

