import uuid
import logging
import json
import sys
from contextlib import contextmanager
from collections import OrderedDict

from mqe import reports
from mqe import util
from mqe import mqeconfig
from mqe import layouts
from mqe.tiles import Tile


report_instances_data = {
    'points': [
        [
            OrderedDict([('user_name', 'john'), ('is_active', True), ('points', 128)]),
            OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 210)]),
        ],
        [
            OrderedDict([('user_name', 'john'), ('is_active', False), ('points', 133)]),
            OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 220)]),
        ],
        [
            OrderedDict([('user_name', 'monique'), ('is_active', True), ('points', 265)]),
            OrderedDict([('user_name', 'john'), ('is_active', False), ('points', 133)]),
            OrderedDict([('user_name', 'mike'), ('is_active', True), ('points', 0)]),
        ],
        [
            OrderedDict([('user_name', 'mike'), ('is_active', True), ('points', 130)]),
            OrderedDict([('user_name', 'peter'), ('is_active', True), ('points', 132)]),
        ],
    ],
}

class ReportData(object):
    """A class holding a set of objects needed to create a dashboard - a report, an owner id etc. The ``name`` parameter must be a key in the global ``report_instances_data`` dict."""

    def __init__(self, name, tags=[]):
        assert name in report_instances_data

        self.name = name
        self.tags = tags
        self.owner_id = uuid.uuid1()
        self.dashboard_id = uuid.uuid1()
        self.report = reports.Report.select_or_insert(self.owner_id, name)
        self.instances = []

        for data in report_instances_data[self.name]:
            ipres = self.report.process_input(json.dumps(data), tags=self.tags)
            assert ipres.report_instance is not None
            self.instances.append(ipres.report_instance)

    def only_tile_from_layout(self):
        layout = layouts.Layout.select(self.owner_id, self.dashboard_id)
        assert len(layout.layout_dict) == 1
        tile = Tile.select(self.dashboard_id, layout.layout_dict.keys()[0])
        assert tile
        return tile

    def layout(self):
        layout = layouts.Layout.select(self.owner_id, self.dashboard_id)
        return layout

    def tile_from_layout(self, idx, expected_layout_size=None):
        layout = layouts.Layout.select(self.owner_id, self.dashboard_id)
        if expected_layout_size is not None:
            assert len(layout.layout_dict) == expected_layout_size
        ids = sorted(layout.layout_dict.keys(), key=lambda ud: ud.time)
        tile = Tile.select(self.dashboard_id, ids[idx])
        assert tile
        return tile


class CustomData(object):
    """Similar to :class:`ReportData`, but requires passing a list of input string
    from which report instances will be created"""

    def __init__(self, in_strs, tags=[]):
        self.tags = tags
        self.name = uuid.uuid4().hex

        self.owner_id = uuid.uuid1()
        self.dashboard_id = uuid.uuid1()
        self.report = reports.Report.select_or_insert(self.owner_id, self.name)
        self.instances = []

        for input_string in in_strs:
            if not isinstance(input_string, (str, unicode)):
                input_string = json.dumps(input_string)
            ipres = self.report.process_input(input_string, tags=self.tags)
            assert ipres.report_instance is not None
            self.instances.append(ipres.report_instance)


@util.cache()
def report_data(name):
    """The cached :class:`ReportData` objects"""
    return ReportData(name)

def new_report_data(name, tags=[]):
    return ReportData(name, tags)

def call(test_method, *args, **kwargs):
    """Call a method of an external :class:`.TestCase` class"""
    cls = test_method.im_class
    test_case = cls(test_method.__name__)
    return test_method(test_case, *args, **kwargs)

@contextmanager
def patch(old_module, old_fun, new_fun):
    """Simple mocking"""
    new_fun.old_fun = old_fun
    setattr(old_module, old_fun.__name__,  new_fun)
    yield
    setattr(old_module, old_fun.__name__,  new_fun.old_fun)
    del new_fun.old_fun



def enable_logging(debug=True, queries=True):
    logging.getLogger().propagate = False
    if queries:
        mqeconfig.DEBUG_QUERIES = True
    hdlr = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    def add(l, level):
        l.addHandler(hdlr)
        l.setLevel(level)
    add(logging.getLogger('mqe'), logging.DEBUG if debug else logging.INFO)
    add(logging.getLogger('mqeweb'), logging.DEBUG if debug else logging.INFO)
    add(logging.getLogger('mqeapi'), logging.DEBUG if debug else logging.INFO)

    logging.getLogger('mqe.pars').setLevel(logging.WARN)

def disable_logging():
    mqeconfig.DEBUG_QUERIES = False
    for logger in ('mqe', 'mqeweb', 'mqeapi'):
        logging.getLogger(logger).setLevel(logging.CRITICAL)

@contextmanager
def logenabled(debug=True, cql=True):
    enable_logging(debug, cql)
    yield
    disable_logging()
