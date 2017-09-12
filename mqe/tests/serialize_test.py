import unittest
import uuid
import datetime
import json

from mqe import serialize
from mqe.dataseries import SeriesSpec
from mqe import util


class SerializeTest(unittest.TestCase):
    maxDiff = None

    def test_mjson(self):
        self.assertRaises(TypeError, lambda: serialize.mjson(object()))

        self.assertEqual("""{"a":{"b":[1,2,3]}}""", serialize.mjson({'a': {'b': [1, 2, 3]}}))

    def test_custom_types(self):
        d = {
            'id': uuid.uuid1(),
            'id2': uuid.uuid4(),
            'dt': datetime.datetime.utcnow(),
            'da': datetime.datetime.utcnow(),
            'ss': SeriesSpec(2, 0, dict(op='eq', args=['monique'])),
        }
        self.assertEqual(d, serialize.json_loads(serialize.mjson(d)))

        dt = datetime.datetime.utcnow()
        self.assertEqual(util.datetime_from_date(dt.date()), serialize.json_loads(serialize.mjson(dt.date())))

        ext_d = json.loads(serialize.json_dumps_external(util.dictwithout(d, 'ss')))
        self.assertEqual(d['id2'].hex, ext_d['id2'])

    def test_registering_custom_class(self):
        @serialize.json_type('A')
        class A(object):
            def __init__(self):
                self.a = 10
            def for_json(self):
                return {'a': self.a}
            @classmethod
            def from_rawjson(cls, x):
                return A()

        doc = serialize.json_loads(serialize.json_dumps({'obj': A()}))
        self.assertIsInstance(doc['obj'], A)
