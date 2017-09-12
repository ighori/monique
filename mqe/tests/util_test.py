import unittest

from mqe import util


class CachedPropertyTest(unittest.TestCase):

    def test_simple(self):
        class A(object):
            num = 1

            @util.cached_property
            def f(self):
                A.num += 1
                return A.num

        a = A()
        self.assertEqual(2, a.f)
        self.assertEqual(2, a.f)
        self.assertEqual(2, a.f)

    def test_del(self):
        class A(object):
            num = 1

            @util.cached_property
            def f(self):
                A.num += 1
                return A.num

        a = A()
        self.assertEqual(2, a.f)
        self.assertEqual(2, a.f)
        del a.f
        self.assertEqual(3, a.f)
        self.assertEqual(3, a.f)


class UtilModuleTest(unittest.TestCase):

    def test_all_equal(self):
        self.assertTrue(util.all_equal([]))
        self.assertTrue(util.all_equal(x for x in []))

        self.assertTrue(util.all_equal(x for x in [1]))
        self.assertFalse(util.all_equal(x for x in [1, 2]))
        self.assertFalse(util.all_equal([1, 2]))
