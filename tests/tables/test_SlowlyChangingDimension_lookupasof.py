import unittest

from tests import utilities
import pygrametl
import pygrametl.drawntabletesting as dtt
from pygrametl.tables import SlowlyChangingDimension


class SlowlyChangingDimensionLookupasofTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | city:varchar | fromdate:timestamp | todate:timestamp | version:int |
        | ----------- | ------------ | ------------ | ------------------ | ---------------- | ----------- |
        | 1           | Ann          | Aalborg      | 2001-01-01         | 2001-12-31       | 1           |
        | 2           | Bob          | Boston       | 2001-01-01         | 2001-12-31       | 1           |
        | 3           | Ann          | Aarhus       | 2002-01-01         | 2002-12-31       | 2           |
        | 4           | Charlie      | Copenhagen   | 2001-01-01         | NULL             | 1           |
        | 5           | Ann          | Aabenraa     | 2003-01-01         | NULL             | 3           |
        | 6           | Bob          | Birkelse     | 2002-01-01         | 2002-12-31       | 2           |
        """)

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.ann = {'name':'Ann'}
        self.bob = {'name':'Bob'}

    def test_lookupasof_usingto(self):
        test_dimension = SlowlyChangingDimension(
            name=self.initial.name,
            key=self.initial.key(),
            attributes=self.initial.attributes,
            lookupatts=['name'],
            versionatt='version',
            toatt='todate',
            cachesize=100,
            prefill=False)
        key = test_dimension.lookupasof(self.ann, "2001-05-05", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof(self.ann, "2001-12-31", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof(self.ann, "2001-12-31", False)
        self.assertEqual(key, 3) # intended behaviour since we ignore fromdate in this test
        key = test_dimension.lookupasof(self.ann, "2222-12-31", True)
        self.assertEqual(key, 5)
        key = test_dimension.lookupasof(self.bob, "2222-12-31", True)
        self.assertEqual(key, None)

    def test_lookupasof_usingfrom(self):
        self.initial += "| 0 | Ann | Arden | NULL | NULL | 0 |" # from ignored;
        self.initial.reset()
        test_dimension = SlowlyChangingDimension(
            name=self.initial.name,
            key=self.initial.key(),
            attributes=self.initial.attributes,
            lookupatts=['name'],
            versionatt='version',
            fromatt='fromdate',
            cachesize=100,
            prefill=False)
        key = test_dimension.lookupasof(self.ann, "2001-05-05", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof(self.ann, "2002-01-01", True)
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof(self.ann, "2002-01-01", False)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof(self.ann, "1999-12-31", True)
        self.assertEqual(key, 0)
        key = test_dimension.lookupasof(self.bob, "1999-05-05", True)
        self.assertEqual(key, None)

    def test_lookupasof_usingfromto(self):
        test_dimension = SlowlyChangingDimension(
            name=self.initial.name,
            key=self.initial.key(),
            attributes=self.initial.attributes,
            lookupatts=['name'],
            versionatt='version',
            fromatt='fromdate',
            toatt='todate',
            cachesize=100,
            prefill=False)
        key = test_dimension.lookupasof(self.ann, "2001-05-05", (True, False))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof(self.ann, "2001-05-05", (False, True))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof(self.ann, "2001-12-31", (False, True))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof(self.ann, "2002-12-31", (True, True))
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof(self.ann, "2222-12-31", (True, True))
        self.assertEqual(key, 5)
        key = test_dimension.lookupasof(self.bob, "2222-12-31", (True, True))
        self.assertEqual(key, None)
