import unittest
from tests import utilities
import pygrametl
import pygrametl.drawntabletesting as dtt
from pygrametl.tables import Dimension
from pygrametl.tables import CachedDimension
from pygrametl.tables import BulkDimension
from pygrametl.tables import CachedBulkDimension
from pygrametl.tables import SlowlyChangingDimension
from pygrametl.tables import SnowflakedDimension


class DimensionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("dogs", """
        | id:int (pk) | name:varchar | city:varchar | age:int |
        | ----------- | ------------ | ------------ | ------- |
        | 1           | Rufus        | Hundeby      | 2       |
        | 2           | Rex          | Vuffestad    | 5       |
        | 3           | King         | Bjæfstrup    | 2       |
        """)
        cls.namemapping = {"name": "firstname", "city": "town", "age": "years"}

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.test_dim = Dimension(name=self.initial.name,
                                  key=self.initial.key(),
                                  attributes=self.initial.attributes)

    # The first help methods before the actual tests are (or can be) overridden
    # in test classes for dimensions that inherit from Dimension. Hence, the
    # tests for Dimension can be reused by test classes for other dimensions
    # (possibly with different schemas) by overriding these help methods.
    def get_existing_row(self, withkey=False):
        existing_row = {
            "name": "King",
            "city": "Bjæfstrup",
            "age": 2
        }
        if withkey:
            existing_row["id"] = 3

        return existing_row

    # Returns a tuple consisting of: a part of a row (i.e. a dict containing a
    # subset of the attributes) and the number of occurrences of the part in the
    # test dimension
    def get_part_of_row_and_num_of_occurrences(self):
        part_of_row = {
            "age": 2
        }
        occurrences = 2

        return part_of_row, occurrences

    # Returns a part of a row that is not "contained" in any existing row in the
    # test dimension
    def generate_part_of_row_that_is_nonexisting(self):
        return {
            "age": 99
        }

    # Returns a row that does not preexist in the test dimension. The row is
    # returned with or without the key depending on the withkey argument
    def generate_nonexisting_row(self, withkey=False):
        row = {}
        if withkey:
            row["id"] = 4

        row.update({"name": "Rollo", "city": "Dogville", "age": 4})
        return row

    def generate_row_with_missing_atts(self):
        return {
            "name": "King",
            "city": "Bjæfstrup"
        }

    # Returns an updated version of a preexisting row together with the row's
    # index in the table
    def generate_updated_row(self):
        row_index = 2
        updated_row = {
            "id": 3,
            "name": "Kong",
            "city": "Andeby",
            "age": 25
        }
        return row_index, updated_row

    # Currently returns only two new rows
    def generate_multiple_nonexisting_rows(self):
        return [{"id": 4, "name": "Rollo", "city": "Dogville", "age": 4},
                {"id": 5, "name": "Wuffie", "city": "Dogtown", "age": 8}]

    # Converts a row from a dict format to a DTT format:
    # {att_1: val_1,...,att_n: val_n} -> "| val_1 |...| val_n |"
    # This help method is not overridden anywhere but is needed: The DTT format
    # of a row cannot always be "hardcoded" since the content of the dict
    # depends on what is returned from other help methods
    def convert_row_to_dtt_str(self, row):
        dtt_str = "| "

        for value in row.values():
            dtt_str += str(value)
            dtt_str += " | "

        return dtt_str

    # Not overridden but can be used to apply different namemappings for
    # different schemas
    def apply_namemapping(self, row_orig):
        row_namemapped = {}

        for key in row_orig.keys():
            if key in self.namemapping.keys():
                key_namemapped = self.namemapping[key]
                row_namemapped[key_namemapped] = row_orig[key]
            else:
                row_namemapped[key] = row_orig[key]

        return row_namemapped

    def test_lookup(self):
        postcondition = self.initial

        row = self.get_existing_row(withkey=True)

        expected_key = row["id"]
        actual_key = self.test_dim.lookup(row)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition = self.initial

        row = self.get_existing_row(withkey=True)
        namemapped_row = self.apply_namemapping(row)

        expected_key = row["id"]
        actual_key = self.test_dim.lookup(
            namemapped_row, namemapping=self.namemapping)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_lookup_with_lookupatts(self):
        dimension = Dimension(name=self.initial.name,
                              key=self.initial.key(),
                              attributes=self.initial.attributes,
                              lookupatts={"name"})
        postcondition = self.initial
        row = {
            "name": "King"
        }

        self.assertEqual(3, dimension.lookup(row))
        self.cw.commit()

        postcondition.assertEqual()

    # The row does not exist in the database
    def test_lookup_nonexisting_row(self):
        postcondition = self.initial
        row = self.generate_nonexisting_row()

        self.assertIsNone(self.test_dim.lookup(row))
        self.cw.commit()

        postcondition.assertEqual()

    # Looking up a row with a missing attribute
    def test_lookup_with_missing_atts(self):
        postcondition = self.initial
        row = self.generate_row_with_missing_atts()
        self.assertRaises(KeyError, self.test_dim.lookup, row)
        self.cw.commit()

        postcondition.assertEqual()

    def test_getbykey(self):
        postcondition = self.initial
        expected_row = self.get_existing_row(withkey=True)
        key = expected_row["id"]
        actual_row = self.test_dim.getbykey(key)
        self.cw.commit()

        self.assertDictEqual(expected_row, actual_row)
        postcondition.assertEqual()

    def test_getbykey_nonexisting_row(self):
        postcondition = self.initial

        # No row exists with this key
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        nonexisting_key = nonexisting_row["id"]

        actual_row = self.test_dim.getbykey(nonexisting_key)
        self.cw.commit()

        for att in actual_row:
            self.assertIsNone(actual_row[att])

        postcondition.assertEqual()

    def test_getbyvals(self):
        postcondition = self.initial
        vals, expected_num_of_rows = self.get_part_of_row_and_num_of_occurrences()

        rows = self.test_dim.getbyvals(vals)
        self.cw.commit()

        self.assertEqual(expected_num_of_rows, len(rows))

        for row in rows:
            for att, expected_val in vals.items():
                actual_val = row[att]
                self.assertEqual(expected_val, actual_val)

        postcondition.assertEqual()

    def test_getbyvals_none(self):
        postcondition = self.initial
        vals = self.generate_part_of_row_that_is_nonexisting()

        rows = self.test_dim.getbyvals(vals)
        self.cw.commit()

        # There should be 0 rows with the specified vals
        self.assertEqual(0, len(rows))

        postcondition.assertEqual()

    def test_getbyvals_with_namemapping(self):
        postcondition = self.initial
        vals, expected_num_of_rows = self.get_part_of_row_and_num_of_occurrences()
        vals_namemapped = self.apply_namemapping(vals)
        namemapping = {}

        # Determine the namemapping argument since vals_namemapped might only
        # contain a subset of the attributes. Hence, self.namemapping cannot be
        # passed directly
        for key, value in self.namemapping.items():
            if value in vals_namemapped.keys():
                namemapping[key] = value

        rows = self.test_dim.getbyvals(
            vals_namemapped, namemapping=namemapping)
        self.cw.commit()

        self.assertEqual(expected_num_of_rows, len(rows))

        for row in rows:
            for att, expected_val in vals.items():
                actual_val = row[att]
                self.assertEqual(expected_val, actual_val)

        postcondition.assertEqual()

    def test_update(self):
        row_index, updated_row = self.generate_updated_row()
        dtt_update_str = self.convert_row_to_dtt_str(updated_row)

        postcondition = self.initial.update(row_index, dtt_update_str)

        self.test_dim.update(updated_row)
        self.cw.commit()

        postcondition.assertEqual()

    def test_update_with_namemapping(self):
        row_index, updated_row = self.generate_updated_row()
        dtt_update_str = self.convert_row_to_dtt_str(updated_row)

        postcondition = self.initial.update(row_index, dtt_update_str)

        updated_row_namemapped = self.apply_namemapping(updated_row)

        self.test_dim.update(updated_row_namemapped,
                             namemapping=self.namemapping)
        self.cw.commit()

        postcondition.assertEqual()

    # Update a non-existing row. The state of the database should remain
    # unchanged.
    def test_update_nonexisting_row(self):
        postcondition = self.initial

        updated_row = self.generate_nonexisting_row(withkey=True)

        self.test_dim.update(updated_row)
        self.cw.commit()
        postcondition.assertEqual()

    def test_update_missing_key(self):
        postcondition = self.initial

        # Key is missing in the row
        updated_row = self.generate_nonexisting_row(withkey=False)

        self.assertRaises(KeyError, self.test_dim.update, updated_row)

        self.cw.commit()
        postcondition.assertEqual()

    def test_update_do_nothing(self):
        postcondition = self.initial

        # No attribute values specified - the state should remain unchanged
        updated_row = {
            "id": 1
        }

        self.test_dim.update(updated_row)
        self.cw.commit()

        postcondition.assertEqual()

    def test_ensure_once(self):
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        dtt_str = self.convert_row_to_dtt_str(nonexisting_row)

        postcondition = self.initial + dtt_str

        expected_key = nonexisting_row["id"]
        actual_key = self.test_dim.ensure(nonexisting_row)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_ensure_twice(self):
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        dtt_str = self.convert_row_to_dtt_str(nonexisting_row)

        postcondition = self.initial + dtt_str

        expected_key = nonexisting_row["id"]
        actual_key_first_time = self.test_dim.ensure(nonexisting_row)
        actual_key_second_time = self.test_dim.ensure(nonexisting_row)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key_first_time)
        self.assertEqual(actual_key_first_time, actual_key_second_time)

        postcondition.assertEqual()

    def test_ensure_multiple_rows(self):
        postcondition = self.initial

        # Generate new nonexisting rows
        new_rows = self.generate_multiple_nonexisting_rows()
        new_rows_dtt_str = []

        # Convert the new rows to DTT string formats
        for row in new_rows:
            new_rows_dtt_str.append(self.convert_row_to_dtt_str(row))

        for dtt_str in new_rows_dtt_str:
            postcondition = postcondition + dtt_str

        for row in postcondition.additions(withKey=True):
            expected_key = row["id"]
            actual_key = self.test_dim.ensure(row)
            self.cw.commit()
            self.assertEqual(expected_key, actual_key)

        postcondition.assertEqual()

    def test_ensure_existing_row(self):
        postcondition = self.initial

        existing_row = self.get_existing_row(withkey=True)

        expected_key = existing_row["id"]
        actual_key = self.test_dim.ensure(existing_row)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_ensure_with_namemapping(self):
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        dtt_str = self.convert_row_to_dtt_str(nonexisting_row)
        namemapped_row = self.apply_namemapping(nonexisting_row)

        postcondition = self.initial + dtt_str

        expected_key = namemapped_row["id"]
        actual_key = self.test_dim.ensure(
            namemapped_row, namemapping=self.namemapping)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_ensure_existing_row_with_namemapping(self):
        postcondition = self.initial

        namemapped_existing_row = self.apply_namemapping(
            self.get_existing_row(withkey=True))

        expected_key = namemapped_existing_row["id"]
        actual_key = self.test_dim.ensure(
            namemapped_existing_row, namemapping=self.namemapping)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    # Insert a non-existing row
    def test_insert_one(self):
        postcondition = self.initial + \
            self.convert_row_to_dtt_str(
                self.generate_nonexisting_row(withkey=True))

        for row in postcondition.additions(withKey=True):
            expected_key = row["id"]
            actual_key = self.test_dim.insert(row)
            self.cw.commit()
            self.assertEqual(expected_key, actual_key)

        postcondition.assertEqual()

    def test_insert_two(self):
        postcondition = self.initial

        # Generate new nonexisting rows
        new_rows = self.generate_multiple_nonexisting_rows()
        new_rows_dtt_str = []

        # Convert the new rows to a DTT string format
        for row in new_rows:
            new_rows_dtt_str.append(self.convert_row_to_dtt_str(row))

        for dtt_str in new_rows_dtt_str:
            postcondition = postcondition + dtt_str

        for row in postcondition.additions(withKey=True):
            actual_key = self.test_dim.insert(row)
            self.cw.commit()
            expected_key = row["id"]
            self.assertEqual(expected_key, actual_key)

        postcondition.assertEqual()

    # Inserts a new row that contains a superfluous attribute
    def test_insert_with_extra_att(self):
        postcondition = self.initial + \
            self.convert_row_to_dtt_str(
                self.generate_nonexisting_row(withkey=True))

        for row in postcondition.additions(withKey=True):
            row["superfluousAttribute"] = 100
            expected_key = row["id"]
            actual_key = self.test_dim.insert(row)
            self.cw.commit()
            self.assertEqual(expected_key, actual_key)

        postcondition.assertEqual()

    def test_insert_with_namemapping(self):
        new_row = self.generate_nonexisting_row(withkey=True)

        postcondition = self.initial + self.convert_row_to_dtt_str(new_row)

        new_row_namemapped = self.apply_namemapping(new_row)

        expected_key = new_row["id"]
        actual_key = self.test_dim.insert(
            new_row_namemapped, namemapping=self.namemapping)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_idfinder(self):
        # This new row does not contain a key value
        row_without_key = self.generate_nonexisting_row(withkey=False)

        row_with_dummy_key = self.generate_nonexisting_row(withkey=True)
        # Currently, the dummy idfinder simply returns 99
        row_with_dummy_key["id"] = 99

        postcondition = self.initial + \
            self.convert_row_to_dtt_str(row_with_dummy_key)

        dimension = Dimension(name=self.initial.name,
                              key=self.initial.key(),
                              attributes=self.initial.attributes,
                              idfinder=self.dummy_idfinder)

        expected_key = row_with_dummy_key["id"]
        actual_key = dimension.insert(row_without_key)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def dummy_idfinder(self, row, namemapping):
        return 99


class CachedDimensionTest(DimensionTest):
    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial = dtt.Table("dogs", """
        | id:int (pk) | name:varchar | city:varchar | age:int |
        | ----------- | ------------ | ------------ | ------- |
        | 1           | Rufus        | Hundeby      | 2       |
        | 2           | Rex          | Vuffestad    | 5       |
        | 3           | King         | Bjæfstrup    | 2       |
        """)
        self.initial.reset()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=True)

    def test_prefill_true(self):
        # Connection has been closed - only cached rows can be retrieved
        self.cw.close()

        # The following three rows should have been cached
        for row, key_expected in [({"name": "Rufus", "city": "Hundeby", "age": 2}, 1),
                                  ({"name": "Rex", "city": "Vuffestad", "age": 5}, 2),
                                  ({"name": "King", "city": "Bjæfstrup", "age": 2}, 3)
                                  ]:
            self.assertEqual(key_expected, self.test_dim.lookup(row))

    def test_prefill_false(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=False)

        self.cw.close()

        # The following three rows should not have been cached, and an exception
        # is raised since the connection is closed
        for row in [{"name": "Rufus", "city": "Hundeby", "age": 2},
                    {"name": "Rex", "city": "Vuffestad", "age": 5},
                    {"name": "King", "city": "Bjæfstrup", "age": 2},
                    ]:
            self.assertRaises(Exception, self.test_dim.lookup, row)

    def test_cachefullrows_true(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        lookupatts=("name", "city"),
                                        prefill=True,
                                        cachefullrows=True)

        self.cw.close()

        # All the attributes of the following three rows should have been cached
        for entire_row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                           {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5},
                           {"id": 3, "name": "King", "city": "Bjæfstrup", "age": 2}
                           ]:
            key = entire_row["id"]
            self.assertDictEqual(entire_row, self.test_dim.getbykey(key))

    def test_cachefullrows_false(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        lookupatts=("name", "city"),
                                        prefill=True,
                                        cachefullrows=False)

        self.cw.close()

        # Full rows are not cached. Thus, looking up by key will raise an
        # exception because the connection is closed
        for entire_row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                           {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5},
                           {"id": 3, "name": "King", "city": "Bjæfstrup", "age": 2}
                           ]:
            key = entire_row["id"]
            self.assertRaises(Exception, self.test_dim.getbykey, key)

        # However, the cache can still be used to lookup keys by lookup
        # attributes
        for row, key_expected in [({"name": "Rufus", "city": "Hundeby", "age": 2}, 1),
                                  ({"name": "Rex", "city": "Vuffestad", "age": 5}, 2),
                                  ({"name": "King", "city": "Bjæfstrup", "age": 2}, 3)
                                  ]:
            self.assertEqual(key_expected, self.test_dim.lookup(row))

    def test_cacheoninsert_true(self):
        cache_size = 1000
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        cacheoninsert=True,
                                        size=cache_size)
        inserted_rows = []

        # Insert a number of new rows into the dimension (number of inserted
        # rows is equal to the cache size). The loop starts from 4 because the
        # keys 1-3 are already in use
        for i in range(4, cache_size + 4):
            row = {"id": i, "name": "Somename", "city": "Somecity", "age": i}
            self.test_dim.insert(row)
            inserted_rows.append(row)

        self.cw.close()

        # Check if the rows are cached correctly
        for row in inserted_rows:
            key_expected = row["id"]
            self.assertEqual(key_expected, self.test_dim.lookup(row))

    def test_cacheoninsert_false(self):
        cache_size = 1000
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        cacheoninsert=False,
                                        size=cache_size)
        inserted_rows = []

        for i in range(4, cache_size + 4):
            row = {"id": i, "name": "Somename", "city": "Somecity", "age": i}
            self.test_dim.insert(row)
            inserted_rows.append(row)

        self.cw.close()

        # The inserted rows should not have been cached -> exception raised
        # because connection is closed
        for row in inserted_rows:
            self.assertRaises(Exception, self.test_dim.lookup, row)

    def test_size_custom_finite(self):
        cache_size = 500
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=False,
                                        cacheoninsert=True,
                                        size=cache_size)
        inserted_rows_first_time = []
        inserted_rows_second_time = []

        # Insert a number of new rows into the dimension (number of inserted
        # rows is equal to the cache size). The loop starts from 4 because the
        # keys 1-3 are already in use
        for i in range(4, cache_size + 4):
            row = {"id": i, "name": "Somename", "city": "Somecity", "age": i}
            self.test_dim.insert(row)
            inserted_rows_first_time.append(row)

        # Now, insert another n rows where n = cache_size
        for i in range(cache_size + 4, 4 + 2 * cache_size):
            row = {"id": i, "name": "Somename", "city": "Somecity", "age": i}
            self.test_dim.insert(row)
            inserted_rows_second_time.append(row)

        self.cw.close()

        # None of the inserted rows in the first round should still be cached
        for row in inserted_rows_first_time:
            self.assertRaises(Exception, self.test_dim.lookup, row)

        # Only the rows inserted in the second round should still be in the
        # cache (FIFO cache)
        for row in inserted_rows_second_time:
            key_expected = row["id"]
            self.assertEqual(key_expected, self.test_dim.lookup(row))

    def test_size_custom_infinite(self):
        number_of_rows_to_insert = 12000
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=False,
                                        cacheoninsert=True,
                                        size=-1)
        inserted_rows = []

        for i in range(4, number_of_rows_to_insert + 4):
            row = {"id": i, "name": "Somename", "city": "Somecity", "age": i}
            self.test_dim.insert(row)
            inserted_rows.append(row)

        self.cw.close()

        for row in inserted_rows:
            key_expected = row["id"]
            self.assertEqual(key_expected, self.test_dim.lookup(row))

    def test_lookup_cache_initially_empty(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=False)

        self.cw.close()

        # The cache should initially be empty because prefill is False and no
        # lookups have been made
        for row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                    {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5},
                    {"id": 3, "name": "King", "city": "Bjæfstrup", "age": 2}]:
            self.assertRaises(Exception, self.test_dim.lookup, row)

    def test_lookup_cache_after_some_lookups(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=False)

        # These two rows should now be cached
        for row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                    {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5}]:
            self.test_dim.lookup(row)

        self.cw.close()

        for row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                    {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5}]:
            key_expected = row["id"]
            self.assertEqual(key_expected, self.test_dim.lookup(row))

        # However, this row should not be in the cache
        self.assertRaises(Exception, self.test_dim.lookup, {
            "id": 3, "name": "King", "city": "Bjæfstrup", "age": 2})

    def test_getbykey_cache_initially_empty(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=False)

        self.cw.close()

        # The cache should initially be empty because prefill is False and no
        # lookups have been made
        for row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                    {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5},
                    {"id": 3, "name": "King", "city": "Bjæfstrup", "age": 2}]:
            key = row["id"]
            self.assertRaises(Exception, self.test_dim.getbykey, key)

    def test_getbykey_cache_after_some_lookups(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=False,
                                        cachefullrows=True)

        # These two rows should now be cached
        for row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                    {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5}]:
            key = row["id"]
            self.test_dim.getbykey(key)

        self.cw.close()

        for row in [{"id": 1, "name": "Rufus", "city": "Hundeby", "age": 2},
                    {"id": 2, "name": "Rex", "city": "Vuffestad", "age": 5}]:
            key = row["id"]
            self.assertDictEqual(row, self.test_dim.getbykey(key))

        # However, this key should not be in the cache
        self.assertRaises(Exception, self.test_dim.getbykey, 3)

    # Everything is cached since prefill and cacheonsinsert are true and as long
    # the cache size has not been exceeded
    def test_update_caching_where_prefill_and_cacheoninsert_are_true_and_cache_size_is_not_exceeded(self):
        cache_size = 100
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=True,
                                        cachefullrows=True,
                                        size=cache_size)

        # Update the attributes of the first row
        updated_row = {"id": 1, "name": "new_name",
                       "city": "new_city", "age": 56}
        self.test_dim.update(updated_row)

        self.cw.close()

        # The row in the cache should have been updated
        self.assertDictEqual(updated_row, self.test_dim.getbykey(1))
        self.assertEqual(1, self.test_dim.lookup(updated_row))

        # The old row should not be in the cache
        self.assertIsNone(self.test_dim.lookup(
            {"name": "Rufus", "city": "Hundeby", "age": 2}))

    # Not all rows in the dimension are necessarily cached
    def test_update_caching_where_cacheoninsert_is_false(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=True,
                                        cacheoninsert=False,
                                        cachefullrows=True)

        # Update the attributes of the first row
        updated_row = {"id": 1, "name": "new_name",
                       "city": "new_city", "age": 56}
        self.test_dim.update(updated_row)

        self.cw.close()

        # Both the new and old row should have been deleted from the cache
        self.assertRaises(Exception, self.test_dim.lookup, {
            "name": "new_name", "city": "new_city", "age": 56})
        self.assertRaises(Exception, self.test_dim.lookup, {
            "name": "Rufus", "city": "Hundeby", "age": 2})
        self.assertRaises(Exception, self.test_dim.getbykey, 1)

    def test_defaultidvalue(self):
        self.test_dim = CachedDimension(name=self.initial.name,
                                        key=self.initial.key(),
                                        attributes=self.initial.attributes,
                                        prefill=True,
                                        defaultidvalue="unknown")

        non_existing_row = {"name": "Random", "city": "Nowhere", "age": 0}
        self.assertEqual("unknown", self.test_dim.lookup(non_existing_row))

        existing_row = {"name": "Rufus", "city": "Hundeby", "age": 2}
        self.assertEqual(1, self.test_dim.lookup(existing_row))


class BulkDimensionTest(DimensionTest):
    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.test_dim = BulkDimension(name=self.initial.name,
                                      key=self.initial.key(),
                                      attributes=self.initial.attributes,
                                      bulkloader=self.loader)

    def loader(self, name, attributes, fieldsep, rowsep, nullval, filehandle):
        sql = "INSERT INTO dogs(id, name, city, age) VALUES({}, '{}', '{}', {})"
        encoding = utilities.get_os_encoding()
        for line in filehandle:
            values = line.decode(encoding).strip().split('\t')
            insert = sql.format(*values)
            self.cw.execute(insert)

    def test_awaitingempty(self):
        self.assertEqual(self.test_dim.awaitingrows, 0)

    def test_awaiting_insert(self):
        expected = self.initial + "| 4 | Rollo | Dogville | 4 |" \
            + "| 5 | Frygtløs | Intetsted | 8 |" \
            + "| 6 | Fido | Østrig | 4 |"
        [self.test_dim.insert(row) for row in expected.additions(withKey=True)]
        self.assertEqual(self.test_dim.awaitingrows, 3)

    def test_awaiting_insert_commit(self):
        self.initial.reset()
        expected = self.initial + "| 4 | Rollo | Dogville | 4 |" \
            + "| 5 | Frygtløs | Intetsted | 8 |" \
            + "| 6 | Fido | Østrig | 4 |"
        [self.test_dim.insert(row) for row in expected.additions(withKey=True)]
        self.assertEqual(self.test_dim.awaitingrows, 3)
        self.cw.commit()
        self.assertEqual(self.test_dim.awaitingrows, 0)


class CachedBulkDimensionTest(BulkDimensionTest):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("dogs", """
        | id:int (pk) | name:varchar | city:varchar | age:int |
        | ----------- | ------------ | ------------ | ------- |
        | 1           | Rufus        | Hundeby      | 2       |
        | 2           | Rex          | Vuffestad    | 5       |
        | 3           | King         | Bjæfstrup    | 2       |
        """)

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.test_dim = CachedBulkDimension(name=self.initial.name,
                                            key=self.initial.key(),
                                            attributes=self.initial.attributes,
                                            bulkloader=self.loader)


class SlowlyChangingDimensionTest(DimensionTest):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | city:varchar | fromdate:timestamp | todate:timestamp | version:int |
        | ----------- | ------------ | ------- | ------------ | ------------------ | ---------------- | ----------- |
        | 1           | Ann          | 20      | Aalborg      | 2010-01-01         | 2010-03-03       | 1           |
        | 2           | Bob          | 31      | Boston       | 2010-02-02         | NULL             | 1           |
        | 3           | Ann          | 20      | Aarhus       | 2010-03-03         | NULL             | 2           |
        | 4           | Charlie      | 19      | Copenhagen   | 2011-01-01         | NULL             | 1           |
        """)
        cls.namemapping = {"name": "firstname", "age": "years", "city": "town"}

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                lookupatts=['name'],
                                                versionatt='version',
                                                fromatt='fromdate',
                                                toatt='todate',
                                                type1atts='age',
                                                srcdateatt='from',
                                                cachesize=100,
                                                prefill=True)

    def get_existing_row(self, withkey=False):
        row = {}

        if withkey:
            row["id"] = 2

        row.update({
            "name": "Bob",
            "age": 31,
            "city": "Boston",
            "fromdate": "2010-02-02",
            "todate": None,
            "version": 1
        })

        return row

    # The returned row is missing the lookup attribute "name"
    def generate_row_with_missing_atts(self):
        return {
            "city": "Aalborg"
        }

    # Returns a tuple consisting of a part of a row (a row containing a subset
    # of the attributes) and the number of occurrences of the part in the test
    # dimension
    def get_part_of_row_and_num_of_occurrences(self):
        part_of_row = {
            "name": "Ann"
        }
        occurences = 2

        return part_of_row, occurences

    def generate_part_of_row_that_is_nonexisting(self):
        return {
            "name": "Peter"
        }

    # Returns a row that does not preexist in the test dimension. The row is
    # returned with or without the key depending on the withkey argument
    def generate_nonexisting_row(self, withkey=False):
        row = {}

        if withkey:
            row["id"] = 5

        row.update({"name": "Dan",
                    "age": 45,
                    "city": "Dublin",
                    "fromdate": "2010-01-02",
                    "todate": "2010-03-04",
                    "version": 1})
        return row

    def generate_updated_row(self, namemapping=False):
        row_index = 0
        updatedrow = {
            "id": 1,
            "name": "Alice",
            "age": 21,
            "city": "Amager",
            "fromdate": "2010-01-01",
            "todate": "2010-03-03",
            "version": 1
        }
        return row_index, updatedrow

    # Currently returns only two new rows
    def generate_multiple_nonexisting_rows(self):
        return [{"id": 5, "name": "Dan", "age": 45, "city": "Dublin",
                 "fromdate": "2010-01-02", "todate": "2010-03-04",
                 "version": 1},
                {"id": 6, "name": "Eric", "age": 50, "city": "Ebeltoft",
                 "fromdate": "2010-02-02", "todate": "2010-02-02",
                 "version": 1}]

    # The lookup method in Dimension is overridden in SlowlyChangingDimension
    def test_lookup(self):
        postcondition = self.initial

        row = {"name": "Ann"}
        # Newest version has key = 3
        expected_key = 3
        actual_key = self.test_dim.lookup(row)
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition = self.initial

        namemapped_row = {"firstname": "Ann"}
        expected_key = 3
        actual_key = self.test_dim.lookup(
            namemapped_row, namemapping={"name": "firstname"})
        self.cw.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_lookup_with_lookupatts(self):
        scdimension = SlowlyChangingDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              lookupatts=['name', 'age'],
                                              versionatt='version',
                                              fromatt='fromdate',
                                              toatt='todate',
                                              srcdateatt='from',
                                              cachesize=100,
                                              prefill=True)
        postcondition = self.initial

        self.assertEqual(3, scdimension.lookup({'name': 'Ann', 'age': 20}))

        # The row is missing a lookup attribute
        self.assertRaises(KeyError, scdimension.lookup, {'name': 'Ann'})

        postcondition.assertEqual()

    def test_lookup_with_lookupatts_toatt_is_none(self):
        scdimension = SlowlyChangingDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              lookupatts=['name', 'age'],
                                              versionatt='version',
                                              fromatt='fromdate',
                                              srcdateatt='from',
                                              cachesize=100,
                                              prefill=True)
        postcondition = self.initial

        self.assertEqual(3, scdimension.lookup({'name': 'Ann', 'age': 20}))

        # The row is missing a lookup attribute
        self.assertRaises(KeyError, scdimension.lookup, {'name': 'Ann'})

        postcondition.assertEqual()

    def test_lookup_with_lookupatts_tooatt_is_none_and_with_custom_quotechar(self):
        # The identifiers are now wrapped with ""
        pygrametl.tables.definequote('\"')

        scdimension = SlowlyChangingDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              lookupatts=['name', 'age'],
                                              versionatt='version',
                                              fromatt='fromdate',
                                              srcdateatt='from',
                                              cachesize=100,
                                              prefill=True)

        postcondition = self.initial

        self.assertEqual(3, scdimension.lookup({'name': 'Ann', 'age': 20}))

        # The row is missing a lookup attribute
        self.assertRaises(KeyError, scdimension.lookup, {'name': 'Ann'})

        postcondition.assertEqual()

        # The quotechar function is reset to default with no wrapping
        pygrametl.tables.definequote(None)

    # The row does not exist in the database
    def test_lookup_nonexisting_row(self):
        postcondition = self.initial
        row = {"name": "Peter"}

        self.assertIsNone(self.test_dim.lookup(row))
        self.cw.commit()

        postcondition.assertEqual()

    # Looking up a row with a missing lookup attribute
    def test_lookup_with_missing_atts(self):
        postcondition = self.initial
        row = self.generate_row_with_missing_atts()

        self.assertRaises(KeyError, self.test_dim.lookup, row)
        self.cw.commit()

        postcondition.assertEqual()

    def test_scdensure_existing_row(self):
        row = {'name': 'Ann', 'age': 20,
               'city': 'Aarhus', 'from': '2010-03-03'}
        postcondition = self.initial

        self.test_dim.scdensure(row)
        expected_key = 3
        actual_key = row["id"]

        self.assertEqual(expected_key, actual_key)

        postcondition.assertEqual()

    def test_scdensure_existing_row_without_srcdateatt(self):
        scdimension = SlowlyChangingDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              lookupatts=['name', 'age'],
                                              versionatt='version',
                                              fromatt='fromdate',
                                              toatt='todate',
                                              cachesize=100,
                                              prefill=True)

        row = {'name': 'Ann', 'age': 20, 'city': 'Aarhus'}

        postcondition = self.initial

        scdimension.scdensure(row)
        expected_key = 3
        actual_key = row["id"]

        self.assertEqual(expected_key, actual_key)

        postcondition.assertEqual()

    def test_scdensure_namemapping(self):
        row = {'firstname': 'Ann', 'years': 20,
               'town': 'Aarhus', 'from': '2010-03-03'}
        postcondition = self.initial

        self.test_dim.scdensure(row, namemapping=self.namemapping)
        expected_key = 3
        actual_key = row["id"]

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_scdensure_newversion(self):
        row = {'name': 'Ann', 'age': 20, 'city': 'Aabenraa',
               'from': '2010-04-04'}
        postcondition = self.initial.update(
            2, "| 3 | Ann | 20 | Aarhus | 2010-03-03 | 2010-04-04 | 2 |") \
            + "| 5 | Ann | 20 | Aabenraa | 2010-04-04 | NULL | 3 |"
        self.test_dim.scdensure(row)
        expected_key = 5
        # The key is added to the original row by scdensure
        actual_key = row["id"]

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    # The type1 attribute 'age' is changed - The 'age' attribute in all existing
    # rows for Ann should be changed to 21
    def test_scdensure_type1_change_existing_row(self):
        postcondition = \
            self.initial.update(0, "| 1 | Ann | 21 | Aalborg | 2010-01-01 | 2010-03-03 | 1 |") \
                        .update(2, "| 3 | Ann | 21 | Aarhus  | 2010-03-03 | NULL | 2 |")

        self.test_dim.scdensure(
            {'name': 'Ann', 'age': 21, 'city': 'Aarhus', 'from': '2010-03-03'})
        postcondition.assertEqual()

    # A new row should be inserted for Ann, and the attribute 'age' in all rows
    # for Ann should be 21
    def test_scdensure_type1_change_new_row(self):
        postcondition = self.initial.update(0, "| 1 | Ann | 21 | Aalborg | 2010-01-01 | 2010-03-03 | 1 |") \
                                    .update(2, "| 3 | Ann | 21 | Aarhus  | 2010-03-03 | 2010-04-04 | 2 |") \
                                    + "| 5 | Ann | 21 | Aabenraa  | 2010-04-04 | NULL | 3 |"

        self.test_dim.scdensure(
            {'name': 'Ann', 'age': 21, 'city': 'Aabenraa', 'from': '2010-04-04'})
        postcondition.assertEqual()

    def test_scdensure_two_newversions(self):
        postcondition = self.initial.update(
            2, "| 3 | Ann | 20 | Aarhus | 2010-03-03 | 2010-04-04 | 2 |") \
            + "| 5 | Ann | 20 | Aalborg  | 2010-04-04 | 2010-05-05 | 3 |" \
            + "| 6 | Ann | 20 | Aabenraa | 2010-05-05 | NULL       | 4 |"
        self.test_dim.scdensure(
            {'name': 'Ann', 'age': 20, 'city': 'Aalborg', 'from': '2010-04-04'})
        self.test_dim.scdensure(
            {'name': 'Ann', 'age': 20, 'city': 'Aabenraa', 'from': '2010-05-05'})

        postcondition.assertEqual()

    def test_scdensure_new_row(self):
        postcondition = self.initial \
            + "| 5 | Doris | 85 | Dublin | 2010-04-04 | NULL | 1 |"
        self.test_dim.scdensure(
            {'name': 'Doris', 'age': 85, 'city': 'Dublin', 'from': '2010-04-04'})

        postcondition.assertEqual()

    def test_scdensure_side_effects(self):
        scdimension = SlowlyChangingDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              lookupatts=['name', 'age'],
                                              versionatt='version',
                                              fromatt='fromdate',
                                              toatt='todate',
                                              cachesize=100,
                                              prefill=True)

        row = {'name': 'Ann', 'age': 20, 'city': 'Aarhus'}
        scdimension.scdensure(row)
        self.assertIn(self.initial.key(), row)
        self.assertIn('version', row)
        self.assertIn('fromdate', row)
        self.assertIn('todate', row)

    def test_closecurrent(self):
        postcondition = self.initial.update(
            2, "| 3 | Ann | 20 | Aarhus  | 2010-03-03 | 2010-04-04 | 2 |")
        self.test_dim.closecurrent({'name': 'Ann'}, end='2010-04-04')

        postcondition.assertEqual()

    def test_closecurrent_and_lookup(self):
        self.test_closecurrent()
        keyval = self.test_dim.lookup({'name': 'Ann'})
        self.assertEqual(keyval, 3)

    def test_closecurrent_and_scdensure_newversion(self):
        postcondition = self.initial.update(
            2, "| 3 | Ann | 20 | Aarhus  | 2010-03-03 | 2010-04-04 | 2 |") \
            + "| 5 | Ann | 20 | Aabenraa| 2010-05-05 | NULL       | 3 | "
        self.test_dim.closecurrent({'name': 'Ann'}, end='2010-04-04')
        self.test_dim.scdensure(
            {'name': 'Ann', 'age': 20, 'city': 'Aabenraa', 'from': '2010-05-05'})

        postcondition.assertEqual()

    def test_closecurrent_and_scdensure_newversion_and_lookup(self):
        self.test_closecurrent_and_scdensure_newversion()
        keyval = self.test_dim.lookup({'name': 'Ann'})
        self.assertEqual(keyval, 5)

    def test_orderingatt(self):

        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | city:varchar | version:int |
        | ----------- | ------------ | ------- | ------------ | ----------- |
        | 1           | Ann          | 22      | Aalborg      | 2           |
        | 2           | Bob          | 31      | Boston       | 1           |
        | 3           | Ann          | 20      | Aarhus       | 1           |
        | 4           | Charlie      | 19      | Copenhagen   | 1           |
        """)
        self.initial.reset()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                lookupatts=['name'],
                                                orderingatt='version',
                                                cachesize=100,
                                                prefill=True)
        self.assertEqual(1, self.test_dim.lookup({"name": "Ann"}))

    def test_versionatt(self):

        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | city:varchar | version:int |
        | ----------- | ------------ | ------- | ------------ | ----------- |
        | 1           | Ann          | 22      | Aalborg      | 2           |
        | 2           | Bob          | 31      | Boston       | 1           |
        | 3           | Ann          | 20      | Aarhus       | 1           |
        | 4           | Charlie      | 19      | Copenhagen   | 1           |
        """)
        self.initial.reset()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                lookupatts=['name'],
                                                versionatt='version',
                                                cachesize=100,
                                                prefill=True)
        self.assertEqual(1, self.test_dim.lookup({"name": "Ann"}))

    # Tests that if both orderingatt and versionatt are defined, orderingatt is
    # used to identify the newest version
    def test_orderingatt_is_used_to_identify_newest_version(self):

        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | city:varchar | version:int | number:int |
        | ----------- | ------------ | ------- | ------------ | ----------- | -----      |
        | 1           | Ann          | 22      | Aalborg      | 2           | 1          |
        | 2           | Bob          | 31      | Boston       | 1           | 1          |
        | 3           | Ann          | 20      | Aarhus       | 1           | 2          |
        | 4           | Charlie      | 19      | Copenhagen   | 1           | 1          |
        """)
        self.initial.reset()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                lookupatts=['name'],
                                                versionatt='version',
                                                orderingatt='number',
                                                cachesize=100,
                                                prefill=True)
        self.assertEqual(3, self.test_dim.lookup({"name": "Ann"}))

    # A ValueError is raised since both the orderingatt, versionatt, and toatt
    # are all None
    def test_orderingatt_versionatt_toatt_are_all_none(self):
        self.assertRaises(ValueError, SlowlyChangingDimension, name=self.initial.name,
                          key=self.initial.key(),
                          attributes=self.initial.attributes,
                          lookupatts=['name'],
                          fromatt='fromdate',
                          cachesize=100,
                          prefill=True)

    # minfrom is set to a default value
    def test_minfrom(self):
        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | fromdate:timestamp | city:varchar | version:int |
        | ----------- | ------------ | ------- | ------------------ | ------------ | ----------- |
        | 1           | Ann          | 22      | 2010-01-01         | Aalborg      | 1           |
        | 2           | Bob          | 31      | 2010-02-02         | Boston       | 1           |
        """)
        self.initial.reset()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                fromatt='fromdate',
                                                lookupatts=['name'],
                                                versionatt='version',
                                                minfrom='2012-12-12',
                                                cachesize=100,
                                                prefill=True)

        self.test_dim.scdensure(
            {'name': 'Charlie', 'age': 24, 'city': 'Copenhagen'})

        postcondition = self.initial \
            + "| 3 | Charlie | 24 | 2012-12-12 | Copenhagen | 1 |"
        postcondition.assertEqual()

    # minfrom should be ignored when inserting the row since it already contains
    # a fromatt value
    def test_minfrom_is_ignored(self):
        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | fromdate:timestamp | city:varchar | version:int |
        | ----------- | ------------ | ------- | ------------------ | ------------ | ----------- |
        | 1           | Ann          | 22      | 2010-01-01         | Aalborg      | 1           |
        | 2           | Bob          | 31      | 2010-02-02         | Boston       | 1           |
        """)
        self.initial.reset()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                fromatt='fromdate',
                                                lookupatts=['name'],
                                                versionatt='version',
                                                minfrom='2012-12-12',
                                                cachesize=100,
                                                prefill=True)

        self.test_dim.scdensure({'name': 'Charlie', 'age': 24,
                                 'city': 'Copenhagen',
                                 'fromdate': '2010-03-03'})

        postcondition = self.initial \
            + "| 3 | Charlie | 24 | 2010-03-03 | Copenhagen | 1 |"
        postcondition.assertEqual()

    # maxto is set to a default value
    def test_maxto(self):
        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | city:varchar | fromdate:timestamp | todate:timestamp | version:int |
        | ----------- | ------------ | ------- | ------------ | ------------------ | ---------------- | ----------- |
        | 1           | Ann          | 20      | Aalborg      | 2010-01-01         | 2099-12-12       | 1           |
        | 2           | Bob          | 31      | Boston       | 2010-02-02         | 2099-12-12       | 1           |
        """)
        self.initial.reset()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                fromatt='fromdate',
                                                srcdateatt='from',
                                                toatt='todate',
                                                maxto='2099-12-12',
                                                lookupatts=['name'],
                                                versionatt='version',
                                                cachesize=100,
                                                prefill=True)

        self.test_dim.scdensure(
            {'name': 'Ann', 'age': 20, 'city': 'Aarhus', 'from': '2012-12-12'})

        postcondition = self.initial.update(
            0, "| 1 | Ann | 20 | Aalborg | 2010-01-01 | 2012-12-12 | 1 |") \
            + "| 3 | Ann | 20 | Aarhus | 2012-12-12 | 2099-12-12 | 2 |"
        postcondition.assertEqual()

    def test_idfinder(self):
        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | city:varchar | version:int |
        | ----------- | ------------ | ------- | ------------ | ----------- |
        | 1           | Ann          | 20      | Aalborg      | 1           |
        | 2           | Bob          | 31      | Boston       | 1           |
        """)
        self.initial.reset()
        self.test_dim = SlowlyChangingDimension(name=self.initial.name,
                                                key=self.initial.key(),
                                                attributes=self.initial.attributes,
                                                lookupatts=['name'],
                                                versionatt='version',
                                                idfinder=self.dummy_idfinder,
                                                cachesize=100,
                                                prefill=True)

        self.test_dim.scdensure(
            {'name': 'Charlie', 'age': 30, 'city': 'Copenhagen'})

        postcondition = self.initial + "| 99 | Charlie | 30 | Copenhagen | 1 |"
        postcondition.assertEqual()


class SnowflakedDimensionTest(unittest.TestCase):
    def setUp(self):
        utilities.ensure_default_connection_wrapper()

        self.year_dt = dtt.Table("year", """
        | YearID:int (pk) | Year:int         |
        | --------------- | ---------------- |
        | 1               | 2000             |
        | 2               | 2001             |
        | 3               | 2002             |
        """)

        self.month_dt = dtt.Table("month", """
        | MonthID:int (pk) | Month:varchar   | YearID:int (fk year(YearID))   |
        | ---------------- | --------------- | ------------------------------ |
        | 1                | January 2000    | 1                              |
        | 2                | February 2000   | 1                              |
        | 13               | January 2001    | 2                              |
        | 25               | January 2002    | 3                              |
        """)

        self.day_dt = dtt.Table("day", """
        | DayID:int (pk) | Day:varchar      | MonthID:int (fk month(MonthID))   |
        | -------------- | ---------------- | --------------------------------- |
        | 1              | January 1, 2000  | 1                                 |
        | 32             | February 1, 2000 | 2                                 |
        | 33             | February 2, 2000 | 2                                 |
        | 366            | January 1, 2001  | 13                                |
        | 731            | January 1, 2002  | 25                                |
        """)

        self.year_dt.reset()
        self.month_dt.reset()
        self.day_dt.reset()

        self.cw = pygrametl.getdefaulttargetconnection()

        self.day_dim = Dimension(name=self.day_dt.name,
                                 key=self.day_dt.key(),
                                 attributes=self.day_dt.attributes)

        self.month_dim = Dimension(name=self.month_dt.name,
                                   key=self.month_dt.key(),
                                   attributes=self.month_dt.attributes)

        self.year_dim = Dimension(name=self.year_dt.name,
                                  key=self.year_dt.key(),
                                  attributes=self.year_dt.attributes)

        self.snowflaked_dim = SnowflakedDimension(
            [(self.day_dim, self.month_dim), (self.month_dim, self.year_dim)])

    def test_lookup(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertEqual(1, self.snowflaked_dim.lookup(
            {"Day": "January 1, 2000", "MonthID": 1}))
        self.assertEqual(32, self.snowflaked_dim.lookup(
            {"Day": "February 1, 2000", "MonthID": 2}))
        self.assertEqual(33, self.snowflaked_dim.lookup(
            {"Day": "February 2, 2000", "MonthID": 2}))
        self.assertEqual(366, self.snowflaked_dim.lookup(
            {"Day": "January 1, 2001", "MonthID": 13}))
        self.assertEqual(731, self.snowflaked_dim.lookup(
            {"Day": "January 1, 2002", "MonthID": 25}))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_lookupatts(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        # Change the day dimension so it only uses the "Day" attribute as lookup
        # attribute
        self.day_dim = Dimension(name=self.day_dt.name,
                                 key=self.day_dt.key(),
                                 attributes=self.day_dt.attributes,
                                 lookupatts=["Day"])
        self.snowflaked_dim = SnowflakedDimension(
            [(self.day_dim, self.month_dim), (self.month_dim, self.year_dim)])

        self.assertEqual(1, self.snowflaked_dim.lookup(
            {"Day": "January 1, 2000"}))
        self.assertEqual(32, self.snowflaked_dim.lookup(
            {"Day": "February 1, 2000"}))
        self.assertEqual(33, self.snowflaked_dim.lookup(
            {"Day": "February 2, 2000"}))
        self.assertEqual(366, self.snowflaked_dim.lookup(
            {"Day": "January 1, 2001"}))
        self.assertEqual(731, self.snowflaked_dim.lookup(
            {"Day": "January 1, 2002"}))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"Day": "Date"}

        self.assertEqual(1, self.snowflaked_dim.lookup(
            {"Date": "January 1, 2000", "MonthID": 1}, namemapping=namemapping))
        self.assertEqual(32, self.snowflaked_dim.lookup(
            {"Date": "February 1, 2000", "MonthID": 2}, namemapping=namemapping))
        self.assertEqual(33, self.snowflaked_dim.lookup(
            {"Date": "February 2, 2000", "MonthID": 2}, namemapping=namemapping))
        self.assertEqual(366, self.snowflaked_dim.lookup(
            {"Date": "January 1, 2001", "MonthID": 13}, namemapping=namemapping))
        self.assertEqual(731, self.snowflaked_dim.lookup(
            {"Date": "January 1, 2002", "MonthID": 25}, namemapping=namemapping))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_nonexisting_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertIsNone(self.snowflaked_dim.lookup(
            {"Day": "January 45, 2099", "MonthID": 1}))
        self.assertIsNone(self.snowflaked_dim.lookup(
            {"Day": "Non-existing row", "MonthID": -1}))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_missing_atts(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertRaises(KeyError, self.snowflaked_dim.lookup, {
            "Day": "January 1, 2000"})
        self.assertRaises(KeyError, self.snowflaked_dim.lookup, {"MonthID": 2})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbykey(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertDictEqual({"DayID": 1, "Day": "January 1, 2000",
                              "MonthID": 1}, self.snowflaked_dim.getbykey(1))
        self.assertDictEqual({"DayID": 32, "Day": "February 1, 2000",
                              "MonthID": 2}, self.snowflaked_dim.getbykey(32))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbykey_nonexisting_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertDictEqual(
            {"DayID": None, "Day": None, "MonthID": None},
            self.snowflaked_dim.getbykey(99))
        self.assertDictEqual(
            {"DayID": None, "Day": None, "MonthID": None},
            self.snowflaked_dim.getbykey(-1))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbykey_with_fullrow(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        expected_fullrow = {"DayID": 1, "Day": "January 1, 2000", "MonthID": 1,
                            "Month": "January 2000", "YearID": 1, "Year": 2000}

        self.assertDictEqual(
            expected_fullrow, self.snowflaked_dim.getbykey(1, fullrow=True))

        expected_fullrow = {"DayID": 731, "Day": "January 1, 2002",
                            "MonthID": 25, "Month": "January 2002",
                            "YearID": 3, "Year": 2002}

        self.assertDictEqual(
            expected_fullrow, self.snowflaked_dim.getbykey(731, fullrow=True))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        vals = {"MonthID": 2}
        expected_rows = [
            {"DayID": 32, "Day": "February 1, 2000", "MonthID": 2},
            {"DayID": 33, "Day": "February 2, 2000", "MonthID": 2}
        ]
        actual_rows = self.snowflaked_dim.getbyvals(vals, fullrow=False)

        self.assertEqual(len(expected_rows), len(actual_rows))
        for actual_row in actual_rows:
            self.assertTrue(actual_row in expected_rows)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals_with_fullrow(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        vals = {"MonthID": 2}
        expected_rows = [
            {"DayID": 32, "Day": "February 1, 2000", "MonthID": 2,
             "Month": "February 2000", "YearID": 1, "Year": 2000},
            {"DayID": 33, "Day": "February 2, 2000", "MonthID": 2,
             "Month": "February 2000", "YearID": 1, "Year": 2000},
        ]
        actual_rows = self.snowflaked_dim.getbyvals(vals, fullrow=True)

        self.assertEqual(len(expected_rows), len(actual_rows))
        for actual_row in actual_rows:
            self.assertTrue(actual_row in expected_rows)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals_none(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        vals = {"MonthID": 99}
        expected_num_of_rows = 0

        rows = self.snowflaked_dim.getbyvals(vals, fullrow=False)

        self.assertEqual(expected_num_of_rows, len(rows))

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals_with_namemapping(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"MonthID": "MonthKey"}
        vals = {"MonthKey": 2}
        expected_rows = [
            {"DayID": 32, "Day": "February 1, 2000", "MonthID": 2},
            {"DayID": 33, "Day": "February 2, 2000", "MonthID": 2}
        ]
        actual_rows = self.snowflaked_dim.getbyvals(
            vals, fullrow=False, namemapping=namemapping)

        self.assertEqual(len(expected_rows), len(actual_rows))
        for actual_row in actual_rows:
            self.assertTrue(actual_row in expected_rows)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_without_foreign_keys(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        updated_row = {"DayID": 33, "Day": "February 2nd in year 2000"}
        self.snowflaked_dim.update(updated_row)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    # The attributes from the referenced table 'Month' that are present in the
    # row should also be updated
    def test_update_with_foreign_key_and_with_foreign_nonkey_atts_present_in_row(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt.update(
            1, "| 2 | February in year 2000 | 1 |")
        postcondition_year = self.year_dt

        updated_row = {"DayID": 33, "MonthID": 2,
                       "Day": "February 2nd in year 2000",
                       "Month": "February in year 2000"}
        self.snowflaked_dim.update(updated_row)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    # Nothing should be updated in the referenced table 'Month'
    def test_update_with_foreign_key_and_but_without_foreign_atts_present_in_row(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        updated_row = {"DayID": 33, "MonthID": 2,
                       "Day": "February 2nd in year 2000"}
        self.snowflaked_dim.update(updated_row)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_a_child_dimension_only(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt.update(
            0, "| 1 | January in 2000 | 1 |")
        postcondition_year = self.year_dt

        updated_row = {"MonthID": 1, "Month": "January in 2000"}
        self.snowflaked_dim.update(updated_row)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_non_existing_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        updated_row = {"DayID": 9999, "Day": "Nothing should be updated"}
        self.snowflaked_dim.update(updated_row)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_do_nothing(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        # No attributes are changed
        self.snowflaked_dim.update({"DayID": 1})

        # No key of a participating dimension is mentioned
        self.snowflaked_dim.update(
            {"Day": "Some day", "Month": "Some month", "Year": "Some year"})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_with_namemapping(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"Day": "Date"}
        updated_row = {"DayID": 33, "Date": "February 2nd in year 2000"}

        self.snowflaked_dim.update(updated_row, namemapping=namemapping)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_only_new_row_in_root(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.snowflaked_dim.ensure({"DayID": 5, "Day": "January 5, 2000",
                                    "Month": "January 2000", "Year": 2000})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_new_row_in_all_dimensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |"
        postcondition_month = self.month_dt + "| 39 | March 2003 | 4 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |"

        self.snowflaked_dim.ensure({"DayID": 1200, "Day": "March 3, 2003",
                                    "MonthID": 39, "Month": "March 2003",
                                    "Year": 2003})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_two_new_rows_in_all_dimensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |" + \
            "| 1700 | April 4, 2004  | 52 |"
        postcondition_month = self.month_dt + \
            "| 39 | March 2003 | 4 |" + "| 52 | April 2004 | 5 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |" + "| 5 | 2004 |"

        self.snowflaked_dim.ensure({"DayID": 1200, "Day": "March 3, 2003",
                                    "MonthID": 39, "Month": "March 2003",
                                    "Year": 2003})
        self.snowflaked_dim.ensure({"DayID": 1700, "Day": "April 4, 2004",
                                    "MonthID": 52, "Month": "April 2004",
                                    "Year": 2004})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_existing_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.snowflaked_dim.ensure(
            {"Day": "January 1, 2000", "Month": "January 2000", "Year": 2000})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_nonexisting_row_with_namemapping(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"Day": "Date"}

        self.snowflaked_dim.ensure({"DayID": 5, "Date": "January 5, 2000",
                                    "Month": "January 2000", "Year": 2000},
                                   namemapping=namemapping)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_existing_row_with_namemapping(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"Day": "Date"}

        self.snowflaked_dim.ensure({"Date": "January 1, 2000",
                                    "Month": "January 2000", "Year": 2000},
                                   namemapping=namemapping)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_only_new_row_in_root(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.snowflaked_dim.insert({"DayID": 5, "Day": "January 5, 2000",
                                    "Month": "January 2000", "Year": 2000})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_new_row_in_all_dimensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |"
        postcondition_month = self.month_dt + "| 39 | March 2003 | 4 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |"

        self.snowflaked_dim.insert({"DayID": 1200, "Day": "March 3, 2003",
                                    "MonthID": 39, "Month": "March 2003",
                                    "Year": 2003})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_two_new_rows_in_all_dimensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |" + \
            "| 1700 | April 4, 2004  | 52 |"
        postcondition_month = self.month_dt + \
            "| 39 | March 2003 | 4 |" + "| 52 | April 2004 | 5 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |" + "| 5 | 2004 |"

        self.snowflaked_dim.insert({"DayID": 1200, "Day": "March 3, 2003",
                                    "MonthID": 39, "Month": "March 2003",
                                    "Year": 2003})
        self.snowflaked_dim.insert({"DayID": 1700, "Day": "April 4, 2004",
                                    "MonthID": 52, "Month": "April 2004",
                                    "Year": 2004})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_existing_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertRaises(Exception, self.snowflaked_dim.insert,
                          {"Day": "January 1, 2000", "Month": "January 2000",
                           "Year": 2000})

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_new_row_with_namemapping(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"Day": "Date"}

        self.snowflaked_dim.insert({"DayID": 5, "Date": "January 5, 2000",
                                    "Month": "January 2000", "Year": 2000},
                                   namemapping=namemapping)

        self.cw.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()
