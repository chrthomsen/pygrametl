# Copyright (c) 2023, Aalborg University (pygrametl@cs.aau.dk)
# All rights reserved.

# Redistribution and use in source anqd binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# - Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
        cls.initial = dtt.Table("book", """
        | id:int (pk) | title:text            | genre:text |
        | ----------- | --------------------- | ---------- |
        | 1           | Unknown               | Unknown    |
        | 2           | Nineteen Eighty-Four  | Novel      |
        | 3           | Calvin and Hobbes One | Comic      |
        | 4           | Calvin and Hobbes Two | Comic      |
        | 5           | The Silver Spoon      | Cookbook   |
        """)
        cls.namemapping = {"title": "name", "genre": "type"}

    # These helper methods are intended to be overridden in the test classes for
    # different types of dimensions. This allows the tests for Dimension can be
    # reused by test classes for other dimensions despite using different schema

    # Return a row that exists in cls.initial
    def get_existing_row(self, withkey=False):
        existing_row = {
            "title": "Calvin and Hobbes One",
            "genre": "Comic"
        }
        if withkey:
            existing_row["id"] = 3

        return existing_row

    # Return a dict containing a subset of the attributes in the test dimension
    # to get by and the number of occurrences of the part in the test dimension
    def get_part_of_row_and_num_of_occurrences(self):
        return { "genre": "Comic" }, 2

    # Return a dict containing a subset of the attributes in the test dimension
    # to get by, this row should not match any rows in the test dimension
    def generate_part_of_row_that_is_nonexisting(self):
        return { "genre": "Thriller" }

    # Return a row that does not exist in the test dimension. The row is
    # returned with or without the key depending on the withkey argument
    def generate_nonexisting_row(self, withkey=False):
        row = {"title": "Calvin and Hobbes Three", "genre": "Comic"}
        if withkey:
            row["id"] = 6
        return row

    # Return a row that exists in the test dimension but with missing values
    def generate_row_with_missing_name_attribute(self):
        return { "title": "Nineteen Eighty-Four" }

    # Return an updated version of an existing row together with the row's index
    def generate_updated_row(self):
        row_index = 2
        updated_row = {
            "id": 3,
            "title": "Calvin and Hobbes Three",
            "genre": "Comic",
        }
        return row_index, updated_row

    # Return two rows that do not exist in the test dimension
    def generate_multiple_nonexisting_rows(self):
        return [{"id": 6, "title": "Calvin and Hobbes Three", "genre": "Comic"},
                {"id": 7, "title": "Calvin and Hobbes Four", "genre": "Comic"}]

    # Converts a row from a dict to a DTT row
    def convert_row_to_dtt_str(self, row):
        dtt_str = "| "

        # self.initial.attributes is used to ensure the columns are ordered
        for key in ["id"] + self.initial.attributes:
            dtt_str += str(row[key])
            dtt_str += " | "

        return dtt_str

    # Apply the namemapping in cls.namemapping
    def apply_namemapping(self, row):
        return { self.namemapping.get(key, key): value
                 for (key, value) in row.items() }

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.test_dimension = self.get_test_subject()

    # Get an instance of the class being tested by this TestCase
    def get_test_subject(self, **specifics):
        args = {'name':self.initial.name, 'key':self.initial.key(),
                'attributes':self.initial.attributes}
        args.update(specifics)
        return Dimension(**args)

    def test_lookup(self):
        postcondition = self.initial

        row = self.get_existing_row(withkey=True)

        actual_key = self.test_dimension.lookup(row)
        self.connection_wrapper.commit()

        self.assertEqual(row["id"], actual_key)
        postcondition.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition = self.initial

        row = self.get_existing_row(withkey=True)
        namemapped_row = self.apply_namemapping(row)

        actual_key = self.test_dimension.lookup(
            namemapped_row, namemapping=self.namemapping)
        self.connection_wrapper.commit()

        self.assertEqual(row["id"], actual_key)
        postcondition.assertEqual()

    def test_lookup_with_lookupatts(self):
        dimension = self.get_test_subject(lookupatts={"title"})
        postcondition = self.initial
        row = { "title": "Calvin and Hobbes One" }

        self.assertEqual(3, dimension.lookup(row))
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_lookup_nonexisting_row(self):
        postcondition = self.initial
        row = self.generate_nonexisting_row()

        self.assertIsNone(self.test_dimension.lookup(row))
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_lookup_with_missing_atttributes(self):
        postcondition = self.initial
        row = self.generate_row_with_missing_name_attribute()
        self.assertRaises(KeyError, self.test_dimension.lookup, row)
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_getbykey(self):
        postcondition = self.initial
        expected_row = self.get_existing_row(withkey=True)
        key = expected_row["id"]
        actual_row = self.test_dimension.getbykey(key)
        self.connection_wrapper.commit()

        self.assertDictEqual(expected_row, actual_row)
        postcondition.assertEqual()

    def test_getbykey_nonexisting_row(self):
        postcondition = self.initial

        # No row exists with this key
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        nonexisting_key = nonexisting_row["id"]

        actual_row = self.test_dimension.getbykey(nonexisting_key)
        self.connection_wrapper.commit()

        for att in actual_row:
            self.assertIsNone(actual_row[att])

        postcondition.assertEqual()

    def test_lookuprow(self):
        postcondition = self.initial
        expected_row = self.get_existing_row(withkey=True)
        actual_row = self.test_dimension.lookuprow(expected_row)
        self.connection_wrapper.commit()

        self.assertDictEqual(expected_row, actual_row)
        postcondition.assertEqual()

    def test_lookuprow_nonexisting_row(self):
        postcondition = self.initial
        nonexisting = self.generate_nonexisting_row(withkey=False)
        result = self.test_dimension.lookuprow(nonexisting)
        self.connection_wrapper.commit()

        for att in result:
            self.assertIsNone(result[att])

        postcondition.assertEqual()

    def test_lookuprow_with_lookupatts(self):
        dimension = self.get_test_subject(lookupatts={"title"})
        postcondition = self.initial
        expected_row = self.get_existing_row(withkey=True)
        actual_row = dimension.lookuprow(expected_row)
        self.connection_wrapper.commit()

        self.assertDictEqual(expected_row, actual_row)
        postcondition.assertEqual()


    def test_lookuprow_with_lookupatts_nonexisting_row(self):
        dimension = self.get_test_subject(lookupatts={"title"})
        postcondition = self.initial
        nonexisting = self.generate_nonexisting_row(withkey=False)
        result = dimension.lookuprow(nonexisting)
        self.connection_wrapper.commit()

        for att in result:
            self.assertIsNone(result[att])

        postcondition.assertEqual()

    def test_getbyvals(self):
        postcondition = self.initial
        vals, expected_num_of_rows = \
            self.get_part_of_row_and_num_of_occurrences()

        rows = self.test_dimension.getbyvals(vals)
        self.connection_wrapper.commit()

        self.assertEqual(expected_num_of_rows, len(rows))

        for row in rows:
            for att, expected_val in vals.items():
                actual_val = row[att]
                self.assertEqual(expected_val, actual_val)

        postcondition.assertEqual()

    def test_getbyvals_none(self):
        postcondition = self.initial
        vals = self.generate_part_of_row_that_is_nonexisting()

        rows = self.test_dimension.getbyvals(vals)
        self.connection_wrapper.commit()

        # There should be 0 rows with the specified vals
        self.assertEqual(0, len(rows))

        postcondition.assertEqual()

    def test_getbyvals_with_namemapping(self):
        postcondition = self.initial
        vals, expected_num_of_rows = \
            self.get_part_of_row_and_num_of_occurrences()
        vals_namemapped = self.apply_namemapping(vals)

        # Determine the namemapping arguments as vals_namemapped may contain a
        # subset of the attributes. So the namemapping cannot be passed directly
        namemapping = {}
        for key, value in self.namemapping.items():
            if value in vals_namemapped.keys():
                namemapping[key] = value

        rows = self.test_dimension.getbyvals(
            vals_namemapped, namemapping=namemapping)
        self.connection_wrapper.commit()

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

        self.test_dimension.update(updated_row)
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_update_with_namemapping(self):
        row_index, updated_row = self.generate_updated_row()
        dtt_update_str = self.convert_row_to_dtt_str(updated_row)

        postcondition = self.initial.update(row_index, dtt_update_str)

        updated_row_namemapped = self.apply_namemapping(updated_row)

        self.test_dimension.update(updated_row_namemapped,
                                   namemapping=self.namemapping)
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_update_nonexisting_row(self):
        postcondition = self.initial

        updated_row = self.generate_nonexisting_row(withkey=True)

        self.test_dimension.update(updated_row)
        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_update_missing_key(self):
        postcondition = self.initial

        # Key is missing in the row
        updated_row = self.generate_nonexisting_row(withkey=False)

        self.assertRaises(KeyError, self.test_dimension.update, updated_row)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_update_no_changes(self):
        postcondition = self.initial

        updated_row = { "id": 1 }

        self.test_dimension.update(updated_row)
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_ensure_once(self):
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        dtt_str = self.convert_row_to_dtt_str(nonexisting_row)

        postcondition = self.initial + dtt_str

        actual_key = self.test_dimension.ensure(nonexisting_row)
        self.connection_wrapper.commit()

        self.assertEqual(nonexisting_row["id"], actual_key)
        postcondition.assertEqual()

    def test_ensure_twice(self):
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        dtt_str = self.convert_row_to_dtt_str(nonexisting_row)

        postcondition = self.initial + dtt_str

        actual_key_first_time = self.test_dimension.ensure(nonexisting_row)
        actual_key_second_time = self.test_dimension.ensure(nonexisting_row)
        self.connection_wrapper.commit()

        self.assertEqual(nonexisting_row["id"], actual_key_first_time)
        self.assertEqual(actual_key_first_time, actual_key_second_time)

        postcondition.assertEqual()

    def test_ensure_multiple_rows(self):
        postcondition = self.initial

        for row in self.generate_multiple_nonexisting_rows():
            postcondition = postcondition + self.convert_row_to_dtt_str(row)

        for row in postcondition.additions(withKey=True):
            actual_key = self.test_dimension.ensure(row)
            self.connection_wrapper.commit()
            self.assertEqual(row["id"], actual_key)

        postcondition.assertEqual()

    def test_ensure_existing_row(self):
        postcondition = self.initial

        existing_row = self.get_existing_row(withkey=True)

        actual_key = self.test_dimension.ensure(existing_row)
        self.connection_wrapper.commit()

        self.assertEqual(existing_row["id"], actual_key)
        postcondition.assertEqual()

    def test_ensure_with_namemapping(self):
        nonexisting_row = self.generate_nonexisting_row(withkey=True)
        dtt_str = self.convert_row_to_dtt_str(nonexisting_row)
        namemapped_row = self.apply_namemapping(nonexisting_row)

        postcondition = self.initial + dtt_str

        actual_key = self.test_dimension.ensure(
            namemapped_row, namemapping=self.namemapping)
        self.connection_wrapper.commit()

        self.assertEqual(namemapped_row["id"], actual_key)
        postcondition.assertEqual()

    def test_ensure_existing_row_with_namemapping(self):
        postcondition = self.initial

        namemapped_existing_row = self.apply_namemapping(
            self.get_existing_row(withkey=True))

        actual_key = self.test_dimension.ensure(
            namemapped_existing_row, namemapping=self.namemapping)
        self.connection_wrapper.commit()

        self.assertEqual(namemapped_existing_row["id"], actual_key)
        postcondition.assertEqual()

    def test_insert_once(self):
        postcondition = self.initial + \
            self.convert_row_to_dtt_str(
                self.generate_nonexisting_row(withkey=True))

        for row in postcondition.additions(withKey=True):
            actual_key = self.test_dimension.insert(row)
            self.connection_wrapper.commit()
            self.assertEqual(row["id"], actual_key)

        postcondition.assertEqual()

    def test_insert_twice(self):
        postcondition = self.initial

        for row in self.generate_multiple_nonexisting_rows():
            postcondition = postcondition + self.convert_row_to_dtt_str(row)

        for row in postcondition.additions(withKey=True):
            actual_key = self.test_dimension.insert(row)
            self.connection_wrapper.commit()
            self.assertEqual(row["id"], actual_key)

        postcondition.assertEqual()

    def test_insert_with_an_extra_attribute(self):
        postcondition = self.initial + \
            self.convert_row_to_dtt_str(
                self.generate_nonexisting_row(withkey=True))

        for row in postcondition.additions(withKey=True):
            row["extra_attribute"] = 100
            actual_key = self.test_dimension.insert(row)
            self.connection_wrapper.commit()
            self.assertEqual(row["id"], actual_key)

        postcondition.assertEqual()

    def test_insert_with_namemapping(self):
        new_row = self.generate_nonexisting_row(withkey=True)

        postcondition = self.initial + self.convert_row_to_dtt_str(new_row)

        new_row_namemapped = self.apply_namemapping(new_row)

        actual_key = self.test_dimension.insert(
            new_row_namemapped, namemapping=self.namemapping)
        self.connection_wrapper.commit()

        self.assertEqual(new_row["id"], actual_key)
        postcondition.assertEqual()

    def test_idfinder(self):
        row_without_key = self.generate_nonexisting_row(withkey=False)
        row_with_mock_key = self.generate_nonexisting_row(withkey=True)
        row_with_mock_key["id"] = 99  # Must matchmock_idfinder()

        postcondition = self.initial + \
            self.convert_row_to_dtt_str(row_with_mock_key)

        dimension = Dimension(name=self.initial.name,
                              key=self.initial.key(),
                              attributes=self.initial.attributes,
                              idfinder=self.mock_idfinder)

        actual_key = dimension.insert(row_without_key)
        self.connection_wrapper.commit()

        self.assertEqual(row_with_mock_key["id"], actual_key)
        postcondition.assertEqual()

    def mock_idfinder(self, row, namemapping):
        return 99


class CachedDimensionTest(DimensionTest):

    def setUp(self):
        # A new DTT is created to ensure it uses the latest connection wrapper
        utilities.ensure_default_connection_wrapper()
        self.initial = dtt.Table("book", """
        | id:int (pk) | title:text            | genre:text |
        | ----------- | --------------------- | ---------- |
        | 1           | Unknown               | Unknown    |
        | 2           | Nineteen Eighty-Four  | Novel      |
        | 3           | Calvin and Hobbes One | Comic      |
        | 4           | Calvin and Hobbes Two | Comic      |
        | 5           | The Silver Spoon      | Cookbook   |
        """)
        self.initial.reset()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=True)

    # Get an instance of the class being tested by this TestCase
    def get_test_subject(self, **specifics):
        args = {'name':self.initial.name, 'key':self.initial.key(),
                'attributes':self.initial.attributes, 'prefill':True} #FIXME: prefill?
        args.update(specifics)
        return CachedDimension(**args)


    def test_prefill_true(self):
        # Ensure that only cached rows can be retrieved
        self.connection_wrapper.close()

        for row, key in [({ "title": "Unknown", "genre": "Unknown" }, 1),
                         ({ "title": "Nineteen Eighty-Four", "genre": "Novel" }, 2),
                         ({ "title": "Calvin and Hobbes One", "genre": "Comic" }, 3),
                         ({ "title": "Calvin and Hobbes Two", "genre": "Comic" }, 4),
                         ({ "title": "The Silver Spoon", "genre": "Cookbook" }, 5)
                         ]:
            self.assertEqual(key, self.test_dimension.lookup(row))

    def test_prefill_false(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=False)

        # Ensure that only cached rows can be retrieved
        self.connection_wrapper.close()

        # The following three rows should not have been cached, and an exception
        # should be raised for each row since the connection is closed
        for row, _ in [({ "title": "Unknown", "genre": "Unknown" }, 1),
                       ({ "title": "Nineteen Eighty-Four", "genre": "Novel" }, 2),
                       ({ "title": "Calvin and Hobbes One", "genre": "Comic" }, 3),
                       ({ "title": "Calvin and Hobbes Two", "genre": "Comic" }, 4),
                       ({ "title": "The Silver Spoon", "genre": "Cookbook" }, 5)
                       ]:
            self.assertRaises(Exception, self.test_dimension.lookup, row)

    def test_cachefullrows_true(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=True,
                                              cachefullrows=True)

        self.connection_wrapper.close()

        # All the attributes of the following three rows should have been cached
        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    { "id": 3, "title": "Calvin and Hobbes One", "genre": "Comic" },
                    { "id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic" },
                    { "id": 5, "title": "The Silver Spoon", "genre": "Cookbook" },
                    ]:
            key = row["id"]
            self.assertEqual(key, self.test_dimension.lookup(row))
            self.assertDictEqual(row, self.test_dimension.getbykey(key))

    def test_cachefullrows_false(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=True,
                                              cachefullrows=False)
        self.connection_wrapper.close()

        # The following three rows should not have been cached, and an exception
        # should be raised for each row since the connection is closed
        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    { "id": 3, "title": "Calvin and Hobbes One", "genre": "Comic" },
                    { "id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic" },
                    { "id": 5, "title": "The Silver Spoon", "genre": "Cookbook" },
                    ]:
            self.assertRaises(Exception, self.test_dimension.getbykey, row["id"])

        # However, the cache can still be used to lookup keys by attributes
        for row, key in [({ "title": "Unknown", "genre": "Unknown" }, 1),
                         ({ "title": "Nineteen Eighty-Four", "genre": "Novel" }, 2),
                         ({ "title": "Calvin and Hobbes One", "genre": "Comic" }, 3),
                         ({ "title": "Calvin and Hobbes Two", "genre": "Comic" }, 4),
                         ({ "title": "The Silver Spoon", "genre": "Cookbook" }, 5)
                         ]:
            self.assertEqual(key, self.test_dimension.lookup(row))

    def test_cacheoninsert_true(self):
        cache_size = 1000
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              cacheoninsert=True,
                                              size=cache_size)

        # Insert a number of new rows into the dimension (number of inserted
        # rows is equal to the cache size). The loop starts from 6 because the
        # keys 1-5 are already in use
        inserted_rows = []
        for i in range(6, cache_size + 4):
            row = {"id": i, "title": "Title " + str(i), "genre": "Genre" }
            self.test_dimension.insert(row)
            inserted_rows.append(row)

        self.connection_wrapper.close()

        # Check if the rows are cached correctly
        for row in inserted_rows:
            self.assertEqual(row["id"], self.test_dimension.lookup(row))

    def test_cacheoninsert_false(self):
        cache_size = 1000
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              cacheoninsert=False,
                                              size=cache_size)

        # Insert a number of new rows into the dimension (number of inserted
        # rows is equal to the cache size). The loop starts from 6 because the
        # keys 1-5 are already in use
        inserted_rows = []
        for i in range(6, cache_size + 4):
            row = {"id": i, "title": "Title " + str(i), "genre": "Genre" }
            self.test_dimension.insert(row)
            inserted_rows.append(row)

        self.connection_wrapper.close()

        # The inserted rows should not have been cached so an exception should
        # be raised since the connection is closed
        for row in inserted_rows:
            self.assertRaises(Exception, self.test_dimension.lookup, row)

    def test_size_custom_finite(self):
        cache_size = 500
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=False,
                                              cacheoninsert=True,
                                              size=cache_size)

        # Insert a number of new rows into the dimension (number of inserted
        # rows is equal to the cache size). The loop starts from 6 because the
        # keys 1-5 are already in use
        inserted_rows_first_time = []
        for i in range(6, cache_size + 4):
            row = {"id": i, "title": "Title " + str(i), "genre": "Genre" }
            self.test_dimension.insert(row)
            inserted_rows_first_time.append(row)

        # Now, insert another cache_size rows
        inserted_rows_second_time = []
        for i in range(cache_size + 4, 4 + 2 * cache_size):
            row = {"id": i, "title": "Title " + str(i), "genre": "Genre" }
            self.test_dimension.insert(row)
            inserted_rows_second_time.append(row)

        self.connection_wrapper.close()

        # None of the inserted rows in the first round should still be cached
        for row in inserted_rows_first_time:
            self.assertRaises(Exception, self.test_dimension.lookup, row)

        # The rows inserted in the second round should still be in the cache
        for row in inserted_rows_second_time:
            self.assertEqual(row["id"], self.test_dimension.lookup(row))

    def test_size_custom_infinite(self):
        number_of_rows_to_insert = 12000
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=False,
                                              cacheoninsert=True,
                                              size=-1)

        # Insert a number of new rows into the dimension (number of inserted
        # rows is equal to the cache size). The loop starts from 6 because the
        # keys 1-5 are already in use
        inserted_rows = []
        for i in range(6, number_of_rows_to_insert + 4):
            row = {"id": i, "title": "Title " + str(i), "genre": "Genre" }
            self.test_dimension.insert(row)
            inserted_rows.append(row)

        self.connection_wrapper.close()

        for row in inserted_rows:
            self.assertEqual(row["id"], self.test_dimension.lookup(row))

    def test_lookup_cache_initially_empty(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=False)

        self.connection_wrapper.close()

        # The cache should initially be empty because prefill is False and no
        # lookups have been made
        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    { "id": 3, "title": "Calvin and Hobbes One", "genre": "Comic" },
                    { "id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic" },
                    { "id": 5, "title": "The Silver Spoon", "genre": "Cookbook" },
                    ]:
            self.assertRaises(Exception, self.test_dimension.lookup, row)

    def test_lookup_cache_after_some_lookups(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=False)

        # Lookup two rows, afterwards they should now cached
        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    ]:
            self.test_dimension.lookup(row)

        self.connection_wrapper.close()

        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    ]:
            self.assertEqual(row["id"], self.test_dimension.lookup(row))

        # However, the other row should not be in the cache
        for row in [{ "id": 3, "title": "Calvin and Hobbes One", "genre": "Comic" },
                    { "id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic" },
                    { "id": 5, "title": "The Silver Spoon", "genre": "Cookbook" },
                    ]:
            self.assertRaises(Exception, self.test_dimension.lookup, row)

    def test_getbykey_cache_initially_empty(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=False)

        self.connection_wrapper.close()

        # The cache should initially be empty because prefill is False and no
        # lookups have been made
        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    { "id": 3, "title": "Calvin and Hobbes One", "genre": "Comic" },
                    { "id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic" },
                    { "id": 5, "title": "The Silver Spoon", "genre": "Cookbook" },
                    ]:
            self.assertRaises(Exception, self.test_dimension.getbykey, row["id"])

    def test_getbykey_cache_after_some_lookups(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=False,
                                              cachefullrows=True)

        # Lookup two rows, afterwards they should now cached
        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    ]:
            self.test_dimension.getbykey(row["id"])

        self.connection_wrapper.close()

        for row in [{ "id": 1, "title": "Unknown", "genre": "Unknown" },
                    { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
                    ]:
            self.assertDictEqual(row, self.test_dimension.getbykey(row["id"]))

        # However, this key should not be in the cache
        self.assertRaises(Exception, self.test_dimension.getbykey, 3)

    # Everything is cached since prefill and cacheonsinsert are true and as long
    # the cache size has not been exceeded
    def test_update_caching_where_prefill_and_cacheoninsert_are_true_and_cache_size_is_not_exceeded(self):
        cache_size = 100
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=True,
                                              cachefullrows=True,
                                              size=cache_size)

        # Update the attributes of the first row
        updated_row = { "id": 1, "title": "Error", "genre": "Error" }
        self.test_dimension.update(updated_row)

        self.connection_wrapper.close()

        # The row in the cache should have been updated
        self.assertDictEqual(updated_row, self.test_dimension.getbykey(1))
        self.assertEqual(1, self.test_dimension.lookup(updated_row))

        # The old row should no longer be in the cache
        self.assertIsNone(self.test_dimension.lookup(
            { "id": 1, "title": "Unknown", "genre": "Unknown" }))

    # Not all rows in the dimension are necessarily cached
    def test_update_caching_where_cacheoninsert_is_false(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=True,
                                              cacheoninsert=False,
                                              cachefullrows=True)

        # Update the attributes of the first row
        updated_row = { "id": 1, "title": "Error", "genre": "Error" }
        self.test_dimension.update(updated_row)

        self.connection_wrapper.close()

        # Both the new and old row should have been deleted from the cache
        self.assertRaises(Exception, self.test_dimension.lookup,
                          { "title": "Unknown", "genre": "Unknown" })
        self.assertRaises(Exception, self.test_dimension.lookup,
                          { "title": "Error", "genre": "Error" })
        self.assertRaises(Exception, self.test_dimension.getbykey, 1)

    def test_defaultidvalue(self):
        self.test_dimension = CachedDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              prefill=True,
                                              defaultidvalue="unknown")

        non_existing_row = {"title": "Title", "genre": "Genre"}
        self.assertEqual("unknown", self.test_dimension.lookup(non_existing_row))

        existing_row = { "id": 1, "title": "Unknown", "genre": "Unknown" }
        self.assertEqual(1, self.test_dimension.lookup(existing_row))


class BulkDimensionTest(DimensionTest):

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.test_dimension = BulkDimension(name=self.initial.name,
                                            key=self.initial.key(),
                                            attributes=self.initial.attributes,
                                            bulkloader=self.loader)

    # Get an instance of the class being tested by this TestCase
    def get_test_subject(self, **specifics):
        args = {'name':self.initial.name, 'key':self.initial.key(),
                'attributes':self.initial.attributes, 'bulkloader':self.loader}
        args.update(specifics)
        return BulkDimension(**args)

    def loader(self, name, attributes, fieldsep, rowsep, nullval, filehandle):
        sql = "INSERT INTO book(id, title, genre) VALUES({}, '{}', '{}')"
        encoding = utilities.get_os_encoding()
        for line in filehandle:
            values = line.decode(encoding).strip().split('\t')
            insert = sql.format(*values)
            self.connection_wrapper.execute(insert)

    def test_awaitingempty(self):
        self.assertEqual(self.test_dimension.awaitingrows, 0)

    def test_awaiting_insert(self):
        expected = self.initial \
            + "| 6 | Book 1 | Genre |" \
            + "| 7 | Book 2 | Genre |" \
            + "| 8 | Book 3 | Genre |"
        [self.test_dimension.insert(row) for row in expected.additions(withKey=True)]
        self.assertEqual(self.test_dimension.awaitingrows, 3)

    def test_awaiting_insert_commit(self):
        self.initial.reset()
        expected = self.initial \
            + "| 6 | Book 1 | Genre |" \
            + "| 7 | Book 2 | Genre |" \
            + "| 8 | Book 3 | Genre |"
        [self.test_dimension.insert(row) for row in expected.additions(withKey=True)]
        self.assertEqual(self.test_dimension.awaitingrows, 3)
        self.connection_wrapper.commit()
        self.assertEqual(self.test_dimension.awaitingrows, 0)


class CachedBulkDimensionTest(BulkDimensionTest):

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.test_dimension = CachedBulkDimension(name=self.initial.name,
                                                  key=self.initial.key(),
                                                  attributes=self.initial.attributes,
                                                  bulkloader=self.loader)

    # Get an instance of the class being tested by this TestCase
    def get_test_subject(self, **specifics):
        args = {'name':self.initial.name, 'key':self.initial.key(),
                'attributes':self.initial.attributes, 'bulkloader':self.loader}
        args.update(specifics)
        return CachedBulkDimension(**args)



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
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.test_dimension = SlowlyChangingDimension(
            name=self.initial.name,
            key=self.initial.key(),
            attributes=self.initial.attributes,
            lookupatts=['name'],
            versionatt='version',
            fromatt='fromdate',
            toatt='todate',
            type1atts=['age'],
            srcdateatt='from',
            cachesize=100,
            prefill=True)

        self.scdimension = SlowlyChangingDimension(
            name=self.initial.name,
            key=self.initial.key(),
            attributes=self.initial.attributes,
            lookupatts=['name', 'age'],
            versionatt='version',
            fromatt='fromdate',
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

    def generate_row_with_missing_name_attribute(self):
        return { "city": "Aalborg" }

    # Return a row containing a subset of the attributes and the number of rows
    # that occur in self.initial with the given attributes
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

    # Return a row that does not exist in the test dimension. The row is
    # returned with or without the key depending on the withkey argument
    def generate_nonexisting_row(self, withkey=False):
        row = {}

        if withkey:
            row["id"] = 5

        row.update({
            "name": "Dan",
            "age": 45,
            "city": "Dublin",
            "fromdate": "2010-01-02",
            "todate": "2010-03-04",
            "version": 1
        })
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

    def generate_multiple_nonexisting_rows(self):
        return [{"id": 5, "name": "Dan", "age": 45, "city": "Dublin",
                 "fromdate": "2010-01-02", "todate": "2010-03-04",
                 "version": 1},
                {"id": 6, "name": "Eric", "age": 50, "city": "Ebeltoft",
                 "fromdate": "2010-02-02", "todate": "2010-02-02",
                 "version": 1}]

    def test_type1atts_must_be_attributes(self):
        with self.assertRaises(ValueError):
            SlowlyChangingDimension(
                name=self.initial.name,
                key=self.initial.key(),
                attributes=self.initial.attributes,
                lookupatts=['name'],
                versionatt='version',
                fromatt='fromdate',
                toatt='todate',
                type1atts=['surname'],
                srcdateatt='from',
                cachesize=100,
                prefill=True)

    def test_type1atts_cannot_be_lookupatts(self):
        with self.assertRaises(ValueError):
            SlowlyChangingDimension(
                name=self.initial.name,
                key=self.initial.key(),
                attributes=self.initial.attributes,
                lookupatts=['name', "age"],
                versionatt='version',
                fromatt='fromdate',
                toatt='todate',
                type1atts=['age'],
                srcdateatt='from',
                cachesize=100,
                prefill=True)

    # The lookup method in Dimension is overridden in SlowlyChangingDimension
    def test_lookup(self):
        postcondition = self.initial

        row = {"name": "Ann"}
        expected_key = 3  # Newest version has key = 3
        actual_key = self.test_dimension.lookup(row)
        self.connection_wrapper.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition = self.initial

        namemapped_row = {"firstname": "Ann"}
        expected_key = 3
        actual_key = self.test_dimension.lookup(
            namemapped_row, namemapping={"name": "firstname"})
        self.connection_wrapper.commit()

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_lookup_with_lookupatts(self):
        postcondition = self.initial

        key = self.scdimension.lookup({'name': 'Ann', 'age': 20})
        self.assertEqual(3, key)

        # The row is missing a lookup attribute
        self.assertRaises(KeyError, self.scdimension.lookup, {'name': 'Ann'})

        postcondition.assertEqual()

    def test_lookup_with_lookupatts_with_custom_quotechar(self):
        # The identifiers are now wrapped with ""
        pygrametl.tables.definequote('\"')

        postcondition = self.initial

        self.assertEqual(3, self.scdimension.lookup({'name': 'Ann', 'age': 20}))

        # The row is missing a lookup attribute
        self.assertRaises(KeyError, self.scdimension.lookup, {'name': 'Ann'})

        postcondition.assertEqual()

        # The quotechar function is reset to default with no wrapping
        pygrametl.tables.definequote(None)

    def test_lookup_nonexisting_row(self):
        postcondition = self.initial
        row = {"name": "Peter"}

        self.assertIsNone(self.test_dimension.lookup(row))
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_lookup_with_missing_atttributes(self):
        postcondition = self.initial
        row = self.generate_row_with_missing_name_attribute()

        self.assertRaises(KeyError, self.test_dimension.lookup, row)
        self.connection_wrapper.commit()

        postcondition.assertEqual()

    def test_lookuprow_with_lookupatts(self):
        postcondition = self.initial
        actual_row = self.scdimension.lookuprow({'name':'Ann', 'age':20})
        self.connection_wrapper.commit()

        expected_row = {'id':3, 'name':'Ann', 'age':20, 'city':'Aarhus', \
                        'fromdate':'2010-03-03', 'todate':None, 'version':2}
        self.assertDictEqual(expected_row, actual_row)
        postcondition.assertEqual()

    def test_lookuprow_with_lookupatts_nonexisting_row(self):
        postcondition = self.initial
        nonexisting = self.generate_nonexisting_row(withkey=False)
        result = self.test_dimension.lookuprow(nonexisting)
        self.connection_wrapper.commit()

        for att in result:
            self.assertIsNone(result[att])

        postcondition.assertEqual()

    def test_scdensure_existing_row(self):
        row = {'name': 'Ann', 'age': 20,
               'city': 'Aarhus', 'from': '2010-03-03'}
        postcondition = self.initial

        self.test_dimension.scdensure(row)
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

        self.test_dimension.scdensure(row, namemapping=self.namemapping)
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
        self.test_dimension.scdensure(row)
        expected_key = 5
        actual_key = row["id"]  # The key is added to the row by scdensure

        self.assertEqual(expected_key, actual_key)
        postcondition.assertEqual()

    def test_scdensure_type1_change_existing_row(self):
        # The type 1 slowly changing attribute age should be 21 in all rows
        postcondition = \
            self.initial.update(0, "| 1 | Ann | 21 | Aalborg | 2010-01-01 | 2010-03-03 | 1 |") \
                        .update(2, "| 3 | Ann | 21 | Aarhus  | 2010-03-03 | NULL | 2 |")

        self.test_dimension.scdensure(
            {'name': 'Ann', 'age': 21, 'city': 'Aarhus', 'from': '2010-03-03'})
        postcondition.assertEqual()

    def test_scdensure_type1_change_all_rows(self):
        # A new row should be inserted for Ann and age should be 21 in all rows
        postcondition = self.initial.update(0, "| 1 | Ann | 21 | Aalborg | 2010-01-01 | 2010-03-03 | 1 |") \
                                    .update(2, "| 3 | Ann | 21 | Aarhus  | 2010-03-03 | 2010-04-04 | 2 |") \
                                    + "| 5 | Ann | 21 | Aabenraa  | 2010-04-04 | NULL | 3 |"

        self.test_dimension.scdensure(
            {'name': 'Ann', 'age': 21, 'city': 'Aabenraa', 'from': '2010-04-04'})

        postcondition.assertEqual()

    def test_scdensure_type1_change_only_latest_rows(self):
        # No new rows should be inserted for Ann and age should be 21 in the latest row
        postcondition = self.initial.update(2, "| 3 | Ann | 21 | Aarhus | 2010-03-03 | NULL | 2 |")
        self.test_dimension.type1attsupdateall['age'] = False  # Only update the latest version

        self.test_dimension.scdensure(
            {'name': 'Ann', 'age': 21, 'city': 'Aarhus', 'from': '2010-03-03'})

        postcondition.assertEqual()

    def test_scdensure_type1_and_type2_change_only_latest_rows(self):
        # No new rows should be inserted for Ann and age should be 21 in the latest row
        postcondition = self.initial.update(2, "| 3 | Ann | 20 | Aarhus  | 2010-03-03 | 2010-04-05 | 2 |") \
                                    + "| 5 | Ann | 21 | Aalborg | 2010-04-05 | NULL | 3 |"
        self.test_dimension.type1attsupdateall['age'] = False  # Only update the latest version

        self.test_dimension.scdensure(
            {'name': 'Ann', 'age': 21, 'city': 'Aalborg', 'from': '2010-04-05'})

        postcondition.assertEqual()

    def test_scdensure_two_newversions(self):
        postcondition = self.initial.update(
            2, "| 3 | Ann | 20 | Aarhus | 2010-03-03 | 2010-04-04 | 2 |") \
            + "| 5 | Ann | 20 | Aalborg  | 2010-04-04 | 2010-05-05 | 3 |" \
            + "| 6 | Ann | 20 | Aabenraa | 2010-05-05 | NULL       | 4 |"

        self.test_dimension.scdensure(
            {'name': 'Ann', 'age': 20, 'city': 'Aalborg', 'from': '2010-04-04'})
        self.test_dimension.scdensure(
            {'name': 'Ann', 'age': 20, 'city': 'Aabenraa', 'from': '2010-05-05'})

        postcondition.assertEqual()

    def test_scdensure_new_row(self):
        postcondition = self.initial \
            + "| 5 | Doris | 85 | Dublin | 2010-04-04 | NULL | 1 |"

        self.test_dimension.scdensure(
            {'name': 'Doris', 'age': 85, 'city': 'Dublin', 'from': '2010-04-04'})

        postcondition.assertEqual()

    def test_scdensure_side_effects(self):
        scdimension = SlowlyChangingDimension(name=self.initial.name,
                                              key=self.initial.key(),
                                              attributes=self.initial.attributes,
                                              lookupatts=['name'],
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
        self.test_dimension.closecurrent({'name': 'Ann'}, end='2010-04-04')
        postcondition.assertEqual()

    def test_closecurrent_and_lookup(self):
        self.test_closecurrent()
        keyval = self.test_dimension.lookup({'name': 'Ann'})
        self.assertEqual(keyval, 3)

    def test_closecurrent_and_scdensure_newversion(self):
        postcondition = self.initial.update(
            2, "| 3 | Ann | 20 | Aarhus  | 2010-03-03 | 2010-04-04 | 2 |") \
            + "| 5 | Ann | 20 | Aabenraa| 2010-05-05 | NULL       | 3 | "

        self.test_dimension.closecurrent({'name': 'Ann'}, end='2010-04-04')
        self.test_dimension.scdensure(
            {'name': 'Ann', 'age': 20, 'city': 'Aabenraa', 'from': '2010-05-05'})

        postcondition.assertEqual()

    def test_closecurrent_and_scdensure_newversion_and_lookup(self):
        self.test_closecurrent_and_scdensure_newversion()
        keyval = self.test_dimension.lookup({'name': 'Ann'})
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

        self.test_dimension = SlowlyChangingDimension(name=self.initial.name,
                                                      key=self.initial.key(),
                                                      attributes=self.initial.attributes,
                                                      lookupatts=['name'],
                                                      orderingatt='version',
                                                      cachesize=100,
                                                      prefill=True)

        self.assertEqual(1, self.test_dimension.lookup({"name": "Ann"}))

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

        self.test_dimension = SlowlyChangingDimension(name=self.initial.name,
                                                      key=self.initial.key(),
                                                      attributes=self.initial.attributes,
                                                      lookupatts=['name'],
                                                      versionatt='version',
                                                      cachesize=100,
                                                      prefill=True)

        self.assertEqual(1, self.test_dimension.lookup({"name": "Ann"}))

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

        # orderingatt should take precedence over versionatt if both are defined
        self.test_dimension = SlowlyChangingDimension(name=self.initial.name,
                                                      key=self.initial.key(),
                                                      attributes=self.initial.attributes,
                                                      lookupatts=['name'],
                                                      versionatt='version',
                                                      orderingatt='number',
                                                      cachesize=100,
                                                      prefill=True)

        self.assertEqual(3, self.test_dimension.lookup({"name": "Ann"}))

    def test_orderingatt_versionatt_fromatt_toatt_are_all_none(self):
        self.assertRaises(ValueError, SlowlyChangingDimension, name=self.initial.name,
                          key=self.initial.key(),
                          attributes=self.initial.attributes,
                          lookupatts=['name'],
                          cachesize=100,
                          prefill=True)

    def test_minfrom(self):
        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | fromdate:timestamp | city:varchar | version:int |
        | ----------- | ------------ | ------- | ------------------ | ------------ | ----------- |
        | 1           | Ann          | 22      | 2010-01-01         | Aalborg      | 1           |
        | 2           | Bob          | 31      | 2010-02-02         | Boston       | 1           |
        """)
        self.initial.reset()

        # minfrom is set to a value
        self.test_dimension = SlowlyChangingDimension(name=self.initial.name,
                                                      key=self.initial.key(),
                                                      attributes=self.initial.attributes,
                                                      lookupatts=['name'],
                                                      versionatt='version',
                                                      fromatt='fromdate',
                                                      minfrom='2012-12-12',
                                                      cachesize=100,
                                                      prefill=True)

        self.test_dimension.scdensure(
            {'name': 'Charlie', 'age': 24, 'city': 'Copenhagen'})

        postcondition = self.initial \
            + "| 3 | Charlie | 24 | 2012-12-12 | Copenhagen | 1 |"
        postcondition.assertEqual()

    def test_minfrom_is_ignored(self):
        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | fromdate:timestamp | city:varchar | version:int |
        | ----------- | ------------ | ------- | ------------------ | ------------ | ----------- |
        | 1           | Ann          | 22      | 2010-01-01         | Aalborg      | 1           |
        | 2           | Bob          | 31      | 2010-02-02         | Boston       | 1           |
        """)
        self.initial.reset()

        # minto is set to a value, it should be ignored when inserting the row
        # since it already contains a fromatt value
        self.test_dimension = SlowlyChangingDimension(name=self.initial.name,
                                                      key=self.initial.key(),
                                                      attributes=self.initial.attributes,
                                                      lookupatts=['name'],
                                                      versionatt='version',
                                                      fromatt='fromdate',
                                                      minfrom='2012-12-12',
                                                      cachesize=100,
                                                      prefill=True)

        self.test_dimension.scdensure({'name': 'Charlie', 'age': 24,
                                       'city': 'Copenhagen',
                                       'fromdate': '2010-03-03'})

        postcondition = self.initial \
            + "| 3 | Charlie | 24 | 2010-03-03 | Copenhagen | 1 |"
        postcondition.assertEqual()

    def test_maxto(self):
        self.initial = dtt.Table("customers", """
        | id:int (pk) | name:varchar | age:int | city:varchar | fromdate:timestamp | todate:timestamp | version:int |
        | ----------- | ------------ | ------- | ------------ | ------------------ | ---------------- | ----------- |
        | 1           | Ann          | 20      | Aalborg      | 2010-01-01         | 2099-12-12       | 1           |
        | 2           | Bob          | 31      | Boston       | 2010-02-02         | 2099-12-12       | 1           |
        """)
        self.initial.reset()

        # maxto is set to a value
        self.test_dimension = SlowlyChangingDimension(name=self.initial.name,
                                                      key=self.initial.key(),
                                                      attributes=self.initial.attributes,
                                                      lookupatts=['name'],
                                                      versionatt='version',
                                                      fromatt='fromdate',
                                                      srcdateatt='from',
                                                      toatt='todate',
                                                      maxto='2099-12-12',
                                                      cachesize=100,
                                                      prefill=True)

        self.test_dimension.scdensure(
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
        self.test_dimension = SlowlyChangingDimension(name=self.initial.name,
                                                      key=self.initial.key(),
                                                      attributes=self.initial.attributes,
                                                      lookupatts=['name'],
                                                      versionatt='version',
                                                      idfinder=self.mock_idfinder,
                                                      cachesize=100,
                                                      prefill=True)

        self.test_dimension.scdensure(
            {'name': 'Charlie', 'age': 30, 'city': 'Copenhagen'})

        postcondition = self.initial + "| 99 | Charlie | 30 | Copenhagen | 1 |"
        postcondition.assertEqual()


class SnowflakedDimensionTest(unittest.TestCase):

    def setUp(self):
        utilities.ensure_default_connection_wrapper()

        self.year_dt = dtt.Table("year", """
        | yid:int (pk) | year:int |
        | ------------ | -------- |
        | 1            | 2000     |
        | 2            | 2001     |
        | 3            | 2002     |
        """)

        self.month_dt = dtt.Table("month", """
        | mid:int (pk) | month:varchar   | yid:int (fk year(yid)) |
        | ------------ | --------------- | ---------------------- |
        | 1            | January 2000    | 1                      |
        | 2            | February 2000   | 1                      |
        | 13           | January 2001    | 2                      |
        | 25           | January 2002    | 3                      |
        """)

        self.day_dt = dtt.Table("day", """
        | did:int (pk) | day:varchar      | mid:int (fk month(mid)) |
        | ------------ | ---------------- | ----------------------- |
        | 1            | January 1, 2000  | 1                       |
        | 32           | February 1, 2000 | 2                       |
        | 33           | February 2, 2000 | 2                       |
        | 366          | January 1, 2001  | 13                      |
        | 731          | January 1, 2002  | 25                      |
        """)

        self.year_dt.reset()
        self.month_dt.reset()
        self.day_dt.reset()

        self.connection_wrapper = pygrametl.getdefaulttargetconnection()

        self.day_dimension = Dimension(name=self.day_dt.name,
                                       key=self.day_dt.key(),
                                       attributes=self.day_dt.attributes)

        self.month_dimension = Dimension(name=self.month_dt.name,
                                         key=self.month_dt.key(),
                                         attributes=self.month_dt.attributes)

        self.year_dimension = Dimension(name=self.year_dt.name,
                                        key=self.year_dt.key(),
                                        attributes=self.year_dt.attributes)

        self.snowflaked_dimension = SnowflakedDimension(
            [(self.day_dimension, self.month_dimension),
             (self.month_dimension, self.year_dimension)])

    def test_lookup(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertEqual(1, self.snowflaked_dimension.lookup(
            {"day": "January 1, 2000", "mid": 1}))
        self.assertEqual(32, self.snowflaked_dimension.lookup(
            {"day": "February 1, 2000", "mid": 2}))
        self.assertEqual(33, self.snowflaked_dimension.lookup(
            {"day": "February 2, 2000", "mid": 2}))
        self.assertEqual(366, self.snowflaked_dimension.lookup(
            {"day": "January 1, 2001", "mid": 13}))
        self.assertEqual(731, self.snowflaked_dimension.lookup(
            {"day": "January 1, 2002", "mid": 25}))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_lookupatts(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        # Change the day dimension so it only uses day as lookup attribute
        self.day_dimension = Dimension(name=self.day_dt.name,
                                       key=self.day_dt.key(),
                                       attributes=self.day_dt.attributes,
                                       lookupatts=["day"])

        self.snowflaked_dimension = SnowflakedDimension(
            [(self.day_dimension, self.month_dimension),
             (self.month_dimension, self.year_dimension)])

        self.assertEqual(1, self.snowflaked_dimension.lookup(
            {"day": "January 1, 2000"}))
        self.assertEqual(32, self.snowflaked_dimension.lookup(
            {"day": "February 1, 2000"}))
        self.assertEqual(33, self.snowflaked_dimension.lookup(
            {"day": "February 2, 2000"}))
        self.assertEqual(366, self.snowflaked_dimension.lookup(
            {"day": "January 1, 2001"}))
        self.assertEqual(731, self.snowflaked_dimension.lookup(
            {"day": "January 1, 2002"}))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"day": "date"}

        self.assertEqual(1, self.snowflaked_dimension.lookup(
            {"date": "January 1, 2000", "mid": 1}, namemapping=namemapping))
        self.assertEqual(32, self.snowflaked_dimension.lookup(
            {"date": "February 1, 2000", "mid": 2}, namemapping=namemapping))
        self.assertEqual(33, self.snowflaked_dimension.lookup(
            {"date": "February 2, 2000", "mid": 2}, namemapping=namemapping))
        self.assertEqual(366, self.snowflaked_dimension.lookup(
            {"date": "January 1, 2001", "mid": 13}, namemapping=namemapping))
        self.assertEqual(731, self.snowflaked_dimension.lookup(
            {"date": "January 1, 2002", "mid": 25}, namemapping=namemapping))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_nonexisting_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertIsNone(self.snowflaked_dimension.lookup(
            {"day": "January 45, 2099", "mid": 1}))
        self.assertIsNone(self.snowflaked_dimension.lookup(
            {"day": "Non-existing row", "mid": -1}))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_lookup_with_missing_attributes(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertRaises(KeyError, self.snowflaked_dimension.lookup,
                          { "day": "January 1, 2000"})
        self.assertRaises(KeyError, self.snowflaked_dimension.lookup,
                          {"mid": 2})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbykey(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertDictEqual({"did": 1, "day": "January 1, 2000", "mid": 1},
                             self.snowflaked_dimension.getbykey(1))
        self.assertDictEqual({"did": 32, "day": "February 1, 2000", "mid": 2},
                             self.snowflaked_dimension.getbykey(32))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbykey_nonexisting_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertDictEqual(
            {"did": None, "day": None, "mid": None},
            self.snowflaked_dimension.getbykey(99))
        self.assertDictEqual(
            {"did": None, "day": None, "mid": None},
            self.snowflaked_dimension.getbykey(-1))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbykey_with_fullrow(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        expected_fullrow = {"did": 1, "day": "January 1, 2000", "mid": 1,
                            "month": "January 2000", "yid": 1, "year": 2000}

        self.assertDictEqual(
            expected_fullrow,
            self.snowflaked_dimension.getbykey(1, fullrow=True))

        expected_fullrow = {"did": 731, "day": "January 1, 2002",
                            "mid": 25, "month": "January 2002",
                            "yid": 3, "year": 2002}

        self.assertDictEqual(
            expected_fullrow,
            self.snowflaked_dimension.getbykey(731, fullrow=True))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        vals = {"mid": 2}
        expected_rows = [
            {"did": 32, "day": "February 1, 2000", "mid": 2},
            {"did": 33, "day": "February 2, 2000", "mid": 2}
        ]
        actual_rows = self.snowflaked_dimension.getbyvals(vals, fullrow=False)

        self.assertEqual(len(expected_rows), len(actual_rows))
        for actual_row in actual_rows:
            self.assertTrue(actual_row in expected_rows)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals_with_fullrow(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        vals = {"mid": 2}
        expected_rows = [
            {"did": 32, "day": "February 1, 2000", "mid": 2,
             "month": "February 2000", "yid": 1, "year": 2000},
            {"did": 33, "day": "February 2, 2000", "mid": 2,
             "month": "February 2000", "yid": 1, "year": 2000},
        ]
        actual_rows = self.snowflaked_dimension.getbyvals(vals, fullrow=True)

        self.assertEqual(len(expected_rows), len(actual_rows))
        for actual_row in actual_rows:
            self.assertTrue(actual_row in expected_rows)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals_none(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        vals = {"mid": 99}
        expected_num_of_rows = 0

        rows = self.snowflaked_dimension.getbyvals(vals, fullrow=False)

        self.assertEqual(expected_num_of_rows, len(rows))

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_getbyvals_with_namemapping(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"mid": "month_id"}
        vals = {"month_id": 2}
        expected_rows = [
            {"did": 32, "day": "February 1, 2000", "mid": 2},
            {"did": 33, "day": "February 2, 2000", "mid": 2}
        ]
        actual_rows = self.snowflaked_dimension.getbyvals(
            vals, fullrow=False, namemapping=namemapping)

        self.assertEqual(len(expected_rows), len(actual_rows))
        for actual_row in actual_rows:
            self.assertTrue(actual_row in expected_rows)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_without_foreign_keys(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        updated_row = {"did": 33, "day": "February 2nd in year 2000"}
        self.snowflaked_dimension.update(updated_row)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_with_foreign_key_and_with_foreign_nonkey_atts_present_in_row(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt.update(
            1, "| 2 | February in year 2000 | 1 |")
        postcondition_year = self.year_dt

        # The attributes from the table month present in the row should be updated
        updated_row = {"did": 33, "mid": 2,
                       "day": "February 2nd in year 2000",
                       "month": "February in year 2000"}
        self.snowflaked_dimension.update(updated_row)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_with_foreign_key_and_but_without_foreign_atts_present_in_row(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        # Nothing should be updated in the referenced table month
        updated_row = {"did": 33, "mid": 2,
                       "day": "February 2nd in year 2000"}
        self.snowflaked_dimension.update(updated_row)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_a_child_dimensionension_only(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt.update(
            0, "| 1 | January in 2000 | 1 |")
        postcondition_year = self.year_dt

        updated_row = {"mid": 1, "month": "January in 2000"}
        self.snowflaked_dimension.update(updated_row)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_non_existing_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        updated_row = {"did": 9999, "day": "Nothing should be updated"}
        self.snowflaked_dimension.update(updated_row)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_do_nothing(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        # No attributes are changed
        self.snowflaked_dimension.update({"did": 1})

        # No key of a participating dimension is mentioned
        self.snowflaked_dimension.update(
            {"day": "Day", "month": "Month", "year": "Year"})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_update_with_namemapping(self):
        postcondition_day = self.day_dt.update(
            2, "| 33 | February 2nd in year 2000 | 2 |")
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"day": "date"}
        updated_row = {"did": 33, "date": "February 2nd in year 2000"}

        self.snowflaked_dimension.update(updated_row, namemapping=namemapping)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_only_new_row_in_root(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.snowflaked_dimension.ensure(
            {"did": 5, "day": "January 5, 2000",
             "month": "January 2000", "year": 2000})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_new_row_in_all_dimensionensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |"
        postcondition_month = self.month_dt + "| 39 | March 2003 | 4 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |"

        self.snowflaked_dimension.ensure({"did": 1200, "day": "March 3, 2003",
                                          "mid": 39, "month": "March 2003",
                                          "year": 2003})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_two_new_rows_in_all_dimensionensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |" + \
            "| 1700 | April 4, 2004  | 52 |"
        postcondition_month = self.month_dt + \
            "| 39 | March 2003 | 4 |" + "| 52 | April 2004 | 5 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |" + "| 5 | 2004 |"

        self.snowflaked_dimension.ensure({"did": 1200, "day": "March 3, 2003",
                                          "mid": 39, "month": "March 2003",
                                          "year": 2003})
        self.snowflaked_dimension.ensure({"did": 1700, "day": "April 4, 2004",
                                          "mid": 52, "month": "April 2004",
                                          "year": 2004})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_existing_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.snowflaked_dimension.ensure(
            {"day": "January 1, 2000", "month": "January 2000", "year": 2000})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_nonexisting_row_with_namemapping(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"day": "date"}

        self.snowflaked_dimension.ensure(
            {"did": 5, "date": "January 5, 2000",
             "month": "January 2000", "year": 2000},
            namemapping=namemapping)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_ensure_existing_row_with_namemapping(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"day": "date"}

        self.snowflaked_dimension.ensure(
            {"date": "January 1, 2000",
             "month": "January 2000", "year": 2000},
            namemapping=namemapping)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_only_new_row_in_root(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.snowflaked_dimension.insert(
            {"did": 5, "day": "January 5, 2000",
             "month": "January 2000", "year": 2000})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_new_row_in_all_dimensionensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |"
        postcondition_month = self.month_dt + "| 39 | March 2003 | 4 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |"

        self.snowflaked_dimension.insert({"did": 1200, "day": "March 3, 2003",
                                          "mid": 39, "month": "March 2003",
                                          "year": 2003})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_two_new_rows_in_all_dimensionensions(self):
        postcondition_day = self.day_dt + "| 1200 | March 3, 2003  | 39 |" + \
            "| 1700 | April 4, 2004  | 52 |"
        postcondition_month = self.month_dt + \
            "| 39 | March 2003 | 4 |" + "| 52 | April 2004 | 5 |"
        postcondition_year = self.year_dt + "| 4 | 2003 |" + "| 5 | 2004 |"

        self.snowflaked_dimension.insert({"did": 1200, "day": "March 3, 2003",
                                          "mid": 39, "month": "March 2003",
                                          "year": 2003})
        self.snowflaked_dimension.insert({"did": 1700, "day": "April 4, 2004",
                                          "mid": 52, "month": "April 2004",
                                          "year": 2004})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_existing_row(self):
        postcondition_day = self.day_dt
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        self.assertRaises(Exception, self.snowflaked_dimension.insert,
                          {"day": "January 1, 2000", "month": "January 2000",
                           "year": 2000})

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()

    def test_insert_new_row_with_namemapping(self):
        postcondition_day = self.day_dt + "| 5 | January 5, 2000  | 1 |"
        postcondition_month = self.month_dt
        postcondition_year = self.year_dt

        namemapping = {"day": "date"}

        self.snowflaked_dimension.insert(
            {"did": 5, "date": "January 5, 2000",
             "month": "January 2000", "year": 2000},
            namemapping=namemapping)

        self.connection_wrapper.commit()
        postcondition_day.assertEqual()
        postcondition_month.assertEqual()
        postcondition_year.assertEqual()


class SlowlyChangingDimensionLookupasofTest(unittest.TestCase):

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()

    def test_lookupasof_usingto(self):
        table = dtt.Table("customers", """
        | id:int (pk) | name:varchar | city:varchar | todate:timestamp | version:int |
        | ----------- | ------------ | ------------ | ---------------- | ----------- |
        | 1           | Ann          | Aalborg      | 2001-12-31       | 1           |
        | 2           | Bob          | Boston       | 2001-12-31       | 1           |
        | 3           | Ann          | Aarhus       | 2002-12-31       | 2           |
        | 4           | Charlie      | Copenhagen   | NULL             | 1           |
        | 5           | Ann          | Aabenraa     | NULL             | 3           |
        | 6           | Bob          | Birkelse     | 2002-12-31       | 2           |
        """)
        table.reset()
        test_dimension = SlowlyChangingDimension(
            name=table.name,
            key=table.key(),
            attributes=table.attributes,
            lookupatts=['name'],
            versionatt='version',
            toatt='todate',
            cachesize=100,
            prefill=True)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-12-31", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-12-31", False)
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof({'name':'Ann'}, "2222-12-31", True)
        self.assertEqual(key, 5)
        key = test_dimension.lookupasof({'name':'Bob'}, "2222-12-31", True)
        self.assertEqual(key, None)

    def test_lookupasof_usingto_noversion(self):
        table = dtt.Table("customers", """
        | id:int (pk) | name:varchar | city:varchar | todate:timestamp |
        | ----------- | ------------ | ------------ | ---------------- |
        | 1           | Ann          | Aalborg      | 2001-12-31       |
        | 2           | Bob          | Boston       | 2001-12-31       |
        | 3           | Ann          | Aarhus       | 2002-12-31       |
        | 4           | Charlie      | Copenhagen   | NULL             |
        | 5           | Ann          | Aabenraa     | NULL             |
        | 6           | Bob          | Birkelse     | 2002-12-31       |
        """)
        table.reset()
        test_dimension = SlowlyChangingDimension(
            name=table.name,
            key=table.key(),
            attributes=table.attributes,
            lookupatts=['name'],
            toatt='todate',
            cachesize=100,
            prefill=True)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-12-31", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-12-31", False)
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof({'name':'Ann'}, "2222-12-31", True)
        self.assertEqual(key, 5)
        key = test_dimension.lookupasof({'name':'Bob'}, "2222-12-31", True)
        self.assertEqual(key, None)
        
    def test_lookupasof_usingfrom(self):
        table = dtt.Table("customers", """
        | id:int (pk) | name:varchar | city:varchar | fromdate:timestamp | version:int |
        | ----------- | ------------ | ------------ | ------------------ | ----------- |
        | 0           | Ann          | Arden        | NULL               | 1           |
        | 1           | Ann          | Aalborg      | 2001-01-01         | 2           |
        | 2           | Bob          | Boston       | 2001-01-01         | 1           |
        | 3           | Ann          | Aarhus       | 2002-01-01         | 3           |
        | 4           | Charlie      | Copenhagen   | 2001-01-01         | 1           |
        | 5           | Ann          | Aabenraa     | 2003-01-01         | 4           |
        | 6           | Bob          | Birkelse     | 2002-01-01         | 2           |
        """)
        table.reset()
        test_dimension = SlowlyChangingDimension(
            name=table.name,
            key=table.key(),
            attributes=table.attributes,
            lookupatts=['name'],
            versionatt='version',
            fromatt='fromdate',
            cachesize=100,
            prefill=True)
        key = test_dimension.lookupasof({'name':'Bob'}, "1999-05-05", True)
        self.assertEqual(key, None)
        key = test_dimension.lookupasof({'name':'Ann'}, "1999-12-31", True)
        self.assertEqual(key, 0)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2002-01-01", True)
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof({'name':'Ann'}, "2002-01-01", False)
        self.assertEqual(key, 1)

    def test_lookupasof_usingfrom_noversion(self):
        table = dtt.Table("customers", """
        | id:int (pk) | name:varchar | city:varchar | fromdate:timestamp |
        | ----------- | ------------ | ------------ | ------------------ |
        | 0           | Ann          | Arden        | NULL               |
        | 1           | Ann          | Aalborg      | 2001-01-01         |
        | 2           | Bob          | Boston       | 2001-01-01         |
        | 3           | Ann          | Aarhus       | 2002-01-01         |
        | 4           | Charlie      | Copenhagen   | 2001-01-01         |
        | 5           | Ann          | Aabenraa     | 2003-01-01         |
        | 6           | Bob          | Birkelse     | 2002-01-01         |
        """)
        table.reset()
        test_dimension = SlowlyChangingDimension(
            name=table.name,
            key=table.key(),
            attributes=table.attributes,
            lookupatts=['name'],
            fromatt='fromdate',
            cachesize=100,
            prefill=True)
        key = test_dimension.lookupasof({'name':'Bob'}, "1999-05-05", True)
        self.assertEqual(key, None)
        key = test_dimension.lookupasof({'name':'Ann'}, "1999-12-31", True)
        self.assertEqual(key, 0)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", True)
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2002-01-01", True)
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof({'name':'Ann'}, "2002-01-01", False)
        self.assertEqual(key, 1)
        
    def test_lookupasof_usingfromto(self):
        table = dtt.Table("customers", """
        | id:int (pk) | name:varchar | city:varchar | fromdate:timestamp | todate:timestamp | version:int |
        | ----------- | ------------ | ------------ | ------------------ | ---------------- | ----------- |
        | 0           | Aida         | Astrup       | NULL               | NULL             | 1           |
        | 1           | Ann          | Aalborg      | 2001-01-01         | 2001-12-31       | 1           |
        | 2           | Bob          | Boston       | 2001-01-01         | 2001-12-31       | 1           |
        | 3           | Ann          | Aarhus       | 2002-01-01         | 2002-12-31       | 2           |
        | 4           | Charlie      | Copenhagen   | 2001-01-01         | NULL             | 1           |
        | 5           | Ann          | Aabenraa     | 2003-01-01         | NULL             | 3           |
        | 6           | Bob          | Birkelse     | 2002-01-01         | 2002-12-31       | 2           |
        """)
        table.reset()
        test_dimension = SlowlyChangingDimension(
            name=table.name,
            key=table.key(),
            attributes=table.attributes,
            lookupatts=['name'],
            versionatt='version',
            fromatt='fromdate',
            toatt='todate',
            cachesize=100,
            prefill=True)
        key = test_dimension.lookupasof({'name':'Aida'}, "2001-05-05", (True, True))
        self.assertEqual(key, 0)
        key = test_dimension.lookupasof({'name':'Ann'}, "1999-09-09", (True, False))
        self.assertEqual(key, None)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", (True, False))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", (False, True))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-12-31", (False, True))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2002-12-31", (True, True))
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof({'name':'Charlie'}, "2002-12-31", (True, True))
        self.assertEqual(key, 4)
        key = test_dimension.lookupasof({'name':'Ann'}, "2222-12-31", (True, True))
        self.assertEqual(key, 5)
        key = test_dimension.lookupasof({'name':'Bob'}, "2222-12-31", (True, True))
        self.assertEqual(key, None)
        self.assertRaises(ValueError, test_dimension.lookupasof, row={'name':'Ann'}, when="2222-12-31", inclusive=(False, False))

    def test_lookupasof_usingfromto_noversion(self):
        table = dtt.Table("customers", """
        | id:int (pk) | name:varchar | city:varchar | fromdate:timestamp | todate:timestamp |
        | ----------- | ------------ | ------------ | ------------------ | ---------------- |
        | 0           | Aida         | Astrup       | NULL               | NULL             |
        | 1           | Ann          | Aalborg      | 2001-01-01         | 2001-12-31       |
        | 2           | Bob          | Boston       | 2001-01-01         | 2001-12-31       |
        | 3           | Ann          | Aarhus       | 2002-01-01         | 2002-12-31       |
        | 4           | Charlie      | Copenhagen   | 2001-01-01         | NULL             |
        | 5           | Ann          | Aabenraa     | 2003-01-01         | NULL             |
        | 6           | Bob          | Birkelse     | 2002-01-01         | 2002-12-31       |
        """)
        table.reset()
        test_dimension = SlowlyChangingDimension(
            name=table.name,
            key=table.key(),
            attributes=table.attributes,
            lookupatts=['name'],
            fromatt='fromdate',
            toatt='todate',
            cachesize=100,
            prefill=True)
        key = test_dimension.lookupasof({'name':'Aida'}, "2001-05-05", (True, True))
        self.assertEqual(key, 0)
        key = test_dimension.lookupasof({'name':'Ann'}, "1999-09-09", (True, False))
        self.assertEqual(key, None)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", (True, False))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-05-05", (False, True))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2001-12-31", (False, True))
        self.assertEqual(key, 1)
        key = test_dimension.lookupasof({'name':'Ann'}, "2002-12-31", (True, True))
        self.assertEqual(key, 3)
        key = test_dimension.lookupasof({'name':'Charlie'}, "2002-12-31", (True, True))
        self.assertEqual(key, 4)
        key = test_dimension.lookupasof({'name':'Ann'}, "2222-12-31", (True, True))
        self.assertEqual(key, 5)
        key = test_dimension.lookupasof({'name':'Bob'}, "2222-12-31", (True, True))
        self.assertEqual(key, None)
        self.assertRaises(ValueError, test_dimension.lookupasof, row={'name':'Ann'}, when="2222-12-31", inclusive=(False, False))
        
