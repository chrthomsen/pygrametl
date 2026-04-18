# Copyright (c) 2023, Aalborg University (pygrametl@cs.aau.dk)
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
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

import sqlite3
import unittest

import pygrametl
import pygrametl.drawntabletesting as dtt
from pygrametl.datasources import (
    SQLSource,
    MappingSource,
    SQLTransformingSource,
    UnpivotingSource,
)
from tests import utilities


class SQLSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = utilities.get_connection()
        cls.connection_wrapper = pygrametl.ConnectionWrapper(cls.connection)
        cls.initial = dtt.Table(
            "book",
            """
        | id:int (pk) | title:text            | genre:text |
        | ----------- | --------------------- | ---------- |
        | 1           | Unknown               | Unknown    |
        | 2           | Nineteen Eighty-Four  | Novel      |
        | 3           | Calvin and Hobbes One | Comic      |
        | 4           | Calvin and Hobbes Two | Comic      |
        | 5           | The Silver Spoon      | Cookbook   |
        """,
        )
        cls.row_len = len(cls.initial)
        cls.column_len = len(cls.initial[0].keys())
        cls.names = list(cls.initial[0].keys())
        cls.initial.ensure()
        cls.query = "SELECT * FROM book"

    def assert_all_rows_and_columns_returned(self, sql_source, names):
        row_counter = 0
        for row in sql_source:
            row_counter += 1
            self.assertEqual(self.column_len, len(row.keys()))
            self.assertEqual(names, list(row.keys()))
        self.assertEqual(self.row_len, row_counter)

    def test_with_default_arguments(self):
        sql_source = SQLSource(self.connection, self.query)
        self.assert_all_rows_and_columns_returned(sql_source, self.names)

    def test_with_correct_number_of_names(self):
        names = ["bid", "title", "genre"]
        sql_source = SQLSource(self.connection, self.query, names=names)
        self.assert_all_rows_and_columns_returned(sql_source, names)

    def test_with_too_few_names(self):
        with self.assertRaises(ValueError):
            sql_source = SQLSource(self.connection, self.query, names=["bid"])
            self.assert_all_rows_and_columns_returned(sql_source, self.names)

    def test_with_too_many_names(self):
        with self.assertRaises(ValueError):
            sql_source = SQLSource(
                self.connection,
                self.query,
                names=["bid", "title", "genre", "publisher"],
            )
            self.assert_all_rows_and_columns_returned(sql_source, self.names)

    def test_with_succeeding_initsql(self):
        sql_source = SQLSource(
            self.connection,
            self.query,
            initsql="CREATE TABLE author (name VARCHAR)",
        )
        self.assert_all_rows_and_columns_returned(sql_source, self.names)

    def test_with_failing_initsql(self):
        with self.assertRaises(sqlite3.OperationalError):
            SQLSource(
                self.connection,
                self.query,
                initsql="CREATE TABLE book (bid INTEGER)",
            )


class MappingSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Ensure other tests does not affect these tests
        utilities.remove_default_connection_wrapper()

    def setUp(self):
        self.input_list = [
            {"id": 1, "title": "Unknown", "genre": "Unknown"},
            {"id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel"},
            {"id": 3, "title": "Calvin and Hobbes One", "genre": "Comic"},
            {"id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic"},
            {"id": 5, "title": "The Silver Spoon", "genre": "Cookbook"},
        ]

    def test_mapping_single_callable(self):
        source = MappingSource(iter(self.input_list), {"id": lambda x: x + 1})
        expected = [
            {"id": 2, "title": "Unknown", "genre": "Unknown"},
            {"id": 3, "title": "Nineteen Eighty-Four", "genre": "Novel"},
            {"id": 4, "title": "Calvin and Hobbes One", "genre": "Comic"},
            {"id": 5, "title": "Calvin and Hobbes Two", "genre": "Comic"},
            {"id": 6, "title": "The Silver Spoon", "genre": "Cookbook"},
        ]

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(expected, list(source))

    def test_mapping_two_callables(self):
        source = MappingSource(
            iter(self.input_list),
            {"id": lambda x: x + 1, "genre": lambda x: x[0]},
        )
        expected = [
            {"id": 2, "title": "Unknown", "genre": "U"},
            {"id": 3, "title": "Nineteen Eighty-Four", "genre": "N"},
            {"id": 4, "title": "Calvin and Hobbes One", "genre": "C"},
            {"id": 5, "title": "Calvin and Hobbes Two", "genre": "C"},
            {"id": 6, "title": "The Silver Spoon", "genre": "C"},
        ]

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(expected, list(source))


class SQLTransformationSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_list = [
            {"id": 1, "title": "Unknown", "genre": "Unknown"},
            {"id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel"},
            {"id": 3, "title": "Calvin and Hobbes One", "genre": "Comic"},
            {"id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic"},
            {"id": 5, "title": "The Silver Spoon", "genre": "Cookbook"},
        ]

        cls.expected_group_by_genre = [
            {"genre": "Comic", "COUNT(title)": 2},
            {"genre": "Cookbook", "COUNT(title)": 1},
            {"genre": "Novel", "COUNT(title)": 1},
            {"genre": "Unknown", "COUNT(title)": 1},
        ]

        # Ensure other tests does not affect these tests
        utilities.remove_default_connection_wrapper()

    def test_transform(self):
        source = SQLTransformingSource(
            iter(self.input_list),
            "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
        )

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(self.expected_group_by_genre, list(source))

    def test_transform_with_batch_size_of_one(self):
        source = SQLTransformingSource(
            iter(self.input_list),
            "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            batchsize=1,
        )

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(self.expected_group_by_genre, list(source))

    def test_transform_with_batch_size_of_one_and_perbatch(self):
        expected_group_by_genre_per_batch = [
            {"genre": "Unknown", "COUNT(title)": 1},
            {"genre": "Novel", "COUNT(title)": 1},
            {"genre": "Comic", "COUNT(title)": 1},
            {"genre": "Comic", "COUNT(title)": 1},
            {"genre": "Cookbook", "COUNT(title)": 1},
        ]

        source = SQLTransformingSource(
            iter(self.input_list),
            "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            batchsize=1,
            perbatch=True,
        )

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(expected_group_by_genre_per_batch, list(source))

    def test_transform_with_renamed_columns(self):
        expected_group_by_genre_renamed = [
            {"genre": "Comic", "count": 2},
            {"genre": "Cookbook", "count": 1},
            {"genre": "Novel", "count": 1},
            {"genre": "Unknown", "count": 1},
        ]

        source = SQLTransformingSource(
            iter(self.input_list),
            "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            columnnames=["genre", "count"],
        )

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(expected_group_by_genre_renamed, list(source))

    def test_transform_with_pep_connection(self):
        source = SQLTransformingSource(
            iter(self.input_list),
            "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            targetconnection=sqlite3.connect(":memory:"),
        )

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(self.expected_group_by_genre, list(source))

    def test_transform_with_connection_wrapper(self):
        source = SQLTransformingSource(
            iter(self.input_list),
            "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            targetconnection=utilities.ensure_default_connection_wrapper(),
        )

        # Ensure this test does not affect the other tests even if it fails
        utilities.remove_default_connection_wrapper()
        self.assertEqual(self.expected_group_by_genre, list(source))


class UnpivotingSourceTest(unittest.TestCase):
    def test_unpivot_with_explicit_attributes(self):
        source = UnpivotingSource(
            [
                {"product": "A", "jan": 1, "feb": 2},
                {"product": "B", "jan": 3, "feb": 4},
            ],
            keyatts=("product",),
            unpivotatts=("jan", "feb"),
            nameatt="month",
            valueatt="sales",
        )

        expected = [
            {"product": "A", "month": "jan", "sales": 1},
            {"product": "A", "month": "feb", "sales": 2},
            {"product": "B", "month": "jan", "sales": 3},
            {"product": "B", "month": "feb", "sales": 4},
        ]
        self.assertEqual(expected, list(source))

    def test_unpivot_with_inferred_attributes(self):
        source = UnpivotingSource(
            [
                {"id": 1, "a": 10, "b": 20},
            ],
            keyatts="id",
        )

        expected = [
            {"id": 1, "name": "a", "value": 10},
            {"id": 1, "name": "b", "value": 20},
        ]
        self.assertEqual(expected, list(source))

    def test_unpivot_ignoring_none(self):
        source = UnpivotingSource(
            [
                {"id": 1, "a": None, "b": 20},
            ],
            keyatts="id",
            ignorenone=True,
        )

        expected = [
            {"id": 1, "name": "b", "value": 20},
        ]
        self.assertEqual(expected, list(source))

    def test_invalid_arguments(self):
        with self.assertRaises(ValueError):
            UnpivotingSource([], keyatts=("id",), nameatt="name", valueatt="name")

        with self.assertRaises(ValueError):
            UnpivotingSource([], keyatts=("name",), nameatt="name")

        with self.assertRaises(ValueError):
            UnpivotingSource([], keyatts=("id",), unpivotatts=("id",))