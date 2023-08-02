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

import sqlite3
import unittest

import pygrametl
from pygrametl.datasources import SQLTransformingSource
from tests import utilities


class SQLTransformationSourceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.input_list = [
            { "id": 1, "title": "Unknown", "genre": "Unknown" },
            { "id": 2, "title": "Nineteen Eighty-Four", "genre": "Novel" },
            { "id": 3, "title": "Calvin and Hobbes One", "genre": "Comic" },
            { "id": 4, "title": "Calvin and Hobbes Two", "genre": "Comic" },
            { "id": 5, "title": "The Silver Spoon", "genre": "Cookbook" }
        ]

        cls.expected_group_by_genre = [
            {'genre': 'Comic', 'COUNT(title)': 2},
            {'genre': 'Cookbook', 'COUNT(title)': 1},
            {'genre': 'Novel', 'COUNT(title)': 1},
            {'genre': 'Unknown', 'COUNT(title)': 1}
        ]

        # Ensure other tests does not affect these tests
        utilities.remove_default_connection_wrapper()

    def test_transform(self):
        source = SQLTransformingSource(
            iter(self.input_list), "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre")

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(self.expected_group_by_genre, list(source))

    def test_transform_with_batch_size_of_one(self):
        source = SQLTransformingSource(
            iter(self.input_list), "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            batchsize=1)

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(self.expected_group_by_genre, list(source))

    def test_transform_with_batch_size_of_one_and_perbatch(self):
        expected_group_by_genre_per_batch = [
            {'genre': 'Unknown', 'COUNT(title)': 1},
            {'genre': 'Novel', 'COUNT(title)': 1},
            {'genre': 'Comic', 'COUNT(title)': 1},
            {'genre': 'Comic', 'COUNT(title)': 1},
            {'genre': 'Cookbook', 'COUNT(title)': 1}
        ]

        source = SQLTransformingSource(
            iter(self.input_list), "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            batchsize=1, perbatch=True)

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(expected_group_by_genre_per_batch, list(source))

    def test_transform_with_batch_size_of_one_perbatch_and_truncate(self):
        source = SQLTransformingSource(
            iter(self.input_list), "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            batchsize=1, perbatch=True, usetruncate=True)

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        with self.assertRaises(sqlite3.OperationalError) as cm:
            list(source)

        e = cm.exception
        self.assertEqual(e.sqlite_errorname, "SQLITE_ERROR")
        self.assertEqual(e.sqlite_errorcode, 1)
        self.assertEqual(str(e), "near \"TRUNCATE\": syntax error")

    def test_transform_with_renamed_columns(self):
        expected_group_by_genre_renamed = [
            {'genre': 'Comic', 'count': 2},
            {'genre': 'Cookbook', 'count': 1},
            {'genre': 'Novel', 'count': 1},
            {'genre': 'Unknown', 'count': 1}
        ]

        source = SQLTransformingSource(
            iter(self.input_list), "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            columnnames=["genre", "count"])

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(expected_group_by_genre_renamed, list(source))

    def test_transform_with_pep_connection(self):
        source = SQLTransformingSource(
            iter(self.input_list), "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            targetconnection=sqlite3.connect(":memory:"))

        self.assertIsNone(pygrametl.getdefaulttargetconnection())
        self.assertEqual(self.expected_group_by_genre, list(source))

    def test_transform_with_connection_wrapper(self):
        source = SQLTransformingSource(
            iter(self.input_list), "book",
            "SELECT genre, COUNT(title) FROM book GROUP BY genre",
            targetconnection=utilities.ensure_default_connection_wrapper())

        # Ensure this test does not affect the other tests even if it fails
        utilities.remove_default_connection_wrapper()
        self.assertEqual(self.expected_group_by_genre, list(source))
