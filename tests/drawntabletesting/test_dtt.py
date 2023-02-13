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
import pygrametl
import pygrametl.drawntabletesting as dtt
from tests import utilities

# Examples are from docs/examples/testing.rst
class TableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("book", """
        | bid:int (pk) | title:text            | genre:text |
        | ------------ | --------------------- | ---------- |
        | 1            | Unknown               | Unknown    |
        | 2            | Nineteen Eighty-Four  | Novel      |
        | 3            | Calvin and Hobbes One | Comic      |
        | 4            | Calvin and Hobbes Two | Comic      |
        | 5            | The Silver Spoon      | Cookbook   |
        """)

    def setUp(self):
        utilities.ensure_default_connection_wrapper()

    def test_init_correct(self):
        dtt.Table("book", """
        | bid:int (pk) | title:text (unique)   | genre:text (not null) |
        | ------------ | --------------------- | --------------------- |
        | 1            | Unknown               | Unknown               |
        | 2            | Nineteen Eighty-Four  | Novel                 |
        | 3            | Calvin and Hobbes One | Comic                 |
        | 4            | Calvin and Hobbes Two | Comic                 |
        | 5            | The Silver Spoon      | Cookbook              |
        """)

    def test_init_unknown_incorrect(self):
        # Unknown constraints
        with self.assertRaises(ValueError):
            dtt.Table("book", """
            | bid:int (pk) | title:text (unique)   | genre:text (notnull) |
            | ------------ | --------------------- | -------------------- |
            | 1            | Unknown               | Unknown              |
            | 2            | Nineteen Eighty-Four  | Novel                |
            | 3            | Calvin and Hobbes One | Comic                |
            | 4            | Calvin and Hobbes Two | Comic                |
            | 5            | The Silver Spoon      | Cookbook             |
            """)

        # Missing : between name and type
        with self.assertRaises(ValueError):
            dtt.Table("book", """
            | bid int (pk) | title text (unique)   | genre text (not null) |
            | ------------ | --------------------- | --------------------- |
            | 1            | Unknown               | Unknown               |
            | 2            | Nineteen Eighty-Four  | Novel                 |
            | 3            | Calvin and Hobbes One | Comic                 |
            | 4            | Calvin and Hobbes Two | Comic                 |
            | 5            | The Silver Spoon      | Cookbook              |
            """)

    def test_ensure_and_foreign_key(self):
        dtt.Table("genre", """
        | bid:int (pk) | genre:text |
        | ------------ | ---------- |
        | 1            | Unknown    |
        | 2            | Novel      |
        | 3            | Comic      |
        | 4            | Cookbook   |
        """).ensure()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(bid)) |
        | ------------ | ---------------------- | ------------------------ |
        | 1            | Unknown                | 1                        |
        | 2            | Nineteen Eighty-Four   | 2                        |
        | 3            | Calvin and Hobbes One  | 3                        |
        | 4            | Calvin and Hobbes Two  | 3                        |
        | 5            | The Silver Spoon       | 4                        |
        """).ensure()

    def test_key(self):
        self.assertEqual(self.initial.key(), "bid")

    def test_getsqltocreate(self):
        self.assertEqual(
            self.initial.getSQLToCreate(),
            "CREATE TABLE book(bid int, title text, genre text, PRIMARY KEY (bid))")

    def test_getsqltoinsert(self):
        self.assertEqual(self.initial.getSQLToInsert(), (
            "INSERT INTO book(bid, title, genre) VALUES"
            "(1, 'Unknown', 'Unknown'), "
            "(2, 'Nineteen Eighty-Four', 'Novel'), "
            "(3, 'Calvin and Hobbes One', 'Comic'), "
            "(4, 'Calvin and Hobbes Two', 'Comic'), "
            "(5, 'The Silver Spoon', 'Cookbook')"))

    def test_assert_equal(self):
        book = self.initial
        book.ensure()
        book.assertEqual()

    def test_assert_not_equal(self):
        book = self.initial
        book.ensure()
        with self.assertRaises(AssertionError):
            (book + "| 6 | Metro 2033 | Novel |").assertEqual()

    def test_assert_disjoint(self):
        self.initial.ensure()
        dtt.Table("book", """
        | bid:int (pk) | title:text            | genre:text |
        | ------------ | --------------------- | ---------- |
        | 1            | None                  | None       |
        """).assertDisjoint()

    def test_assert_not_disjoint(self):
        book = self.initial
        book.ensure()
        with self.assertRaises(AssertionError):
            book.assertDisjoint()

    def test_assert_subset(self):
        self.initial.ensure()
        dtt.Table("book", """
        | bid:int (pk) | title:text            | genre:text |
        | ------------ | --------------------- | ---------- |
        | 1            | Unknown               | Unknown    |
        """).assertSubset()

    def test_assert_not_subset(self):
        book = self.initial
        book.ensure()
        with self.assertRaises(AssertionError):
            (book + "| 6 | Metro 2033 | Novel |").assertSubset()

    def test_create_reset_ensure_clear_drop(self):
        connection_wrapper = pygrametl.getdefaulttargetconnection()
        with self.assertRaises(Exception):
            connection_wrapper.execute("SELECT * FROM " + self.initial.name)

        self.initial.create()
        connection_wrapper.execute("SELECT * FROM " + self.initial.name)
        self.assertEqual(len(list(connection_wrapper.fetchalltuples())), 0)

        self.initial.reset()
        connection_wrapper.execute("SELECT * FROM " + self.initial.name)
        self.assertEqual(len(list(connection_wrapper.fetchalltuples())), 5)

        self.initial.ensure()
        connection_wrapper.execute("SELECT * FROM " + self.initial.name)
        self.assertEqual(len(list(connection_wrapper.fetchalltuples())), 5)

        self.initial.clear()
        with self.assertRaises(Exception):
            connection_wrapper.execute("SELECT * FROM " + self.initial.name)

        self.initial.create()
        with self.assertRaises(Exception):
            self.initial.ensure()

        self.initial.drop()
        with self.assertRaises(Exception):
            connection_wrapper.execute("SELECT * FROM " + self.initial.name)

    def test_add_update_and_additions(self):
        book = self.initial
        book_added = book + "| 6 | Metro 2033 | Novel |" \
            + "| 7 | Metro 2034 | Novel |"
        book_updated = book_added.update(0, "| -1 | Unknown | Unknown |")
        book_expected = [
            {'bid': -1, 'title': 'Unknown', 'genre': 'Unknown'},
            {'bid': 6, 'title': 'Metro 2033', 'genre': 'Novel'},
            {'bid': 7, 'title': 'Metro 2034', 'genre': 'Novel'}
        ]
        self.assertEqual(book_expected, book_updated.additions(withKey=True))

    def test_variables_and_foreign_keys_correct(self):
        dtt.Table("genre", """
        | gid:int (pk) | genre:text |
        | ------------ | ---------- |
        | 1            | Novel      |
        | 2            | Comic      |
        """).ensure()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid))  |
        | ------------ | ---------------------- | ------------------------ |
        | 1            | Nineteen Eighty-Four   | 1                        |
        | 2            | Calvin and Hobbes One  | 2                        |
        | 3            | Calvin and Hobbes Two  | 2                        |
        """).ensure()

        dtt.Table("genre", """
        | gid:int (pk)  | genre:text |
        | ------------- | ---------- |
        | $1            | Novel      |
        | $2            | Comic      |
        """).assertEqual()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid)) |
        | ------------ | ---------------------- | ----------------------- |
        | 1            | Nineteen Eighty-Four   | $1                      |
        | 2            | Calvin and Hobbes One  | $2                      |
        | 3            | Calvin and Hobbes Two  | $2                      |
        """).assertEqual()

    def test_variables_and_foreign_keys_wrong(self):
        dtt.Table("genre", """
        | gid:int (pk) | genre:text |
        | ------------ | ---------- |
        | 1            | Novel      |
        | 2            | Comic      |
        """).ensure()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid))  |
        | ------------ | ---------------------- | ------------------------ |
        | 1            | Nineteen Eighty-Four   | 2                        |
        | 2            | Calvin and Hobbes One  | 1                        |
        | 3            | Calvin and Hobbes Two  | 1                        |
        """).ensure()

        dtt.Table("genre", """
        | gid:int (pk)  | genre:text |
        | ------------- | ---------- |
        | $1            | Novel      |
        | $2            | Comic      |
        """).assertEqual()

        book = dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid)) |
        | ------------ | ---------------------- | ----------------------- |
        | 1            | Nineteen Eighty-Four   | $1                      |
        | 2            | Calvin and Hobbes One  | $2                      |
        | 3            | Calvin and Hobbes Two  | $2                      |
        """)

        with self.assertRaises(AssertionError):
            book.assertEqual()

    def test_variables_underscore(self):
        dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date |
        | ------------ | --------- | ----------------------- | -------------- | ------------ |
        | NULL         | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | 2000-01-01   |
        | NULL         | CS        | Selma Lagerløfs Vej 300 | 2000-01-01     | NULL         |
        """).ensure()

        dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date  |
        | ------------ | --------- | ----------------------- | -------------- | ------------- |
        | $_           | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | $3            |
        | $_           | CS        | Selma Lagerløfs Vej 300 | $3             | NULL          |
        """).assertEqual()

    def test_variables_underscore_not_null(self):
        dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date |
        | ------------ | --------- | ----------------------- | -------------- | ------------ |
        | NULL         | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | 2000-01-01   |
        | NULL         | CS        | Selma Lagerløfs Vej 300 | 2000-01-01     | NULL         |
        """).ensure()

        address = dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date  |
        | ------------ | --------- | ----------------------- | -------------- | ------------- |
        | $_!          | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | $4            |
        | $_!          | CS        | Selma Lagerløfs Vej 300 | $4             | NULL          |
        """)

        with self.assertRaises(AssertionError):
            address.assertEqual()


# The tests are from docs/examples/testing.rst
def executeETLFlow(cw, row):
    if row['bid'] == 5:
        cw.execute("INSERT INTO book (bid, title, genre) VALUES(" +
                   (",".join(map(lambda x: "'" + x + "'" if type(x) is str
                                 else str(x), list(row.values())))) + ")")


class BookStateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("book", """
        | bid:int (pk) | title:text            | genre:text |
        | ------------ | --------------------- | ---------- |
        | 1            | Unknown               | Unknown    |
        | 2            | Nineteen Eighty-Four  | Novel      |
        | 3            | Calvin and Hobbes One | Comic      |
        | 4            | The Silver Spoon      | Cookbook   |
        """)

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()

    def test_insertNew(self):
        expected = self.initial + "| 5 | Calvin and Hobbes Two | Comic |"
        newrow = expected.additions(withKey=True)[0]
        executeETLFlow(pygrametl.getdefaulttargetconnection(), newrow)
        expected.assertEqual()

    def test_insertExisting(self):
        newrow = {'bid': 6, 'book': 'Calvin and Hobbes One', 'genre': 'Comic'}
        executeETLFlow(pygrametl.getdefaulttargetconnection(), newrow)
        self.initial.assertEqual()
