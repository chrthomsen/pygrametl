import unittest

from tests import utilities
import pygrametl
import pygrametl.drawntabletesting as dtt


# The Table Class, Assertions, and Variables
class TheTableClassTest(unittest.TestCase):
    def setUp(self):
        utilities.ensure_default_connection_wrapper()

    def test_init(self):
        return dtt.Table("book", """
        | bid:int (pk) | title:text            | genre:text |
        | ------------ | --------------------- | ---------- |
        | 1            | Unknown               | Unknown    |
        | 2            | Nineteen Eighty-Four  | Novel      |
        | 3            | Calvin and Hobbes One | Comic      |
        | 4            | Calvin and Hobbes Two | Comic      |
        | 5            | The Silver Spoon      | Cookbook   |""")

    def test_ensure_and_foreign_key(self):
        dtt.Table("genre", """
        | bid:int (pk) | genre:text |
        | ------------ | ---------- |
        | 1            | Unknown    |
        | 2            | Novel      |
        | 3            | Comic      |
        | 4            | Cookbook   |""").ensure()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(bid)) |
        | ------------ | ---------------------- | ------------------------ |
        | 1            | Unknown                | 1                        |
        | 2            | Nineteen Eighty-Four   | 2                        |
        | 3            | Calvin and Hobbes One  | 3                        |
        | 4            | Calvin and Hobbes Two  | 3                        |
        | 5            | The Silver Spoon       | 4                        |"""
                  ).ensure()

    def test_additions(self):
        book = self.test_init()
        book_added = book + "| 6 | Metro 2033 | Novel |" \
            + "| 7 | Metro 2034 | Novel |"
        book_updated = book_added.update(0, "| -1 | Unknown | Unknown |")
        book_expected = [
            {'bid': -1, 'title': 'Unknown', 'genre': 'Unknown'},
            {'bid': 6, 'title': 'Metro 2033', 'genre': 'Novel'},
            {'bid': 7, 'title': 'Metro 2034', 'genre': 'Novel'}
        ]
        self.assertEqual(book_expected, book_updated.additions(withKey=True))

    def test_assert_equal(self):
        book = self.test_init()
        book.ensure()
        book.assertEqual()

    def test_assert_not_equal(self):
        book = self.test_init()
        book.ensure()
        with self.assertRaises(AssertionError):
            (book + "| 6 | Metro 2033 | Novel |").assertEqual()

    def test_assert_subset(self):
        self.test_init().ensure()
        dtt.Table("book", """
        | bid:int (pk) | title:text            | genre:text |
        | ------------ | --------------------- | ---------- |
        | 1            | Unknown               | Unknown    |"""
                  ).assertSubset()

    def test_assert_not_subset(self):
        book = self.test_init()
        book.ensure()
        with self.assertRaises(AssertionError):
            (book + "| 6 | Metro 2033 | Novel |").assertSubset()

    def test_assert_disjoint(self):
        self.test_init().ensure()
        dtt.Table("book", """
        | bid:int (pk) | title:text            | genre:text |
        | ------------ | --------------------- | ---------- |
        | 1            | None                  | None       |"""
                  ).assertDisjoint()

    def test_assert_not_disjoint(self):
        book = self.test_init()
        book.ensure()
        with self.assertRaises(AssertionError):
            book.assertDisjoint()

    def test_variables_foreign_key_correct(self):
        dtt.Table("genre", """
        | gid:int (pk) | genre:text |
        | ------------ | ---------- |
        | 1            | Novel      |
        | 2            | Comic      |""").ensure()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid))  |
        | ------------ | ---------------------- | ------------------------ |
        | 1            | Nineteen Eighty-Four   | 1                        |
        | 2            | Calvin and Hobbes One  | 2                        |
        | 3            | Calvin and Hobbes Two  | 2                        |"""
                  ).ensure()

        dtt.Table("genre", """
        | gid:int (pk)  | genre:text |
        | ------------- | ---------- |
        | $1            | Novel      |
        | $2            | Comic      |""").assertEqual()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid)) |
        | ------------ | ---------------------- | ----------------------- |
        | 1            | Nineteen Eighty-Four   | $1                      |
        | 2            | Calvin and Hobbes One  | $2                      |
        | 3            | Calvin and Hobbes Two  | $2                      |"""
                  ).assertEqual()

    def test_variables_foreign_key_wrong(self):
        dtt.Table("genre", """
        | gid:int (pk) | genre:text |
        | ------------ | ---------- |
        | 1            | Novel      |
        | 2            | Comic      |""").ensure()

        dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid))  |
        | ------------ | ---------------------- | ------------------------ |
        | 1            | Nineteen Eighty-Four   | 2                        |
        | 2            | Calvin and Hobbes One  | 1                        |
        | 3            | Calvin and Hobbes Two  | 1                        |"""
                  ).ensure()

        dtt.Table("genre", """
        | gid:int (pk)  | genre:text |
        | ------------- | ---------- |
        | $1            | Novel      |
        | $2            | Comic      |""").assertEqual()

        book = dtt.Table("book", """
        | bid:int (pk) | title:text             | gid:int (fk genre(gid)) |
        | ------------ | ---------------------- | ----------------------- |
        | 1            | Nineteen Eighty-Four   | $1                      |
        | 2            | Calvin and Hobbes One  | $2                      |
        | 3            | Calvin and Hobbes Two  | $2                      |""")
        with self.assertRaises(AssertionError):
            book.assertEqual()

    def test_variables_underscore(self):
        dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date |
        | ------------ | --------- | ----------------------- | -------------- | ------------ |
        | NULL         | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | 2000-01-01   |
        | NULL         | CS        | Selma Lagerløfs Vej 300 | 2000-01-01     | NULL         |""").ensure()

        dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date  |
        | ------------ | --------- | ----------------------- | -------------- | ------------- |
        | $_           | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | $3            |
        | $_           | CS        | Selma Lagerløfs Vej 300 | $3             | NULL          |""").assertEqual()

    def test_variables_underscore_not_null(self):
        dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date |
        | ------------ | --------- | ----------------------- | -------------- | ------------ |
        | NULL         | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | 2000-01-01   |
        | NULL         | CS        | Selma Lagerløfs Vej 300 | 2000-01-01     | NULL         |""").ensure()

        address = dtt.Table("address", """
        | aid:int (pk) | dept:text | location:text           | validfrom:date | validto:date  |
        | ------------ | --------- | ----------------------- | -------------- | ------------- |
        | $_!          | CS        | Fredrik Bajers Vej 7    | 1990-01-01     | $4            |
        | $_!          | CS        | Selma Lagerløfs Vej 300 | $4             | NULL          |""")
        with self.assertRaises(AssertionError):
            address.assertEqual()


# Drawn Table Testing as a Python Library
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
        | 4            | The Silver Spoon      | Cookbook   |""")

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
