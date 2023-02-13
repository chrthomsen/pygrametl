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
import tempfile
from pygrametl.tables import FactTable
from pygrametl.tables import BatchFactTable
from pygrametl.tables import BulkFactTable
from pygrametl.tables import AccumulatingSnapshotFactTable


class FactTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("sales", """
        | BookID:int (pk)  | CityID:int (pk) | DayID:int (pk) | Count:int | Profit:int |
        | -----------------| --------------- | -------------- | --------- | ---------- |
        | 2                | 2               | 60             | 20        | 1000       |
        | 1                | 2               | 60             | 5         | 2000       |
        | 1                | 1               | 72             | 2         | 3000       |
        | 2                | 1               | 72             | 11        | 4000       |
        | 2                | 1               | 60             | 18        | 5000       |
        """)

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.initial.reset()
        self.fact_table = FactTable(name=self.initial.name,
                                    keyrefs=["BookID", "CityID", "DayID"],
                                    measures=["Count", "Profit"])

    def test_lookup(self):
        postcondition = self.initial

        res = self.fact_table.lookup({"BookID": 2, "CityID": 1, "DayID": 72})
        self.assertDictEqual(
            {"BookID": 2, "CityID": 1, "DayID": 72, "Count": 11, "Profit": 4000}, res)

        self.cw.commit()
        postcondition.assertEqual()

    def test_lookup_with_nonexisting_fact(self):
        postcondition = self.initial

        res = self.fact_table.lookup(
            {"BookID": 1000, "CityID": 999, "DayID": 888})
        self.assertIsNone(res)

        self.cw.commit()
        postcondition.assertEqual()

    def test_lookup_with_missing_key(self):
        postcondition = self.initial

        # Missing the key 'DayID'
        self.assertRaises(KeyError, self.fact_table.lookup,
                          {"BookID": 2, "CityID": 1})

        self.cw.commit()
        postcondition.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition = self.initial

        namemapping = {"DayID": "DateID"}

        res = self.fact_table.lookup({"BookID": 2, "CityID": 1, "DateID": 72},
                                     namemapping=namemapping)
        self.assertDictEqual(
            {"BookID": 2, "CityID": 1, "DayID": 72, "Count": 11, "Profit": 4000}, res)

        self.cw.commit()
        postcondition.assertEqual()

    def test_insert_new_fact_with_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        self.fact_table.insert(
            {"BookID": 1, "CityID": 1, "DayID": 60, "Count": 87, "Profit": 7000})

        self.cw.commit()
        postcondition.assertEqual()

    def test_insert_new_fact_with_namemapping_and_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        namemapping = {"DayID": "DateID"}

        self.fact_table.insert({"BookID": 1, "CityID": 1, "DateID": 60,
                                "Count": 87, "Profit": 7000},
                               namemapping=namemapping)

        self.cw.commit()
        postcondition.assertEqual()

    def test_insert_fact_with_missing_key(self):
        postcondition = self.initial

        self.assertRaises(KeyError, self.fact_table.insert, {
                          "BookID": 1, "CityID": 1, "Count": 87, "Profit": 7000})

        self.cw.commit()
        postcondition.assertEqual()

    def test_insert_new_fact_with_missing_measure(self):
        postcondition = self.initial

        # Test the cases where one measure is missing and when both of them are
        # missing
        for fact in [
            {"BookID": 1, "CityID": 1, "DayID": 60, "Count": 87},
            {"BookID": 1, "CityID": 1, "DayID": 60}
        ]:
            self.assertRaises(KeyError, self.fact_table.insert, fact)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_once_with_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        new_fact = {"BookID": 1, "CityID": 1,
                    "DayID": 60, "Count": 87, "Profit": 7000}

        fact_existed_with_same_keys = self.fact_table.ensure(new_fact)
        self.assertFalse(fact_existed_with_same_keys)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_twice_with_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        fact = {"BookID": 1, "CityID": 1,
                "DayID": 60, "Count": 87, "Profit": 7000}

        self.fact_table.ensure(fact)
        # Second call should return True
        fact_existed_with_same_keys = self.fact_table.ensure(fact)
        self.assertTrue(fact_existed_with_same_keys)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_with_namemapping_and_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        namemapping = {"DayID": "DateID"}
        new_fact = {"BookID": 1, "CityID": 1,
                    "DateID": 60, "Count": 87, "Profit": 7000}

        fact_existed_with_same_keys = self.fact_table.ensure(
            new_fact, namemapping=namemapping)
        self.assertFalse(fact_existed_with_same_keys)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_existing_fact_with_same_measures(self):
        postcondition = self.initial

        fact = {"BookID": 2, "CityID": 2,
                "DayID": 60, "Count": 20, "Profit": 1000}

        fact_existed_with_same_keys = self.fact_table.ensure(fact)
        self.assertTrue(fact_existed_with_same_keys)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_existing_fact_with_different_measures_and_compare_is_true(self):
        postcondition = self.initial

        # Test both the case where one measure value is different from the
        # existing fact and when both values differ
        for fact in [
            {"BookID": 2, "CityID": 2, "DayID": 60, "Count": 20, "Profit": 50000},
            {"BookID": 2, "CityID": 2, "DayID": 60, "Count": 30, "Profit": 50000}
        ]:
            self.assertRaises(
                ValueError, self.fact_table.ensure, fact, compare=True)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_existing_fact_with_different_measures_and_compare_is_false(self):
        postcondition = self.initial

        # Test both the case where one measure value is different from the
        # existing fact and when both values differ
        for fact in [
            {"BookID": 2, "CityID": 2, "DayID": 60, "Count": 20, "Profit": 50000},
            {"BookID": 2, "CityID": 2, "DayID": 60, "Count": 30, "Profit": 50000}
        ]:
            fact_existed_with_same_keys = self.fact_table.ensure(fact)
            self.assertTrue(fact_existed_with_same_keys)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_new_fact_with_missing_measure(self):
        postcondition = self.initial

        # Test the cases where one measure is missing and when both of them are
        # missing
        for fact in [
            {"BookID": 1, "CityID": 1, "DayID": 60, "Count": 87},
            {"BookID": 1, "CityID": 1, "DayID": 60}
        ]:
            self.assertRaises(KeyError, self.fact_table.ensure, fact)

        self.cw.commit()
        postcondition.assertEqual()

    def test_ensure_fact_with_missing_key(self):
        postcondition = self.initial

        self.assertRaises(KeyError, self.fact_table.ensure, {
                          "BookID": 1, "CityID": 1, "Count": 87, "Profit": 7000})

        self.cw.commit()
        postcondition.assertEqual()


class BatchFactTableTest(FactTableTest):
    def setUp(self):
        self.initial.reset()

        self.cw = pygrametl.getdefaulttargetconnection()

        self.batchsize = 100
        self.fact_table = BatchFactTable(name=self.initial.name,
                                         keyrefs=["BookID", "CityID", "DayID"],
                                         measures=["Count", "Profit"],
                                         batchsize=self.batchsize)

    def test_insert_less_than_batchsize_num_of_facts_without_commit(self):
        postcondition = self.initial

        # Generate and insert batchsize-1 number of new facts
        for i in range(0, self.batchsize - 1):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})

        # With no commit and without reaching batchsize number of insertions,
        # the db table should remain unchanged
        postcondition.assertEqual()

        # awaitingrows should be equal to batchsize - 1
        self.assertEqual(self.batchsize - 1, self.fact_table.awaitingrows)

        # The facts can still be looked up
        for i in range(0, self.batchsize - 1):
            expected_fact = {"BookID": 10, "CityID": 10,
                             "DayID": i, "Count": i, "Profit": i}
            actual_fact = self.fact_table.lookup(
                {"BookID": 10, "CityID": 10, "DayID": i})
            self.assertDictEqual(expected_fact, actual_fact)

    def test_insert_batchsize_num_of_facts_without_commit(self):
        postcondition = self.initial

        # Generate and insert batchsize number of new facts
        for i in range(0, self.batchsize):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # The facts should have been inserted to the fact table since batchsize
        # has been reached
        postcondition.assertEqual()

        # awaitingrows should be equal to 0
        self.assertEqual(0, self.fact_table.awaitingrows)

    def test_insert_more_than_batchsize_num_of_facts_without_commit(self):
        postcondition = self.initial

        # Generate and insert batchsize number of new facts
        for i in range(0, self.batchsize):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # Generate and insert 10 more facts - these should be cached in main
        # memory and not inserted to the DB table
        for i in range(self.batchsize, self.batchsize + 10):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})

        # Only the first batchsize number of facts should have been inserted to
        # the fact table
        postcondition.assertEqual()

        # awaitingrows should be equal to 10
        self.assertEqual(10, self.fact_table.awaitingrows)

        # The 10 extra facts can still be looked up
        for i in range(self.batchsize, self.batchsize + 10):
            expected_fact = {"BookID": 10, "CityID": 10,
                             "DayID": i, "Count": i, "Profit": i}
            actual_fact = self.fact_table.lookup(
                {"BookID": 10, "CityID": 10, "DayID": i})
            self.assertDictEqual(expected_fact, actual_fact)

    def test_insert_multiple_batches_without_commit(self):
        postcondition = self.initial
        multiplier = 3

        # Generate and insert batchsize number of new facts
        for i in range(0, self.batchsize * multiplier):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # All facts should have been inserted to the fact table
        postcondition.assertEqual()

        # awaitingrows should be equal to 0
        self.assertEqual(0, self.fact_table.awaitingrows)


class BulkFactTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("sales", """
        | BookID:int (pk)  | CityID:int (pk) | DayID:int (pk) | Count:int | Profit:int |
        | ---------------- | --------------- | -------------- | --------- | ---------- |
        | 2                | 2               | 60             | 20        | 1000       |
        | 1                | 2               | 60             | 5         | 2000       |
        | 1                | 1               | 72             | 2         | 3000       |
        | 2                | 1               | 72             | 11        | 4000       |
        | 2                | 1               | 60             | 18        | 5000       |
        """)
        cls.bulksize = 100

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize)

    def loader(self, name, attributes, fieldsep, rowsep, nullval, filehandle):
        sql = "INSERT INTO sales(BookID, CityID, DayID, Count, Profit) VALUES({}, {}, {}, {}, {})"
        encoding = utilities.get_os_encoding()

        # If the default rowsep is used
        if rowsep == '\n':
            for line in filehandle:
                values = line.decode(encoding).strip().split(fieldsep)
                insert = sql.format(*values)
                self.cw.execute(insert)

        # If a custom rowsep is used
        else:
            content = filehandle.read().decode(encoding)
            rows = content.split(rowsep)
            for row in rows:
                # Skip empty lines
                if len(row) == 0:
                    continue
                values = row.strip().split(fieldsep)
                insert = sql.format(*values)
                self.cw.execute(insert)

    def test_insert_less_than_bulksize_number_of_facts(self):
        postcondition = self.initial

        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})

        # The inserted facts should not have been inserted into the db table yet
        postcondition.assertEqual()

        # awaitingrows should be equal to bulksize - 1
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_insert_bulksize_number_of_facts(self):
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # awaitingrows should be equal to 0
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_insert_more_than_bulksize_num_of_facts(self):
        postcondition = self.initial

        # Generate and insert bulksize number of new facts
        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # Generate and insert 10 more facts - these should not be inserted to
        # the DB table
        for i in range(self.bulksize, self.bulksize + 10):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})

        # Only the first batchsize number of facts should have been inserted to
        # the fact table
        postcondition.assertEqual()

        # awaitingrows should be equal to 10
        self.assertEqual(10, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_insert_less_than_bulksize_number_of_facts_with_custom_tempdest(self):
        filehandle = tempfile.NamedTemporaryFile()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle)
        postcondition = self.initial
        inserted_facts = []

        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # The inserted facts should not have been inserted into the db table yet
        postcondition.assertEqual()

        # Check that the passed tempfile contains the correct facts
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        facts_in_file = [line.decode(encoding).strip().split(
            '\t') for line in filehandle]
        self.assertEqual(inserted_facts, facts_in_file)

        # awaitingrows should be equal to bulksize - 1
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_insert_bulksize_number_of_facts_with_custom_tempdest(self):
        filehandle = tempfile.NamedTemporaryFile()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()
        self.assertEqual(0, len(content))

        # awaitingrows should be equal to 0
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_insert_more_than_bulksize_num_of_facts_with_custom_tempdest(self):
        filehandle = tempfile.NamedTemporaryFile()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle)
        postcondition = self.initial

        # Generate and insert bulksize number of new facts
        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # Generate and insert 10 more facts - these should not be inserted to
        # the DB table, but they should be added to the tempfile
        inserted_facts = []
        for i in range(self.bulksize, self.bulksize + 10):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # Only the first batchsize number of facts should have been inserted to
        # the fact table
        postcondition.assertEqual()

        # Check that the passed tempfile contains only the last 10 facts
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        facts_in_file = [line.decode(encoding).strip().split(
            '\t') for line in filehandle]
        self.assertEqual(inserted_facts, facts_in_file)

        # awaitingrows should be equal to 10
        self.assertEqual(10, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_fields_are_separated_by_custom_fieldsep_in_file(self):
        filehandle = tempfile.NamedTemporaryFile()
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        fieldsep=fieldsep)
        postcondition = self.initial
        inserted_facts = []

        # Write bulksize - 1 number of facts to the file
        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # The inserted facts should not have been inserted into the db table yet
        postcondition.assertEqual()

        # Check that the passed tempfile contains the correct facts with the
        # fields separated using fieldsep
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        facts_in_file = [line.decode(encoding).strip().split(
            fieldsep) for line in filehandle]
        self.assertEqual(inserted_facts, facts_in_file)

        # awaitingrows should be equal to bulksize - 1
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_facts_are_loaded_correctly_using_custom_fieldsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        fieldsep=fieldsep)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert({"BookID": 10, "CityID": 10, "DayID": i,
                                    "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()
        self.assertEqual(0, len(content))

        # awaitingrows should be equal to 0
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_fields_are_separated_by_custom_rowsep_in_file(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        rowsep=rowsep)
        postcondition = self.initial
        inserted_facts = []

        # Write bulksize - 1 number of facts to the file
        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # The inserted facts should not have been inserted into the db table yet
        postcondition.assertEqual()

        # Check that the passed tempfile contains the correct facts with the
        # rows separated using rowsep
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        file_content = filehandle.read().decode(encoding)
        facts_in_file = file_content.split(rowsep)
        facts_in_file_with_fields_separated = [fact.strip().split(
            '\t') for fact in facts_in_file if len(fact) != 0]
        self.assertEqual(inserted_facts, facts_in_file_with_fields_separated)

        # awaitingrows should be equal to bulksize - 1
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_facts_are_loaded_correctly_using_custom_rowsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        rowsep=rowsep)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert({"BookID": 10, "CityID": 10,
                                    "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()
        self.assertEqual(0, len(content))

        # awaitingrows should be equal to 0
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_fields_and_rows_are_separated_by_custom_rowsep_and_fieldsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        rowsep=rowsep,
                                        fieldsep=fieldsep)
        postcondition = self.initial
        inserted_facts = []

        # Write bulksize - 1 number of facts to the file
        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # The inserted facts should not have been inserted into the db table yet
        postcondition.assertEqual()

        # Check that the passed tempfile contains the correct facts with the
        # rows and fields separated using rowsep and fieldsep
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        file_content = filehandle.read().decode(encoding)
        facts_in_file = file_content.split(rowsep)
        facts_in_file_with_fields_separated = [fact.strip().split(
            fieldsep) for fact in facts_in_file if len(fact) != 0]
        self.assertEqual(inserted_facts, facts_in_file_with_fields_separated)

        # awaitingrows should be equal to bulksize - 1
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.cw.commit()

    def test_facts_are_loaded_correctly_using_custom_rowsep_and_fieldsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["BookID", "CityID", "DayID"],
                                        measures=["Count", "Profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        rowsep=rowsep,
                                        fieldsep=fieldsep)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"BookID": 10, "CityID": 10, "DayID": i, "Count": i, "Profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()
        self.assertEqual(0, len(content))

        # awaitingrows should be equal to 0
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.cw.commit()


class AccumulatingSnapshotFactTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("facts", """
        | id1:int (pk) | id2:int (pk) | ref1:int | ref2:int | ref3:int | meas:real | lag21:int |
        | ------------ | ------------ | -------- | -------- | -------- | --------- | --------- |
        | 1            | 1            | 1        | 1        | 1        | 1.0       | 0         |
        | 2            | 2            | 2        | NULL     | NULL     | 2.0       | NULL      |
        | 3            | 3            | NULL     | NULL     | NULL     | NULL      | NULL      |
        """)

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.cw = pygrametl.getdefaulttargetconnection()
        self.ft = AccumulatingSnapshotFactTable(name=self.initial.name,
                                                keyrefs=self.initial.key(),
                                                otherrefs=[
                                                    'ref1', 'ref2', 'ref3'],
                                                measures=['meas', 'lag21'],
                                                factexpander=self.__complag)

    def __complag(self, row, namemapping, updated):
        if 'ref2' in updated:
            ref1inrow = namemapping.get('ref1') or 'ref1'
            ref2inrow = namemapping.get('ref2') or 'ref2'
            lag21inrow = namemapping.get('lag21') or 'lag21'
            row[lag21inrow] = row[ref2inrow] - row[ref1inrow]

    def test_insert_one_without_commit(self):
        self.ft.insert({'id1': 4, 'id2': 4,
                        'ref1': 3, 'ref2': 3, 'ref3': 3, 'meas': 3, 'lag21': 3})
        expected = self.initial + "|4|4|3|3|3|3|3|"
        expected.assertEqual()

    def test_insert_one_with_commit(self):
        self.ft.insert({'id1': 4, 'id2': 4,
                        'ref1': 3, 'ref2': 3, 'ref3': 3, 'meas': 3, 'lag21': 3})
        self.cw.commit()
        expected = self.initial + "|4|4|3|3|3|3|3|"
        expected.assertEqual()

    def test_ensure_one_change(self):
        self.ft.ensure({'id1': 2, 'id2': 2, 'ref2': 3})
        expected = self.initial.update(1, "|2|2|2|3|NULL|2.0|1|")
        expected.assertEqual()

    def test_ensure_two_changes(self):
        self.ft.ensure({'id1': 2, 'id2': 2, 'ref2': 3, 'ref3': 3})
        self.ft.ensure({'id1': 2, 'id2': 2, 'ref2': 3, 'ref3': 4})
        expected = self.initial.update(1, "|2|2|2|3|4|2.0|1|")
        expected.assertEqual()

    def test_ensure_two_changes_with_namemapping(self):
        self.ft.ensure({'id1': 2, 'xid2': 2, 'xref2': 3, 'ref3': 3},
                       {'id2': 'xid2', 'ref2': 'xref2'})
        expected = self.initial.update(1, "|2|2|2|3|3|2.0|1|")
        expected.assertEqual()

    def test_ensure_new(self):
        self.ft.ensure({'id1': 4, 'id2': 4, 'ref1': 4})
        expected = self.initial + "|4|4|4|NULL|NULL|NULL|NULL|"
        expected.assertEqual()
