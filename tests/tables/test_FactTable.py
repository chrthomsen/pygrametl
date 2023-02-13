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
        | bib:int (pk)  | cid:int (pk) | did:int (pk) | count:int | profit:int |
        | ------------- | ------------ | ------------ | --------- | ---------- |
        | 2             | 2            | 60           | 20        | 1000       |
        | 1             | 2            | 60           | 5         | 2000       |
        | 1             | 1            | 72           | 2         | 3000       |
        | 2             | 1            | 72           | 11        | 4000       |
        | 2             | 1            | 60           | 18        | 5000       |
        """)

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.initial.reset()
        self.fact_table = FactTable(name=self.initial.name,
                                    keyrefs=["bib", "cid", "did"],
                                    measures=["count", "profit"])

    def test_insert_new_fact_with_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        self.fact_table.insert(
            {"bib": 1, "cid": 1, "did": 60, "count": 87, "profit": 7000})

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_insert_new_fact_with_namemapping_and_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        namemapping = {"did": "DateID"}

        self.fact_table.insert({"bib": 1, "cid": 1, "DateID": 60,
                                "count": 87, "profit": 7000},
                               namemapping=namemapping)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_insert_fact_with_missing_key(self):
        postcondition = self.initial

        self.assertRaises(KeyError, self.fact_table.insert, {
            "bib": 1, "cid": 1, "count": 87, "profit": 7000})

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_insert_new_fact_with_missing_measure(self):
        postcondition = self.initial

        # Test when one measure missing and when both measures are missing
        for fact in [
                {"bib": 1, "cid": 1, "did": 60, "count": 87},
                {"bib": 1, "cid": 1, "did": 60}
        ]:
            self.assertRaises(KeyError, self.fact_table.insert, fact)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_lookup(self):
        postcondition = self.initial

        result = self.fact_table.lookup({"bib": 2, "cid": 1, "did": 72})
        self.assertDictEqual(
            {"bib": 2, "cid": 1, "did": 72, "count": 11, "profit": 4000},
            result)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_lookup_with_nonexisting_fact(self):
        postcondition = self.initial

        result = self.fact_table.lookup(
            {"bib": 1000, "cid": 999, "did": 888})
        self.assertIsNone(result)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_lookup_with_missing_key(self):
        postcondition = self.initial

        self.assertRaises(KeyError, self.fact_table.lookup,
                          {"bib": 2, "cid": 1})

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_lookup_with_namemapping(self):
        postcondition = self.initial

        namemapping = {"did": "DateID"}

        result = self.fact_table.lookup({"bib": 2, "cid": 1, "DateID": 72},
                                        namemapping=namemapping)
        self.assertDictEqual(
            {"bib": 2, "cid": 1, "did": 72, "count": 11, "profit": 4000},
            result)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_once_with_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        new_fact = {"bib": 1, "cid": 1,
                    "did": 60, "count": 87, "profit": 7000}

        self.assertFalse(self.fact_table.ensure(new_fact))

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_twice_with_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        fact = {"bib": 1, "cid": 1,
                "did": 60, "count": 87, "profit": 7000}

        self.fact_table.ensure(fact)
        self.assertTrue(self.fact_table.ensure(fact))

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_with_namemapping_and_commit(self):
        postcondition = self.initial + "| 1 | 1 | 60 | 87 | 7000 |"

        namemapping = {"did": "DateID"}
        new_fact = {"bib": 1, "cid": 1,
                    "DateID": 60, "count": 87, "profit": 7000}

        self.assertFalse(self.fact_table.ensure(
            new_fact, namemapping=namemapping))

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_existing_fact_with_same_measures(self):
        postcondition = self.initial

        fact = {"bib": 2, "cid": 2,
                "did": 60, "count": 20, "profit": 1000}

        fact_existed_with_same_keys = self.fact_table.ensure(fact)
        self.assertTrue(fact_existed_with_same_keys)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_existing_fact_with_other_measures_and_compare_true(self):
        postcondition = self.initial

        # Test both the case where one measure value is different from the
        # existing fact and where both the values of both measures differs
        for fact in [
                {"bib": 2, "cid": 2, "did": 60, "count": 20, "profit": 50000},
                {"bib": 2, "cid": 2, "did": 60, "count": 30, "profit": 50000}
        ]:
            self.assertRaises(
                ValueError, self.fact_table.ensure, fact, compare=True)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_existing_fact_with_other_measures_and_compare_false(self):
        postcondition = self.initial

        # Test both the case where one measure value is different from the
        # existing fact and where both the values of both measures differs
        for fact in [
                {"bib": 2, "cid": 2, "did": 60, "count": 20, "profit": 50000},
                {"bib": 2, "cid": 2, "did": 60, "count": 30, "profit": 50000}
        ]:
            self.assertTrue(self.fact_table.ensure(fact))

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_new_fact_with_missing_measures(self):
        postcondition = self.initial

        # Test when one measure missing and when both measures are missing
        for fact in [
                {"bib": 1, "cid": 1, "did": 60, "count": 87},
                {"bib": 1, "cid": 1, "did": 60}
        ]:
            self.assertRaises(KeyError, self.fact_table.ensure, fact)

        self.connection_wrapper.commit()
        postcondition.assertEqual()

    def test_ensure_fact_with_missing_key(self):
        postcondition = self.initial

        self.assertRaises(KeyError, self.fact_table.ensure, {
            "bib": 1, "cid": 1, "count": 87, "profit": 7000})

        self.connection_wrapper.commit()
        postcondition.assertEqual()


class BatchFactTableTest(FactTableTest):

    def setUp(self):
        self.initial.reset()

        self.connection_wrapper = pygrametl.getdefaulttargetconnection()

        self.batchsize = 100
        self.fact_table = BatchFactTable(name=self.initial.name,
                                         keyrefs=["bib", "cid", "did"],
                                         measures=["count", "profit"],
                                         batchsize=self.batchsize)

    def test_insert_less_than_batchsize_num_of_facts_without_commit(self):
        postcondition = self.initial

        # Generate and insert batchsize - 1 new facts
        for i in range(0, self.batchsize - 1):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})

        # Table is unchanged as batchsize is not reached and commit is not run
        postcondition.assertEqual()
        self.assertEqual(self.batchsize - 1, self.fact_table.awaitingrows)

        # The facts can still be looked up
        for i in range(0, self.batchsize - 1):
            expected_fact = {"bib": 10, "cid": 10,
                             "did": i, "count": i, "profit": i}
            actual_fact = self.fact_table.lookup(
                {"bib": 10, "cid": 10, "did": i})
            self.assertDictEqual(expected_fact, actual_fact)

    def test_insert_batchsize_num_of_facts_without_commit(self):
        postcondition = self.initial

        # Generate and insert batchsize number of new facts
        for i in range(0, self.batchsize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # Table is unchanged as batchsize is not reached and commit is not run
        postcondition.assertEqual()
        self.assertEqual(0, self.fact_table.awaitingrows)

    def test_insert_more_than_batchsize_num_of_facts_without_commit(self):
        postcondition = self.initial

        # Generate and insert batchsize number of new facts
        for i in range(0, self.batchsize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # Generate and insert 10 more facts, these should only be in memory
        for i in range(self.batchsize, self.batchsize + 10):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})

        # Only the first batchsize facts should be in the fact table
        postcondition.assertEqual()
        self.assertEqual(10, self.fact_table.awaitingrows)

        # The 10 extra facts can still be looked up
        for i in range(self.batchsize, self.batchsize + 10):
            expected_fact = {"bib": 10, "cid": 10,
                             "did": i, "count": i, "profit": i}
            actual_fact = self.fact_table.lookup(
                {"bib": 10, "cid": 10, "did": i})
            self.assertDictEqual(expected_fact, actual_fact)

    def test_insert_multiple_batches_without_commit(self):
        postcondition = self.initial

        # Generate and insert batchsize number of new facts
        for i in range(0, 3 * self.batchsize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # All facts should have been inserted to the fact table
        postcondition.assertEqual()
        self.assertEqual(0, self.fact_table.awaitingrows)


class BulkFactTableTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        utilities.ensure_default_connection_wrapper()
        cls.initial = dtt.Table("sales", """
        | bib:int (pk)  | cid:int (pk) | did:int (pk) | count:int | profit:int |
        | ------------- | ------------ | ------------ | --------- | ---------- |
        | 2             | 2            | 60           | 20        | 1000       |
        | 1             | 2            | 60           | 5         | 2000       |
        | 1             | 1            | 72           | 2         | 3000       |
        | 2             | 1            | 72           | 11        | 4000       |
        | 2             | 1            | 60           | 18        | 5000       |
        """)
        cls.bulksize = 100

    def setUp(self):
        utilities.ensure_default_connection_wrapper()
        self.initial.reset()
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize)

    def loader(self, name, attributes, fieldsep, rowsep, nullval, filehandle):
        sql = "INSERT INTO sales(bib, cid, did, count, profit) VALUES({}, {}, {}, {}, {})"
        encoding = utilities.get_os_encoding()

        # If the default rowsep is used
        if rowsep == '\n':
            for line in filehandle:
                values = line.decode(encoding).strip().split(fieldsep)
                insert = sql.format(*values)
                self.connection_wrapper.execute(insert)

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
                self.connection_wrapper.execute(insert)

    def test_insert_less_than_bulksize_number_of_facts(self):
        postcondition = self.initial

        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})

        # The inserted facts should not have been inserted into the table
        postcondition.assertEqual()
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_insert_bulksize_number_of_facts(self):
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the table
        postcondition.assertEqual()
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_insert_more_than_bulksize_num_of_facts(self):
        postcondition = self.initial

        # Generate and insert bulksize number of new facts
        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # Generate and insert 10 more facts, these should be in the tempfile
        for i in range(self.bulksize, self.bulksize + 10):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})

        # Only the first batchsize number of facts should have been inserted to
        # the fact table
        postcondition.assertEqual()
        self.assertEqual(10, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_insert_less_than_bulksize_number_of_facts_with_custom_tempdest(self):
        filehandle = tempfile.NamedTemporaryFile()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle)
        postcondition = self.initial
        inserted_facts = []

        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # The inserted facts should not have been inserted into the table yet
        postcondition.assertEqual()

        # Check that the passed tempfile contains the correct facts
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        facts_in_file = [line.decode(encoding).strip().split(
            '\t') for line in filehandle]

        self.assertEqual(inserted_facts, facts_in_file)
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_insert_bulksize_number_of_facts_with_custom_tempdest(self):
        filehandle = tempfile.NamedTemporaryFile()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()

        self.assertEqual(0, len(content))
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_insert_more_than_bulksize_num_of_facts_with_custom_tempdest(self):
        filehandle = tempfile.NamedTemporaryFile()
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle)
        postcondition = self.initial

        # Generate and insert bulksize number of new facts
        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |".format(
                    dayid=i, count=i, profit=i)

        # Generate and insert 10 more facts, these should be in the tempfile
        inserted_facts = []
        for i in range(self.bulksize, self.bulksize + 10):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # Only the first batchsize facts should be in the fact table
        postcondition.assertEqual()

        # Check that the passed tempfile contains only the last 10 facts
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        facts_in_file = [line.decode(encoding).strip().split(
            '\t') for line in filehandle]

        self.assertEqual(inserted_facts, facts_in_file)
        self.assertEqual(10, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_fields_are_separated_by_custom_fieldsep_in_file(self):
        filehandle = tempfile.NamedTemporaryFile()
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        fieldsep=fieldsep)
        postcondition = self.initial
        inserted_facts = []

        # Write bulksize - 1 number of facts to the file
        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
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
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_facts_are_loaded_correctly_using_custom_fieldsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        fieldsep=fieldsep)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert({"bib": 10, "cid": 10, "did": i,
                                    "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()

        self.assertEqual(0, len(content))
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_fields_are_separated_by_custom_rowsep_in_file(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        rowsep=rowsep)
        postcondition = self.initial
        inserted_facts = []

        # Write bulksize - 1 number of facts to the file
        for i in range(0, self.bulksize - 1):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            inserted_facts.append(['10', '10', str(i), str(i), str(i)])

        # The inserted facts should not have been inserted into the table yet
        postcondition.assertEqual()

        # Check that the passed tempfile contains the inserted facts with the
        # rows separated using rowsep
        encoding = utilities.get_os_encoding()
        filehandle.seek(0)
        file_content = filehandle.read().decode(encoding)
        facts_in_file = file_content.split(rowsep)
        facts_in_file_with_fields_separated = [fact.strip().split(
            '\t') for fact in facts_in_file if len(fact) != 0]

        self.assertEqual(inserted_facts, facts_in_file_with_fields_separated)
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_facts_are_loaded_correctly_using_custom_rowsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        rowsep=rowsep)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert({"bib": 10, "cid": 10,
                                    "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()

        self.assertEqual(0, len(content))
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_fields_and_rows_are_separated_by_custom_rowsep_and_fieldsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
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
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
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
        self.assertEqual(self.bulksize - 1, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()

    def test_facts_are_loaded_correctly_using_custom_rowsep_and_fieldsep(self):
        filehandle = tempfile.NamedTemporaryFile()
        rowsep = ' newline '
        fieldsep = ','
        self.fact_table = BulkFactTable(name=self.initial.name,
                                        keyrefs=["bib", "cid", "did"],
                                        measures=["count", "profit"],
                                        bulkloader=self.loader,
                                        bulksize=self.bulksize,
                                        tempdest=filehandle,
                                        rowsep=rowsep,
                                        fieldsep=fieldsep)
        postcondition = self.initial

        for i in range(0, self.bulksize):
            self.fact_table.insert(
                {"bib": 10, "cid": 10, "did": i, "count": i, "profit": i})
            postcondition = postcondition + \
                "| 10 | 10 | {dayid} | {count} | {profit} |" \
                .format(dayid=i, count=i, profit=i)

        # The inserted facts should have been inserted into the db table
        postcondition.assertEqual()

        # Check that the passed tempfile contains no facts
        filehandle.seek(0)
        content = filehandle.read()

        self.assertEqual(0, len(content))
        self.assertEqual(0, self.fact_table.awaitingrows)

        self.connection_wrapper.commit()


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
        self.connection_wrapper = pygrametl.getdefaulttargetconnection()
        self.fact_table = AccumulatingSnapshotFactTable(
            name=self.initial.name,
            keyrefs=self.initial.key(),
            otherrefs=['ref1', 'ref2', 'ref3'],
            measures=['meas', 'lag21'],
            factexpander=self.__complag)

    def __complag(self, row, namemapping, updated):
        if 'ref2' in updated:
            ref1inrow = namemapping.get('ref1') or 'ref1'
            ref2inrow = namemapping.get('ref2') or 'ref2'
            lag21inrow = namemapping.get('lag21') or 'lag21'
            row[lag21inrow] = row[ref2inrow] - row[ref1inrow]

    def test_insert_one_without_commit(self):
        self.fact_table.insert({'id1': 4, 'id2': 4, 'ref1': 3, 'ref2': 3,
                                'ref3': 3, 'meas': 3, 'lag21': 3})
        expected = self.initial + "|4|4|3|3|3|3|3|"
        expected.assertEqual()

    def test_insert_one_with_commit(self):
        self.fact_table.insert({'id1': 4, 'id2': 4, 'ref1': 3, 'ref2': 3,
                                'ref3': 3, 'meas': 3, 'lag21': 3})
        self.connection_wrapper.commit()
        expected = self.initial + "|4|4|3|3|3|3|3|"
        expected.assertEqual()

    def test_ensure_new(self):
        self.fact_table.ensure({'id1': 4, 'id2': 4, 'ref1': 4})
        expected = self.initial + "|4|4|4|NULL|NULL|NULL|NULL|"
        expected.assertEqual()

    def test_ensure_one_change(self):
        self.fact_table.ensure({'id1': 2, 'id2': 2, 'ref2': 3})
        expected = self.initial.update(1, "|2|2|2|3|NULL|2.0|1|")
        expected.assertEqual()

    def test_ensure_two_changes(self):
        self.fact_table.ensure({'id1': 2, 'id2': 2, 'ref2': 3, 'ref3': 3})
        self.fact_table.ensure({'id1': 2, 'id2': 2, 'ref2': 3, 'ref3': 4})
        expected = self.initial.update(1, "|2|2|2|3|4|2.0|1|")
        expected.assertEqual()

    def test_ensure_two_changes_with_namemapping(self):
        self.fact_table.ensure({'id1': 2, 'xid2': 2, 'xref2': 3, 'ref3': 3},
                               {'id2': 'xid2', 'ref2': 'xref2'})
        expected = self.initial.update(1, "|2|2|2|3|3|2.0|1|")
        expected.assertEqual()
