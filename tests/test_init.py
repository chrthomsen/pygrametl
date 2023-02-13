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
import sys
import unittest
import pygrametl
import pygrametl.drawntabletesting as dtt
from sqlite3.dbapi2 import Timestamp, Date
from datetime import date, datetime
from unittest.mock import patch


class InitTest(unittest.TestCase):

    def setUp(self):
        self.row = {
            "firstname": "John",
            "lastname": "Doe",
            "age": 27,
            "sex": "male"
        }

        self.renaming = {
            "surname": "lastname",
            "gender": "sex"
        }

    def test_project(self):
        atts = ["firstname", "surname", "age", "gender"]
        row = pygrametl.project(atts, self.row, self.renaming)

        self.assertEqual("John", row["firstname"])
        self.assertEqual("Doe", row["surname"])
        self.assertEqual(27, row["age"])
        self.assertEqual("male", row["gender"])

    def test_copy(self):
        renaming = {
            "surname": "lastname",
            "gender": "sex",
            "years": "age",
            "years_in_country": "age"
        }

        row = pygrametl.copy(self.row, **renaming)

        self.assertIsNot(self.row, row)
        self.assertEqual("John", row["firstname"])
        self.assertEqual("Doe", row["surname"])
        self.assertEqual("male", row["gender"])
        self.assertEqual(27, row["years"])
        self.assertEqual(27, row["years_in_country"])

    def test_renamefromto(self):
        renaming = dict((v,k) for k,v in self.renaming.items())

        pygrametl.renamefromto(self.row, renaming)

        self.assertEqual("John", self.row["firstname"])
        self.assertEqual("Doe", self.row["surname"])
        self.assertEqual("male", self.row["gender"])
        self.assertEqual(27, self.row["age"])

    def test_renametofrom(self):
        pygrametl.renametofrom(self.row, self.renaming)

        self.assertEqual("John", self.row["firstname"])
        self.assertEqual("Doe", self.row["surname"])
        self.assertEqual("male", self.row["gender"])
        self.assertEqual(27, self.row["age"])

    def test_getint(self):
        for i in ("2", 2, 2.0):
            self.assertEqual(int, type(pygrametl.getint(i)))
            self.assertEqual(2, pygrametl.getint(i))

        self.assertIsNone(pygrametl.getint("not convertible"))

    def test_getlong(self):
        # long() is not supported in Python 3
        python_version = sys.version_info[0]
        if python_version < 3:
            for i in ("2", 2, 2.0):
                self.assertEqual(long, type(pygrametl.getlong(i)))

            self.assertIsNone(pygrametl.getlong("not convertible"))

    def test_getfloat(self):
        for i in ("2", "2.9", 2.7, 2):
            self.assertEqual(float, type(pygrametl.getfloat(i)))

        self.assertIsNone(pygrametl.getfloat("not convertible"))

    def test_getstr(self):
        for i in ("a string", 5, 5.7):
            self.assertEqual(str, type(pygrametl.getstr(i)))

    def test_getstrippedstr(self):
        unstripped = "     string      "
        stripped = pygrametl.getstrippedstr(unstripped)

        self.assertEqual("string", stripped)

    def test_getdbfriendlystr(self):
        self.assertEqual("None", pygrametl.getdbfriendlystr(None))
        self.assertEqual("1", pygrametl.getdbfriendlystr(True))
        self.assertEqual("0", pygrametl.getdbfriendlystr(False))
        self.assertEqual("10", pygrametl.getsqlfriendlystr(10))
        self.assertEqual("CustomNullValue",
                         pygrametl.getdbfriendlystr(None, "CustomNullValue"))

    def test_getsqlfriendlystr(self):
        self.assertEqual("NULL", pygrametl.getsqlfriendlystr(None))
        self.assertEqual("'string'", pygrametl.getsqlfriendlystr("string"))
        self.assertEqual("10", pygrametl.getsqlfriendlystr(10))

    def test_getstrornullvalue(self):
        self.assertEqual("None", pygrametl.getstrornullvalue(None))
        self.assertEqual("10", pygrametl.getstrornullvalue(10))
        self.assertEqual("CustomNullValue",
                         pygrametl.getstrornullvalue(None, "CustomNullValue"))

    def test_getbool(self):
        # Cases where True should be returned
        for i in (True, 1, "1", "t", "true", "True"):
            self.assertTrue(pygrametl.getbool(i))

        # Cases where False should be returned
        for i in (False, 0, "0", "f", "false", "False"):
            self.assertFalse(pygrametl.getbool(i))

        # Cases where neither True nor False should be returned
        self.assertIsNone(pygrametl.getbool("string"))

        # Cases with custom truevalues and falsevalues
        self.assertEqual(True, pygrametl.getbool(
            "CustomTrueValue", truevalues={"CustomTrueValue"}))
        self.assertEqual(False, pygrametl.getbool(
            "CustomFalseValue", falsevalues={"CustomFalseValue"}))

    def test_getdate(self):
        connection_wrapper = dtt.connectionwrapper()
        date_expected = Date(2021, 4, 16)
        date_actual = pygrametl.getdate(connection_wrapper, "2021-04-16")
        self.assertEqual(date_expected, date_actual)

        date_actual = pygrametl.gettimestamp(connection_wrapper, "string")
        self.assertEqual(None, date_actual)

    def test_gettimestamp(self):
        connection_wrapper = dtt.connectionwrapper()
        timestamp_expected = Timestamp(2021, 4, 16, 12, 55, 32)
        timestamp_actual = pygrametl.gettimestamp(connection_wrapper,
                                                  "2021-04-16 12:55:32")
        self.assertEqual(timestamp_expected, timestamp_actual)

        timestamp_actual = pygrametl.gettimestamp(connection_wrapper, "string")
        self.assertEqual(None, timestamp_actual)

    def test_getvalue(self):
        self.assertEqual("John", pygrametl.getvalue(
            self.row, "firstname", self.renaming))

        # 'surname' and 'gender' are renamed in mapping
        self.assertEqual("Doe", pygrametl.getvalue(
            self.row, "surname", self.renaming))
        self.assertEqual("male", pygrametl.getvalue(
            self.row, "gender", self.renaming))

        # Old keys can still be used despite renaming
        self.assertEqual("male", pygrametl.getvalue(
            self.row, "sex", self.renaming))

    def test_getvalueor(self):
        self.assertEqual("John", pygrametl.getvalueor(
            self.row, "firstname", self.renaming))

        # 'surname' and 'gender' are renamed in mapping
        self.assertEqual("Doe", pygrametl.getvalueor(
            self.row, "surname", self.renaming))
        self.assertEqual("male", pygrametl.getvalueor(
            self.row, "gender", self.renaming))

        # Old keys can still be used despite renaming
        self.assertEqual("male", pygrametl.getvalue(
            self.row, "sex", self.renaming))

        # The key 'salary' does not exist in dict
        self.assertEqual(None, pygrametl.getvalueor(
            self.row, "salary", self.renaming))

        self.assertEqual("Default value", pygrametl.getvalueor(
            self.row, "salary", self.renaming, "Default value"))

    def test_setdefaults_a_parameters_as_sequences_of_atts_and_defaults(self):
        atts = ["age", "sex", "salary", "nationality"]
        defaults = [0, "unknown", 180, "England"]
        pygrametl.setdefaults(self.row, atts, defaults)

        # Existing values should not be updated
        self.assertEqual(27, self.row["age"])
        self.assertEqual("male", self.row["sex"])

        # Values that are not present should be set to the default value
        self.assertEqual(180, self.row["salary"])
        self.assertEqual("England", self.row["nationality"])

    def test_setdefaults_a_exception(self):
        atts = ["age", "sex", "salary", "nationality"]
        defaults = [0, "unknown", 180]

        # An exception should be raised since the lists have different lengths
        self.assertRaises(ValueError, pygrametl.setdefaults,
                          self.row, atts, defaults)

    def test_setdefaults_b_parameters_as_pairs_of_atts_and_defaults(self):
        atts_and_defaults = [("age", 0), ("sex", "unknown"),
                             ("salary", 180), ("nationality", "England")]
        pygrametl.setdefaults(self.row, atts_and_defaults)

        # Existing values should not be updated
        self.assertEqual(27, self.row["age"])
        self.assertEqual("male", self.row["sex"])

        # Values that are not present should be set to the default value
        self.assertEqual(180, self.row["salary"])
        self.assertEqual("England", self.row["nationality"])

    def test_rowfactory_source_with_a_fetchmany(self):
        source = self.MockSourceWithFetchmany(300)
        self.rowfactory_test(source, True, 200)

        source = self.MockSourceWithFetchmany(300)
        self.rowfactory_test(source, False, 200)

    def test_rowfactory_source_with_b_next_or_fetchone(self):
        # If the source contains fetchone()
        source = self.MockSourceWithFetchone(300)
        self.rowfactory_test(source, True, 300)

        source = self.MockSourceWithFetchone(300)
        self.rowfactory_test(source, False, 300)

        # If source contains next()
        source = self.MockSourceWithNext(300)
        self.rowfactory_test(source, True, 300)

        source = self.MockSourceWithNext(300)
        self.rowfactory_test(source, False, 300)

    def test_rowfactory_source_with_c_fetchall(self):
        source = self.MockSourceWithFetchall(400)
        self.rowfactory_test(source, True, 400)

        source = self.MockSourceWithFetchall(400)
        self.rowfactory_test(source, False, 400)

    def rowfactory_test(self, source, close, expected_num_of_rows):
        dicts = pygrametl.rowfactory(source, ["somekey", "anotherkey"], close)
        dict_counter = 0

        for dictionary in dicts:
            self.assertEqual(dict, type(dictionary))
            self.assertTrue('somekey' in dictionary)
            self.assertTrue('anotherkey' in dictionary)
            dict_counter += 1

        self.assertEqual(expected_num_of_rows, dict_counter)

        times_close_should_be_called = 1 if close is True else 0
        self.assertEqual(times_close_should_be_called,
                         source.times_closed_was_called)

    class MockSourceWithFetchmany:
        def __init__(self, rows_left):
            self.rows_left = rows_left
            self.times_closed_was_called = 0

        def fetchmany(self, i):
            if self.rows_left < i:
                return False
            rows = set()

            for i in range(0, i):
                record = (i, i * 2)
                rows.add(record)
                self.rows_left -= 1
            return rows

        def close(self):
            self.times_closed_was_called += 1

    class MockSourceWithFetchone:
        def __init__(self, rows_left):
            self.rows_left = rows_left
            self.random_counter = 0
            self.times_closed_was_called = 0

        def fetchone(self):
            if self.rows_left > 0:
                self.rows_left -= 1
                return self.random_counter, self.random_counter * 2

            return None

        def close(self):
            self.times_closed_was_called += 1

    class MockSourceWithNext:
        def __init__(self, rows_left):
            self.rows_left = rows_left
            self.random_counter = 0
            self.times_closed_was_called = 0

        def next(self):
            if self.rows_left > 0:
                self.rows_left -= 1
                return self.random_counter, self.random_counter * 2

            return None

        def close(self):
            self.times_closed_was_called += 1

    class MockSourceWithFetchall:
        def __init__(self, rows_left):
            self.rows_left = rows_left
            self.times_closed_was_called = 0
            self.random_counter = 0

        def fetchall(self):
            rows = set()

            while self.rows_left > 0:
                record = (self.random_counter, self.random_counter * 2)
                rows.add(record)
                self.rows_left -= 1
                self.random_counter += 1
            return rows

        def close(self):
            self.times_closed_was_called += 1

    def test_endload(self):
        # pygrametl._alltables may contain tables added by other tests
        alltables = pygrametl._alltables
        pygrametl._alltables = []

        for _ in range(0, 10):
            self.MockDimensionOrFacttable()

        for dimension_or_facttable in pygrametl._alltables:
            self.assertEqual(
                0, dimension_or_facttable.times_endload_was_called)

        pygrametl.endload()

        for dimension_or_facttable in pygrametl._alltables:
            self.assertEqual(
                1, dimension_or_facttable.times_endload_was_called)
        pygrametl._alltables = alltables

    class MockDimensionOrFacttable:
        def __init__(self):
            self.times_endload_was_called = 0
            pygrametl._alltables.append(self)

        def endload(self):
            self.times_endload_was_called += 1

    def test_today(self):
        # Test that the first call returns the correct date
        date_first_call = pygrametl.today()
        date_expected = date.today()
        self.assertEqual(date_expected, date_first_call)

        # A second call on a later date is simulated using patch and mocking
        # 'pygrametl.date.today'. The returned date should still be the same as
        # for the first call
        future_date_ord = date_first_call.toordinal() + 2
        future_date = date.fromordinal(future_date_ord)
        with patch('pygrametl.date') as mock_date:
            mock_date.today.return_value = future_date
            self.assertEqual(date_first_call, pygrametl.today())

    def test_now(self):
        time_first_call = pygrametl.now()

        # A second call on a later time is simulated using patch and mocking
        # 'pygrametl.datetime.now'. The returned datetime should still be the
        # same as for the first call
        future_time_stamp = time_first_call.timestamp() + 5
        future_time = datetime.fromtimestamp(future_time_stamp)

        with patch('pygrametl.datetime') as mock_datetime:
            mock_datetime.now.return_value = future_time
            self.assertEqual(time_first_call, pygrametl.now())

    def test_ymdparser(self):
        date_expected = date(2021, 1, 1)

        self.assertEqual(date_expected, pygrametl.ymdparser("2021-01-01"))
        self.assertEqual(None, pygrametl.ymdparser(None))

        # Incorrect input or format
        self.assertRaises(Exception, pygrametl.ymdparser, "Not a date")
        self.assertRaises(Exception, pygrametl.ymdparser, "01-01-2021")
        self.assertRaises(Exception, pygrametl.ymdparser, "2050-01-01-01")

    def test_ymdhmsparser(self):
        datetime_expected = datetime(2021, 1, 1, 14, 32, 56)

        self.assertEqual(datetime_expected,
                         pygrametl.ymdhmsparser("2021-01-01 14:32:56"))
        self.assertEqual(None, pygrametl.ymdparser(None))

        # Incorrect input or format
        self.assertRaises(Exception, pygrametl.ymdhmsparser, "Not a datetime")
        self.assertRaises(Exception, pygrametl.ymdhmsparser,
                          "01-01-2021 14:32:56")
        self.assertRaises(Exception, pygrametl.ymdhmsparser, "2021-01-01")

    def test_datereader(self):
        datereader = pygrametl.datereader("date")

        year_str = '2021'
        month_str = '01'

        for i in range(1, 31):
            # Creates dicts where 'date' maps to a string representation of the
            # date in "yyyy-mm-dd" format
            day_str = '0' + str(i) if i <= 9 else str(i)
            date_str = year_str + '-' + month_str + '-' + day_str
            mydict = {
                "date": date_str,
                "price": 150
            }

            date_obj = datereader(None, mydict)
            self.assertTrue(type(date_obj == date))
            self.assertEqual(2021, date_obj.year)
            self.assertEqual(1, date_obj.month)
            self.assertEqual(i, date_obj.day)

    def test_datetimereader(self):
        datetimereader = pygrametl.datetimereader("time")

        date_str = "2021-01-31"

        for i in range(1, 59):
            # Creates dicts where 'time' maps to a string representation of a
            # datetime in "yyyy-mm-dd hh:mm:ss" format
            hours = i % 24
            hours_str = str(hours) if hours > 9 else "0" + str(hours)
            minutes_str = str(i) if i > 9 else "0" + str(i)
            seconds_str = minutes_str
            datetime_str = date_str + ' ' + hours_str + \
                ':' + minutes_str + ":" + seconds_str
            mydict = {
                "time": datetime_str,
                "price": 150
            }

            datetime_obj = datetimereader(None, mydict)
            self.assertTrue(type(datetime_obj == datetime))
            self.assertEqual(2021, datetime_obj.year)
            self.assertEqual(1, datetime_obj.month)
            self.assertEqual(31, datetime_obj.day)
            self.assertEqual(hours, datetime_obj.hour)
            self.assertEqual(i, datetime_obj.minute)
            self.assertEqual(i, datetime_obj.second)

    def test_datespan_strings(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)
        dategen = pygrametl.datespan("2021-01-01", "2021-01-31")

        self.datespan_test_method(fromdate, todate, dategen)

    def test_datespan_datetime_dates(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)
        dategen = pygrametl.datespan(fromdate, todate)

        self.datespan_test_method(fromdate, todate, dategen)

    def test_datespan_datetime_dates_custom_key(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)
        dategen = pygrametl.datespan(fromdate, todate, key='dateinteger')

        self.datespan_test_method(fromdate, todate, dategen, key='dateinteger')

    def test_datespan_datetime_date_fromdateincl_todateincl(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)

        dategen = pygrametl.datespan(fromdate, todate, fromdateincl=False)
        self.datespan_test_method(date.fromordinal(
            fromdate.toordinal() + 1), todate, dategen)

        dategen = pygrametl.datespan(fromdate, todate, todateincl=False)
        self.datespan_test_method(fromdate, date.fromordinal(
            todate.toordinal() - 1), dategen)

        dategen = pygrametl.datespan(fromdate, todate, fromdateincl=False)
        self.datespan_test_method(date.fromordinal(
            fromdate.toordinal() + 1), todate, dategen)

        dategen = pygrametl.datespan(
            fromdate, todate, fromdateincl=False, todateincl=False)
        self.datespan_test_method(date.fromordinal(fromdate.toordinal() + 1),
                                  date.fromordinal(todate.toordinal() - 1),
                                  dategen)

    def test_datespan_datetime_date_custom_strings_and_ints(self):
        fromdate = date(2021, 1, 2)
        todate = date(2021, 1, 2)

        # Tests that date is now 'dd-mm-yyyy' as specified in 'strings'
        dategen = pygrametl.datespan(
            fromdate, todate, strings={'date': '%d-%m-%Y'})
        date_str = fromdate.strftime('%d-%m-%Y')
        self.assertEqual(date_str, next(dategen)['date'])

        # Tests that year now maps to the month as specified in 'ints'
        dategen = pygrametl.datespan(fromdate, todate, ints={'year': '%m'})
        year_str = int(fromdate.strftime('%m'))
        self.assertEqual(year_str, next(dategen)['year'])

    def datespan_test_method(self, fromdate, todate, dategen,
                             key='dateid'):
        date_counter = fromdate.toordinal()

        for date_dict in dategen:
            date_obj = date.fromordinal(date_counter)

            dateid = int(date_obj.strftime('%Y%m%d'))

            date_str = date_obj.strftime('%Y-%m-%d')
            monthname = date_obj.strftime('%B')
            weekday = date_obj.strftime('%A')

            year = int(date_obj.strftime('%Y'))
            month = int(date_obj.strftime('%m'))
            day = int(date_obj.strftime('%d'))

            self.assertEqual(dateid, date_dict[key])
            self.assertEqual(date_str, date_dict['date'])
            self.assertEqual(monthname, date_dict['monthname'])
            self.assertEqual(weekday, date_dict['weekday'])
            self.assertEqual(year, date_dict['year'])
            self.assertEqual(month, date_dict['month'])
            self.assertEqual(day, date_dict['day'])

            date_counter += 1

        date_counter -= 1

        # Tests that the date of the last dict in generator is in fact todate
        self.assertEqual(todate.toordinal(), date_counter)

    def test_toupper(self):
        for string in ("STRING STRING", "string string", "StrINg StriNG"):
            self.assertEqual("STRING STRING", pygrametl.toupper(string))

    def test_tolower(self):
        for string in ("STRING STRING", "string string", "StrINg StrInG"):
            self.assertEqual("string string", pygrametl.tolower(string))

    def test_keepasis(self):
        for string in ("aaa aaa", "AAA AAA", "AaA AaA"):
            self.assertEqual(string, pygrametl.keepasis(string))

    def test_getdefaulttargetconnection(self):
        # _defaulttargetconnection may contain a connectionwrapper set by other
        pygrametl._defaulttargetconnection = None

        # Create a default sqlite3 connection and ConnectionWrapper. This should
        # then be the default target connection wrapper
        connection_first = sqlite3.connect(':memory:')
        connectionwrapper_first = pygrametl.ConnectionWrapper(connection_first)
        self.assertEqual(connectionwrapper_first,
                         pygrametl.getdefaulttargetconnection())

        # Create a new connection. The returned default target connection
        # wrapper should still be the previous connection wrapper
        connection_second = sqlite3.connect(':memory:')
        connectionwrapper_second = pygrametl.ConnectionWrapper(connection_second)
        self.assertNotEqual(connectionwrapper_second,
                            pygrametl.getdefaulttargetconnection())
        self.assertEqual(connectionwrapper_first,
                         pygrametl.getdefaulttargetconnection())

        # Sets the second connection wrapper as default and tests that this
        # connection wrapper is now the default target connection wrapper
        connectionwrapper_second.setasdefault()
        self.assertEqual(connectionwrapper_second,
                         pygrametl.getdefaulttargetconnection())
        self.assertNotEqual(connectionwrapper_first,
                            pygrametl.getdefaulttargetconnection())
