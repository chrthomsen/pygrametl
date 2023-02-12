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
        self.dict = {
            "firstname": "John",
            "lastname": "Doe",
            "age": 27,
            "sex": "male"
        }

        self.renaming = {
            "surname": "lastname",
            "gender": "sex"
        }

    # Tests that the 'lastname' and 'sex' attributes are correctly renamed to 'surname' and 'gender'
    def test_project(self):
        atts = ["firstname", "surname", "age", "gender"]
        res = pygrametl.project(atts, self.dict, self.renaming)

        self.assertEqual("John", res["firstname"])
        self.assertEqual("Doe", res["surname"])
        self.assertEqual(27, res["age"])
        self.assertEqual("male", res["gender"])

    # The dictionary is copied with 'lastname' and 'sex' renamed to 'surname' and 'gender',
    # and both 'years' and 'years_in_country' maps to the same oldname
    def test_copy(self):
        renaming = {
            "surname": "lastname",
            "gender": "sex",
            "years": "age",
            "years_in_country": "age"
        }

        res = pygrametl.copy(self.dict, **renaming)

        self.assertEqual("John", res["firstname"])
        self.assertEqual("Doe", res["surname"])
        self.assertEqual("male", res["gender"])
        self.assertEqual(27, res["years"])
        self.assertEqual(27, res["years_in_country"])

        # Check that the two dict references actually point to different objects
        self.assertFalse(self.dict is res)

    # The keys "lastname" and "sex" should be renamed to "surname" and "gender" in the original dict
    def test_renamefromto(self):
        renaming = {
            "lastname": "surname",
            "sex": "gender"
        }
        pygrametl.renamefromto(self.dict, renaming)

        self.assertEqual("John", self.dict["firstname"])
        self.assertEqual("Doe", self.dict["surname"])
        self.assertEqual("male", self.dict["gender"])
        self.assertEqual(27, self.dict["age"])

    # The keys "lastname" and "sex" should be renamed to "surname" and "gender" in the original dict
    def test_renametofrom(self):
        pygrametl.renametofrom(self.dict, self.renaming)

        self.assertEqual("John", self.dict["firstname"])
        self.assertEqual("Doe", self.dict["surname"])
        self.assertEqual("male", self.dict["gender"])
        self.assertEqual(27, self.dict["age"])

    def test_getint(self):
        for i in ("2", 2, 2.0):
            self.assertEqual(int, type(pygrametl.getint(i)))
            self.assertEqual(2, pygrametl.getint(i))

        self.assertIsNone(pygrametl.getint("not convertible"))

    # long() is not supported in Python 3 - assertions are only tested if python
    # version is older than 3
    def test_getlong(self):
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
        self.assertEqual("1", pygrametl.getdbfriendlystr(True))
        self.assertEqual("0", pygrametl.getdbfriendlystr(False))
        self.assertEqual("None", pygrametl.getdbfriendlystr(None))
        self.assertEqual("CustomNullValue", pygrametl.getdbfriendlystr(None, "CustomNullValue"))

    def test_getstrornullvalue(self):
        self.assertEqual("10", pygrametl.getstrornullvalue(10))
        self.assertEqual("None", pygrametl.getstrornullvalue(None))
        self.assertEqual("CustomNullValue", pygrametl.getstrornullvalue(None, "CustomNullValue"))

    def test_getbool(self):
        # Cases where True should be returned
        for i in (True, 1, "1", "t", "true", "True"):
            self.assertTrue(pygrametl.getbool(i))

        # Cases where False should be returned
        for i in (False, 0, "0", "f", "false", "False"):
            self.assertFalse(pygrametl.getbool(i))

        # Cases with custom truevalues and falsevalues
        self.assertEqual(True, pygrametl.getbool("CustomTrueValue", truevalues={"CustomTrueValue"}))
        self.assertEqual(False, pygrametl.getbool("CustomFalseValue", falsevalues={"CustomFalseValue"}))

    # Test performed using the default connection to a temporary SQLite in-memory database
    def test_getdate(self):
        cw = dtt.connectionwrapper()

        date_expected = Date(2021, 4, 16)
        date_actual = pygrametl.getdate(cw, "2021-04-16")
        self.assertEqual(date_expected, date_actual)

    def test_gettimestamp(self):
        cw = dtt.connectionwrapper()

        timestamp_expected = Timestamp(2021, 4, 16, 12, 55, 32)
        timestamp_actual = pygrametl.gettimestamp(cw, "2021-04-16 12:55:32")
        self.assertEqual(timestamp_expected, timestamp_actual)

    def test_getvalue(self):
        self.assertEqual("John", pygrametl.getvalue(self.dict, "firstname", self.renaming))

        # 'surname' and 'gender' are renamed in mapping
        self.assertEqual("Doe", pygrametl.getvalue(self.dict, "surname", self.renaming))
        self.assertEqual("male", pygrametl.getvalue(self.dict, "gender", self.renaming))

        # Old keys can still be used despite renaming
        self.assertEqual("male", pygrametl.getvalue(self.dict, "sex", self.renaming))

    def test_getvalueor(self):
        self.assertEqual("John", pygrametl.getvalueor(self.dict, "firstname", self.renaming))

        # 'surname' and 'gender' are renamed in mapping
        self.assertEqual("Doe", pygrametl.getvalueor(self.dict, "surname", self.renaming))
        self.assertEqual("male", pygrametl.getvalueor(self.dict, "gender", self.renaming))

        # The key 'salary' does not exist in dict
        self.assertEqual(None, pygrametl.getvalueor(self.dict, "salary", self.renaming))

        self.assertEqual("Default value", pygrametl.getvalueor(self.dict, "salary", self.renaming, "Default value"))

    # The parameters are passed as in case A): a sequence of atts and an equally long sequence of defaults
    def test_setdefaults_parameters_as_a_sequence_of_atts_and_a_sequence_of_defaults(self):
        atts = ["age", "sex", "salary", "nationality"]
        defaults = [0, "unknown", 180, "England"]
        pygrametl.setdefaults(self.dict, atts, defaults)

        # Existing values should not be updated
        self.assertEqual(27, self.dict["age"])
        self.assertEqual("male", self.dict["sex"])

        # Values that are not present should be set to the default value
        self.assertEqual(180, self.dict["salary"])
        self.assertEqual("England", self.dict["nationality"])

    # The parameters are passed as in case B): a sequence of (attribute, defaultvalue) pairs
    def test_setdefaults_parameters_as_pairs_of_atts_and_defaults(self):
        atts = [("age", 0), ("sex", "unknown"), ("salary", 180), ("nationality", "England")]
        pygrametl.setdefaults(self.dict, atts)

        # Existing values should not be updated
        self.assertEqual(27, self.dict["age"])
        self.assertEqual("male", self.dict["sex"])

        # Values that are not present should be set to the default value
        self.assertEqual(180, self.dict["salary"])
        self.assertEqual("England", self.dict["nationality"])

    def test_setdefaults_exception(self):
        atts = ["age", "sex", "salary", "nationality"]
        defaults = [0, "unknown", 180]

        # Exception should be raised since the lists have different lengths
        self.assertRaises(ValueError, pygrametl.setdefaults, self.dict, atts, defaults)

    def test_rowfactory_source_with_fetchmany(self):
        source = self.MockSourceWithFetchmany(300)
        self.rowfactory_test(source, True, 200)

        source = self.MockSourceWithFetchmany(300)
        self.rowfactory_test(source, False, 200)

    def test_rowfactory_source_with_next_or_fetchone(self):
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

    def test_rowfactory_source_with_fetchall(self):
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
        self.assertEqual(times_close_should_be_called, source.times_closed_was_called)

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
            self.assertEqual(0, dimension_or_facttable.times_endload_was_called)

        pygrametl.endload()

        for dimension_or_facttable in pygrametl._alltables:
            self.assertEqual(1, dimension_or_facttable.times_endload_was_called)
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

        # A second call on a later date is simulated using patch and mocking 'pygrametl.date.today'.
        # The returned date should still be the same as for the first call
        future_date_ord = date_first_call.toordinal() + 2
        future_date = date.fromordinal(future_date_ord)
        with patch('pygrametl.date') as mock_date:
            mock_date.today.return_value = future_date
            self.assertEqual(date_first_call, pygrametl.today())

    def test_now(self):
        time_first_call = pygrametl.now()

        # A second call on a later time is simulated using patch and mocking 'pygrametl.datetime.now'.
        # The returned datetime should still be the same as for the first call
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

        self.assertEqual(datetime_expected, pygrametl.ymdhmsparser("2021-01-01 14:32:56"))
        self.assertEqual(None, pygrametl.ymdparser(None))

        # Incorrect input or format
        self.assertRaises(Exception, pygrametl.ymdhmsparser, "Not a datetime")
        self.assertRaises(Exception, pygrametl.ymdhmsparser, "01-01-2021 14:32:56")
        self.assertRaises(Exception, pygrametl.ymdhmsparser, "2021-01-01")

    def test_datereader(self):
        datereader = pygrametl.datereader("date")

        year_str = '2021'
        month_str = '01'

        for i in range(1, 31):
            # Creates dicts where 'date' maps to a string representation of the date
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
            # Creates dicts where 'time' maps to a string representation of a datetime in "yyyy-mm-dd hh:mm:ss" format
            hours = i % 24
            hours_str = str(hours) if hours > 9 else "0" + str(hours)
            minutes_str = str(i) if i > 9 else "0" + str(i)
            seconds_str = minutes_str
            datetime_str = date_str + ' ' + hours_str + ':' + minutes_str + ":" + seconds_str
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

    # Tests the method using string-formatted dates as arguments
    def test_datespan_strings(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)
        dategen = pygrametl.datespan("2021-01-01", "2021-01-31")

        self.datespan_test_method(fromdate, todate, dategen)

    # Tests the method using datetime.dates as arguments
    def test_datespan_dates(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)
        dategen = pygrametl.datespan(fromdate, todate)

        self.datespan_test_method(fromdate, todate, dategen)

    # Uses a custom key instead of 'dateid'
    def test_datespan_custom_key(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)
        dategen = pygrametl.datespan(fromdate, todate, key='dateinteger')

        self.datespan_test_method(fromdate, todate, dategen, key='dateinteger')

    # Tests that the values of fromdateincl and todateincl results in the expected behavior
    def test_datespan_incl(self):
        fromdate = date(2021, 1, 1)
        todate = date(2021, 1, 31)

        dategen = pygrametl.datespan(fromdate, todate, fromdateincl=False)
        self.datespan_test_method(date.fromordinal(fromdate.toordinal() + 1), todate, dategen)

        dategen = pygrametl.datespan(fromdate, todate, todateincl=False)
        self.datespan_test_method(fromdate, date.fromordinal(todate.toordinal() - 1), dategen)

        dategen = pygrametl.datespan(fromdate, todate, fromdateincl=False)
        self.datespan_test_method(date.fromordinal(fromdate.toordinal() + 1), todate, dategen)

        dategen = pygrametl.datespan(fromdate, todate, fromdateincl=False, todateincl=False)
        self.datespan_test_method(date.fromordinal(fromdate.toordinal() + 1), date.fromordinal(todate.toordinal() - 1),
                                  dategen)

    def test_datespan_custom_strings_and_ints(self):
        fromdate = date(2021, 1, 2)
        todate = date(2021, 1, 2)

        # Tests that the date format is now 'dd-mm-yyyy' as specified in 'strings' argument
        dategen = pygrametl.datespan(fromdate, todate, strings={'date': '%d-%m-%Y'})
        date_str = fromdate.strftime('%d-%m-%Y')
        self.assertEqual(date_str, next(dategen)['date'])

        # Tests that the year key now maps to the month of the date as specified in 'ints'
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
        # Create a default sqlite3 connection and ConnectionWrapper. This should be the default target connection
        connectionwrapper_first = dtt.connectionwrapper()
        self.assertEqual(connectionwrapper_first, pygrametl.getdefaulttargetconnection())

        # Create a new connection. The returned default target connection should still be the previous connection
        newconnection = sqlite3.connect(':memory:')
        connectionwrapper_second = pygrametl.ConnectionWrapper(newconnection)
        self.assertNotEqual(connectionwrapper_second, pygrametl.getdefaulttargetconnection())
        self.assertEqual(connectionwrapper_first, pygrametl.getdefaulttargetconnection())

        # Sets the second connection as default and tests that this connection is in fact the default now
        connectionwrapper_second.setasdefault()
        self.assertEqual(connectionwrapper_second, pygrametl.getdefaulttargetconnection())
        self.assertNotEqual(connectionwrapper_first, pygrametl.getdefaulttargetconnection())
