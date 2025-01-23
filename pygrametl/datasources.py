"""This module holds classes that can be used as data soures. Note that it is
   easy to create other data sources: A data source must be iterable and
   provide dicts that map from attribute names to attribute values.
"""

# Copyright (c) 2009-2023, Aalborg University (pygrametl@cs.aau.dk)
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

from csv import DictReader
import sys
import sqlite3

import pygrametl
from pygrametl import tables
from pygrametl import ConnectionWrapper


if sys.platform.startswith('java'):
    # Jython specific code
    from pygrametl.jythonmultiprocessing import Queue, Process
else:
    from multiprocessing import Queue, Process

try:
    from Queue import Empty  # Python 2
except ImportError:
    from queue import Empty  # Python 3


__all__ = ['CSVSource', 'TypedCSVSource', 'SQLSource', 'PandasSource',
           'JoiningSource', 'HashJoiningSource', 'MergeJoiningSource',
           'BackgroundSource', 'ProcessSource', 'MappingSource',
           'TransformingSource', 'SQLTransformingSource', 'UnionSource',
           'CrossTabbingSource', 'FilteringSource', 'DynamicForEachSource',
           'RoundRobinSource']


CSVSource = DictReader


class TypedCSVSource(DictReader):
    """A class for iterating a CSV file and type cast the values."""

    def __init__(self, f, casts, fieldnames=None, restkey=None,
                 restval=None, dialect='excel', *args, **kwds):
        """Arguments:

           - f: An iterable object such as as file. Passed on to
             csv.DictReader
           - casts: A dict mapping from attribute names to functions to apply
             to these names, e.g., {'id':int, 'salary':float}
           - fieldnames: Passed on to csv.DictReader
           - restkey: Passed on to csv.DictReader
           - restval: Passed on to csv.DictReader
           - dialect: Passed on to csv.DictReader
           - *args: Passed on to csv.DictReader
           - **kwds: Passed on to csv.DictReader
        """
        DictReader.__init__(self, f, fieldnames=fieldnames,
                            restkey=restkey, restval=restval, dialect=dialect,
                            *args, **kwds)

        if not type(casts) == dict:
            raise TypeError("The casts argument must be a dict")
        for v in casts.values():
            if not callable(v):
                raise TypeError("The values in casts must be callable")
        self._casts = casts

    def __next__(self):  # For Python 3
        row = DictReader.__next__(self)
        for (att, func) in self._casts.items():
            row[att] = func(row[att])
        return row

    def next(self):  # For Python 2
        row = DictReader.next(self)
        for (att, func) in self._casts.items():
            row[att] = func(row[att])
        return row


class SQLSource(object):

    """A class for iterating the result set of a single SQL query."""

    def __init__(self, connection, query, names=(), initsql=None,
                 cursorarg=None, parameters=None, fetch_size=500):
        """Arguments:

           - connection: the PEP 249 connection to use. NOT a
             ConnectionWrapper!
           - query: the query that generates the result
           - names: names of attributes in the result. If not set,
             the names from the database are used. Default: ()
           - initsql: SQL that is executed before the query. The result of this
             initsql is not returned. Default: None.
           - cursorarg: if not None, this argument is used as an argument when
             the connection's cursor method is called. Default: None.
           - parameters: if not None, this sequence or mapping of parameters
             will be sent when the query is executed.
           - fetch_size: The amount of rows to fetch into memory for each round trip to the source.
             All rows will be fetched at once if fetch_size is set to 0 or less.
        """
        self.connection = connection
        if cursorarg is not None:
            self.cursor = connection.cursor(cursorarg)
        else:
            self.cursor = connection.cursor()
        if initsql:
            self.cursor.execute(initsql)
        self.query = query
        self.names = names
        self.executed = False
        self.parameters = parameters

    def __iter__(self):
        try:
            if not self.executed:
                if self.parameters:
                    self.cursor.execute(self.query, self.parameters)
                else:
                    self.cursor.execute(self.query)
                names = None
                if self.names or self.cursor.description:
                    names = self.names or \
                        [t[0] for t in self.cursor.description]
            while True:
                if fetch_size <= 0:
                    data = self.cursor.fetchall()
                else:
                    data = self.cursor.fetchmany(fetch_size)

                if not data:
                    break
                if not names:
                    # We do this to support cursor objects that only have
                    # a meaningful .description after data has been fetched.
                    # This is, for example, the case when using a named
                    # psycopg2 cursor.
                    names = [t[0] for t in self.cursor.description]
                if len(names) != len(data[0]):
                    raise ValueError(
                        "Incorrect number of names provided. " +
                        "%d given, %d needed." % (len(names), len(data[0])))
                for row in data:
                    yield dict(zip(names, row))

                # It is not well defined in PEP 249 what fetchall will do if called twice
                # Therefore it is safest to break the loop if fetchall is used
                if fetch_size <= 0:
                    break
        finally:
            try:
                self.cursor.close()
            except Exception:
                pass

class PandasSource(object):

    """A source for iterating a Pandas DataFrame and cast each row to a dict."""

    def __init__(self, dataFrame):
        """Arguments:

           - dataFrame: A Pandas DataFrame
        """
        self._dataFrame = dataFrame

    def __iter__(self):
        for (_, series) in self._dataFrame.iterrows():
            row = series.to_dict()
            yield row


class ProcessSource(object):

    """A class for iterating another source in a separate process"""

    def __init__(self, source, batchsize=500, queuesize=20):
        """Arguments:

           - source: the source to iterate
           - batchsize: the number of rows passed from the worker process each
             time it passes on a batch of rows. Must be positive. Default: 500
           - queuesize: the maximum number of batches that can wait in a queue
             between the processes. 0 means unlimited. Default: 20
        """
        if not isinstance(batchsize, int) or batchsize < 1:
            raise ValueError('batchsize must be a positive integer')
        self.__source = source
        self.__batchsize = batchsize
        self.__queue = Queue(queuesize)
        p = Process(target=self.__worker)
        p.name = "Process for ProcessSource"
        p.start()

    def __worker(self):
        batch = []
        try:
            for row in self.__source:
                batch.append(row)
                if len(batch) == self.__batchsize:
                    self.__queue.put(batch)
                    batch = []
            # We're done. Send the batch if it has any data and a signal
            if batch:
                self.__queue.put(batch)
            self.__queue.put('STOP')
        except Exception:
            # Jython 2.5.X does not support the as syntax required by Python 3
            e = sys.exc_info()[1]

            if batch:
                self.__queue.put(batch)
            self.__queue.put('EXCEPTION')
            self.__queue.put(e)

    def __iter__(self):
        while True:
            data = self.__queue.get()
            if data == 'STOP':
                break
            elif data == 'EXCEPTION':
                exc = self.__queue.get()
                raise exc
            # else we got a list of rows from the other process
            for row in data:
                yield row

BackgroundSource = ProcessSource  # for compatability
# The old thread-based BackgroundSource has been removed and
# replaced by ProcessSource


class HashJoiningSource(object):

    """A class for equi-joining two data sources."""

    def __init__(self, src1, key1, src2, key2):
        """Arguments:

           - src1: the first source. This source is iterated row by row.
           - key1: the attribute of the first source to use in the join
           - src2: the second source. The rows of this source are all loaded
             into memory.
           - key2: the attriubte of the second source to use in the join.
        """
        self.__hash = {}
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2

    def __buildhash(self):
        for row in self.__src2:
            keyval = row[self.__key2]
            l = self.__hash.get(keyval, [])
            l.append(row)
            self.__hash[keyval] = l
        self.__ready = True

    def __iter__(self):
        self.__buildhash()
        for row in self.__src1:
            matches = self.__hash.get(row[self.__key1], [])
            for match in matches:
                newrow = row.copy()
                newrow.update(match)
                yield newrow


JoiningSource = HashJoiningSource  # for compatability


class MergeJoiningSource(object):

    """A class for merge-joining two sorted data sources"""

    def __init__(self, src1, key1, src2, key2):
        """Arguments:

        - src1: a data source
        - key1: the attribute to use from src1
        - src2: a data source
        - key2: the attribute to use from src2
        """
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2
        self.__next = None

    def __iter__(self):
        iter1 = self.__src1.__iter__()
        iter2 = self.__src2.__iter__()

        row1 = next(iter1)
        keyval1 = row1[self.__key1]
        rows2 = self.__getnextrows(iter2)
        keyval2 = rows2[0][self.__key2]

        try:
            while True:  # At one point there will be a StopIteration
                if keyval1 == keyval2:
                    # Output rows
                    for part in rows2:
                        resrow = row1.copy()
                        resrow.update(part)
                        yield resrow
                    row1 = next(iter1)
                    keyval1 = row1[self.__key1]
                elif keyval1 < keyval2:
                    row1 = next(iter1)
                    keyval1 = row1[self.__key1]
                else:  # k1 > k2
                    rows2 = self.__getnextrows(iter2)
                    keyval2 = rows2[0][self.__key2]
        except StopIteration:
            return # Needed in Python 3.7+ due to PEP 479

    def __getnextrows(self, iterval):
        res = []
        keyval = None
        if self.__next is not None:
            res.append(self.__next)
            keyval = self.__next[self.__key2]
            self.__next = None
        while True:
            try:
                row = next(iterval)
            except StopIteration:
                if res:
                    return res
                else:
                    raise
            if keyval is None:
                keyval = row[self.__key2]  # for the first row in this round
            if row[self.__key2] == keyval:
                res.append(row)
            else:
                self.__next = row
                return res


class MappingSource(object):
    """A class for iterating a source and applying a function to each column."""

    def __init__(self, source, callables):
        """Arguments:

           - source: A data source
           - callables: A dict mapping from attribute names to functions to
             apply to these names, e.g. type casting {'id':int, 'salary':float}
        """
        if not type(callables) == dict:
            raise TypeError("The callables argument must be a dict")
        for v in callables.values():
            if not callable(v):
                raise TypeError("The values in callables must be callable")

        self._source = source
        self._callables = callables

    def __iter__(self):
        for row in self._source:
            for (att, func) in self._callables.items():
                row[att] = func(row[att])
                yield row


class TransformingSource(object):

    """A source that applies functions to the rows from another source"""

    def __init__(self, source, *transformations):
        """Arguments:

        - source: a data source
        - *transformations: the transformations to apply. Must be callables
          of the form func(row) where row is a dict. Will be applied in the
          given order.
        """
        self.__source = source
        self.__transformations = transformations

    def __iter__(self):
        for row in self.__source:
            for func in self.__transformations:
                func(row)
            yield row


class SQLTransformingSource(object):

    """A source that transforms rows from another source by loading them into a
       temporary table in an RDBMS and then retrieving them using an SQL query.

       .. Warning::
          Creates, empties, and drops the temporary table.
    """

    def __init__(self, source, temptablename, query, additionalcasts=None,
                 batchsize=10000, perbatch=False, columnnames=None,
                 usetruncate=True, targetconnection=None):
        """Arguments:

        - source: a data source that yields rows with the same schema, i.e.,
          they contain the same columns and the columns' types do not change
        - temptablename: a string with the name of the temporary table to use.
          This table must use the same schema as the rows from source
        - query: the query that is executed on temptablename in targetconnection
          or an in-memory SQLite database to transforms the rows from source
        - additionalcasts: a dict with additional casts from Python types to SQL
          types in the form of strings that takes precedences over the default.
          Default: None, i.e., only int, float, and str is mapped to simple SQL
          types that should be supported by most RDBMSs
        - batchsize: an int deciding how many insert operations should be done
          in one batch. Default: 10000
        - perbatch: a boolean deciding if the transformation should be applied
          for each batch or for all rows in source.
          Default: False, i.e., the transformation is applied once for all rows
          in source
        - columnnames: a sequence of column names to use for transformed rows.
          Default: None, i.e., the column names from query are used
        - usetruncate: a boolean deciding if TRUNCATE should be used instead of
          DELETE FROM when emptying temptablename in targetconnection.
          Default: True, i.e.,  TRUNCATE is used instead of DELETE FROM
        - targetconnection: the PEP 249 connection to use, the ConnectionWrapper
          to use, or None. If None, a new temporary in-memory SQLite database is
          created
        """
        self.__source = source
        self.__query = query
        self.__batchsize = batchsize
        self.__perbatch = perbatch
        self.__columnnames = columnnames

        # Extensible mapping of Python types to SQL types
        self.__casts = {
            int: "INTEGER",
            float: "DOUBLE PRECISION",
            str: "VARCHAR(4000)",  # Maximum size for Oracle 11g R2
        }

        if additionalcasts:
            self.__casts |= additionalcasts

        # Only close the connection if it is created by SQLTransformingSource
        if targetconnection is None:
            self.__close = True
            targetconnection = sqlite3.connect(":memory:")

            # TRUNCATE is not supported in SQLite
            usetruncate=False
        else:
            self.__close = False

        # Check if targetconnection is a ConnectioNWrapper without importing
        # modules that require Jython when running under CPython and vice versa.
        # A ConnectionWrapper is always used to support multiple paramstyles
        if "ConnectionWrapper" in type(targetconnection).__name__:
            self.__targetconnection = targetconnection
        else:
            self.__targetconnection = ConnectionWrapper(targetconnection)

            # Ensure the implicitly created ConnectionWrapper is not default
            if self.__targetconnection == \
               pygrametl.getdefaulttargetconnection():
                pygrametl._defaulttargetconnection = None

        # Create table SQL
        self.__batch = []
        row = next(source)
        self.__batch.append(row)

        createsql = "CREATE TABLE {}({})".format(temptablename, ', '.join(
            [name + " " + self.__casts[type(value)]
             for name, value in row.items()]))
        self.__targetconnection.execute(createsql)
        self.__targetconnection.commit()

        # Create insert SQL
        # This gives "INSERT INTO tablename(att1, att2, att3, ...)
        #             VALUES (%(att1)s, %(att2)s, %(att3)s, ...)"
        quote = tables._quote
        quotelist = lambda x: [quote(xn) for xn in x]
        self.__insertsql = "INSERT INTO " + temptablename \
            + "(" + ", ".join(quotelist(row.keys())) + ") VALUES (" + \
            ", ".join(["%%(%s)s" % (att,) for att in row.keys()]) + ")"

        # Create drop and truncate SQL
        self.__dropsql = "DROP TABLE " + temptablename
        if usetruncate:
            self.__deletefrom = "TRUNCATE TABLE " + temptablename
        else:
            self.__deletefrom = "DELETE FROM " + temptablename

    def __iter__(self):
        for row in self.__source:
            # Insert and maybe transform current batch
            if len(self.__batch) == self.__batchsize:
                self.__insertnow()
                if self.__perbatch:
                    for transformed_row in self.__transform():
                        yield transformed_row
                    self.__targetconnection.execute(self.__deletefrom)

            # The first row is read and added in __init__
            self.__batch.append(row)

        # Insert last batch and transform last or entire batch
        self.__insertnow()
        for transformed_row in self.__transform():
            yield transformed_row

        # Cleanup
        self.__targetconnection.execute(self.__dropsql)
        self.__targetconnection.commit()
        if self.__close:
            self.__targetconnection.close()

    def __insertnow(self):
        if self.__batch:
            self.__targetconnection.executemany(self.__insertsql, self.__batch)
            self.__targetconnection.commit()
            self.__batch.clear()

    def __transform(self):
        self.__targetconnection.execute(self.__query)

        if self.__columnnames:
            return self.__targetconnection.rowfactory(self.__columnnames)
        else:
            return self.__targetconnection.rowfactory()


class CrossTabbingSource(object):

    """A source that produces a crosstab from another source"""

    def __init__(self, source, rowvaluesatt, colvaluesatt, values,
                 aggregator=None, nonevalue=0, sortrows=False):
        """Arguments:

        - source: the data source to pull data from
        - rowvaluesatt: the name of the attribute that holds the values that
          appear as rows in the result
        - colvaluesatt: the name of the attribute that holds the values that
          appear as columns in the result
        - values: the name of the attribute that holds the values to aggregate
        - aggregator: the aggregator to use (see pygrametl.aggregators). If not
          given, pygrametl.aggregators.Sum is used to sum the values
        - nonevalue: the value to return when there is no data to aggregate.
          Default: 0
        - sortrows: A boolean deciding if the rows should be sorted.
          Default: False
        """
        self.__source = source
        self.__rowvaluesatt = rowvaluesatt
        self.__colvaluesatt = colvaluesatt
        self.__values = values
        if aggregator is None:
            from pygrametl.aggregators import Sum
            self.__aggregator = Sum()
        else:
            self.__aggregator = aggregator
        self.__nonevalue = nonevalue
        self.__sortrows = sortrows
        self.__allcolumns = set()
        self.__allrows = set()

    def __iter__(self):
        for data in self.__source:  # first we iterate over all source data ...
            row = data[self.__rowvaluesatt]
            col = data[self.__colvaluesatt]
            self.__allrows.add(row)
            self.__allcolumns.add(col)
            self.__aggregator.process((row, col), data[self.__values])

        # ... and then we build result rows
        for row in (self.__sortrows and sorted(self.__allrows) or
                    self.__allrows):
            res = {self.__rowvaluesatt: row}
            for col in self.__allcolumns:
                res[col] = \
                    self.__aggregator.finish((row, col), self.__nonevalue)
            yield res


class FilteringSource(object):

    """A source that applies a filter to another source"""

    def __init__(self, source, filter=bool):
        """Arguments:

           - source: the source to filter
           - filter: a callable f(row). If the result is a True value,
             the row is passed on. If not, the row is discarded.
             Default: bool, i.e., Python's standard boolean conversion which
             removes empty rows.
        """
        self.__source = source
        self.__filter = filter

    def __iter__(self):
        for row in self.__source:
            if self.__filter(row):
                yield row


class UnionSource(object):

    """A source to union other sources (possibly with different types of rows).
    All rows are read from the 1st source before rows are read from the 2nd
    source and so on (to interleave the rows, use a RoundRobinSource)
    """

    def __init__(self, *sources):
        """Arguments:

           - *sources: The sources to union in the order they should be used.
        """
        self.__sources = sources

    def __iter__(self):
        for src in self.__sources:
            for row in src:
                yield row


class RoundRobinSource(object):

    """A source that reads sets of rows from sources in round robin-fashion"""

    def __init__(self, sources, batchsize=500):
        """Arguments:

           - sources: a sequence of data sources
           - batchsize: the amount of rows to read from a data source before
             going to the next data source. Must be positive (to empty a source
             before going to the next, use UnionSource)
        """
        self.__sources = [iter(src) for src in sources]
        self.__sources.reverse()  # we iterate it from the back in __iter__
        if not batchsize > 0:
            raise ValueError("batchsize must be positive")
        self.__batchsize = batchsize

    def __iter__(self):
        while self.__sources:
            # iterate from back
            for i in range(len(self.__sources) - 1, -1, -1):
                cursrc = self.__sources[i]
                # now return up to __batchsize from cursrc
                try:
                    for _ in range(self.__batchsize):
                        yield next(cursrc)
                except StopIteration:
                    # we're done with this source and can delete it since
                    # we iterate the list as we do
                    del self.__sources[i]
        return


class DynamicForEachSource(object):

    """A source that for each given argument creates a new source that
    will be iterated by this source.

    For example, useful for directories where a CSVSource should be created
    for each file.

    The user must provide a function that when called with a single argument,
    returns a new source to iterate. A DynamicForEachSource instance can be
    given to several ProcessSource instances.
    """

    def __init__(self, seq, callee):
        """Arguments:

           - seq: a sequence with the elements for each of which a unique
             source must be created. the elements are given (one by one) to
             callee.
           - callee: a function f(e) that must accept elements as those in the
             seq argument. the function should return a source which then will
             be iterated by this source. the function is called once for every
             element in seq.
        """
        self.__queue = Queue()  # a multiprocessing.Queue
        if not callable(callee):
            raise TypeError('callee must be callable')
        self.__callee = callee
        for e in seq:
            # put them in a safe queue such that this object can be used from
            # different fork'ed processes
            self.__queue.put(e)

    def __iter__(self):
        while True:
            try:
                arg = self.__queue.get(False)
                src = self.__callee(arg)
                for row in src:
                    yield row
            except Empty:
                return
