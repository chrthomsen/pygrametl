"""This module holds classes that can be used as data soures. Note that it is
   easy to create other data sources: A data source must be iterable and
   provide dicts that map from attribute names to attribute values.
"""

# Copyright (c) 2009-2014, Aalborg University (chr@cs.aau.dk)
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

from csv import DictReader

import sys
if sys.platform.startswith('java'):
    # Jython specific code
    from pygrametl.jythonmultiprocessing import Queue, Process
else:
    from multiprocessing import Queue, Process

try:
    from Queue import Empty # Python 2
except ImportError:
    from queue import Empty # Python 3

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '2.3a'
__all__ = ['CSVSource', 'SQLSource', 'JoiningSource', 'HashJoiningSource', 
           'MergeJoiningSource', 'BackgroundSource', 'ProcessSource', 
           'TransformingSource', 'UnionSource', 'CrossTabbingSource', 
           'FilteringSource', 'DynamicForEachSource', 'RoundRobinSource']


CSVSource = DictReader


class SQLSource(object):
    """A class for iterating the result set of a single SQL query."""

    def __init__(self, connection, query, names=(), initsql=None, \
                     cursorarg=None):
        """Arguments:
           - connection: the PEP 249 connection to use. NOT a ConnectionWrapper!
           - query: the query that generates the result
           - names: names of attributes in the result. If not set,
             the names from the database are used. Default: ()
           - initsql: SQL that is executed before the query. The result of this
             initsql is not returned. Default: None.
           - cursorarg: if not None, this argument is used as an argument when
             the connection's cursor method is called. Default: None.
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

    def __iter__(self):
        try:
            if not self.executed:
                self.cursor.execute(self.query)
                names = None
                if self.names or self.cursor.description:
                    names = self.names or \
                        [t[0] for t in self.cursor.description]
            while True:
                data = self.cursor.fetchmany(500)
                if not data:
                    break
                if not names:
                    # We do this to support cursor objects that only have
                    # a meaningful .description after data has been fetched.
                    # This is, for example, the case when using a named
                    # psycopg2 cursor.
                    names = [t[0] for t in self.cursor.description]
                if len(names) != len(data[0]):
                    raise ValueError(\
                        "Incorrect number of names provided. " + \
                        "%d given, %d needed." % (len(names), len(data[0])))
                for row in data:
                    yield dict(zip(names, row))
        finally:
            try:
                self.cursor.close()
            except Exception:
                pass


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
        if type(batchsize) != int or batchsize < 1:
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
        except Exception as e:
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

BackgroundSource = ProcessSource # for compatability
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


JoiningSource = HashJoiningSource # for compatability


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

        row1 = iter1.next()
        keyval1 = row1[self.__key1]
        rows2 = self.__getnextrows(iter2)
        keyval2 = rows2[0][self.__key2]

        while True: # At one point there will be a StopIteration
            if keyval1 == keyval2:
                # Output rows
                for part in rows2:
                    resrow = row1.copy()
                    resrow.update(part)
                    yield resrow
                row1 = iter1.next()
                keyval1 = row1[self.__key1]
            elif keyval1 < keyval2:
                row1 = iter1.next()
                keyval1 = row1[self.__key1]
            else: # k1 > k2
                rows2 = self.__getnextrows(iter2)
                keyval2 = rows2[0][self.__key2]

    def __getnextrows(self, iter):
        res = []
        keyval = None
        if self.__next is not None:
            res.append(self.__next)
            keyval = self.__next[self.__key2]
            self.__next = None
        while True:
            try:
                row = iter.next()
            except StopIteration:
                if res:
                    return res
                else:
                    raise
            if keyval is None:
                keyval = row[self.__key2] # for the first row in this round
            if row[self.__key2] == keyval:
                res.append(row)
            else:
                self.__next = row
                return res


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
        for data in self.__source: # first we iterate over all source data ...
            row = data[self.__rowvaluesatt]
            col = data[self.__colvaluesatt]
            self.__allrows.add(row)
            self.__allcolumns.add(col)
            self.__aggregator.process((row, col), data[self.__values])

        # ... and then we build result rows
        for row in (self.__sortrows and sorted(self.__allrows) \
                        or self.__allrows): 
            res = {self.__rowvaluesatt : row}
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
        self.__sources.reverse() # we iterate it from the back in __iter__
        if not batchsize > 0:
            raise ValueError("batchsize must be positive")
        self.__batchsize = batchsize

    def __iter__(self):
        while self.__sources:
            for i in range(len(self.__sources)-1, -1, -1): #iterate from back
                cursrc = self.__sources[i]
                # now return up to __batchsize from cursrc
                try:
                    for n in range(self.__batchsize):
                        yield cursrc.next()
                except StopIteration:
                    # we're done with this source and can delete it since
                    # we iterate the list as we do
                    del self.__sources[i]
        raise StopIteration()


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
           - seq: a sequence with the elements for each of which a unique source 
             must be created. the elements are given (one by one) to callee.
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
                raise StopIteration()
