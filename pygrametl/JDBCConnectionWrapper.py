"""This module holds a ConnectionWrapper that is used with a 
   JDBC Connection. The module should only be used when running Jython.
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

import java.sql as jdbc

from copy import copy as pcopy
from datetime import datetime
from sys import modules
from threading import Thread
from Queue import Queue

import pygrametl
from pygrametl.FIFODict import FIFODict

# NOTE: This module is made for Jython.

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '2.2'
__all__ = ['JDBCConnectionWrapper', 'BackgroundJDBCConnectionWrapper']


class JDBCConnectionWrapper(object):
    """Wrap a JDBC Connection.

       All Dimension and FactTable communicate with the data warehouse using 
       a ConnectionWrapper. In this way, the code for loading the DW does not
       have to care about which parameter format is used.  
       This ConnectionWrapper is a special one for JDBC in Jython.
    """

    def __init__(self, jdbcconn, stmtcachesize=20):
        """Create a ConnectionWrapper around the given JDBC connection.

           If no default ConnectionWrapper already exists, the new
           ConnectionWrapper is set to be the default ConnectionWrapper.

           Arguments:
           - jdbcconn: An open JDBC Connection (not a PEP249 Connection)
           - stmtcachesize: The maximum number of PreparedStatements kept
             open. Default: 20.
        """
        if not isinstance(jdbcconn, jdbc.Connection):
            raise TypeError, '1st argument must implement java.sql.Connection'
        if jdbcconn.isClosed():
            raise ValueError, '1st argument must be an open Connection'
        self.__jdbcconn = jdbcconn
        # Add a finalizer to __prepstmts to close PreparedStatements when
        # they are pushed out
        self.__prepstmts = FIFODict(stmtcachesize, lambda k, v: v[0].close())
        self.__resultmeta = FIFODict(stmtcachesize)
        self.__resultset = None
        self.__resultnames = None
        self.__resulttypes = None
        self.nametranslator = lambda s: s
        self.__jdbcconn.setAutoCommit(False)
        if pygrametl._defaulttargetconnection is None:
            pygrametl._defaulttargetconnection = self

    def __preparejdbcstmt(self, sql):
        # Find pyformat arguments and change them to question marks while
        # appending the attribute names to a list
        names = []
        newsql = sql
        while True:
            start = newsql.find('%(')
            if start == -1:
                break
            end = newsql.find(')s', start)
            if end == -1: 
                break
            name = newsql[start+2 : end]
            names.append(name)
            newsql = newsql.replace(newsql[start:end+2], '?', 1)

        ps = self.__jdbcconn.prepareStatement(newsql)

        # Find parameter types
        types = []
        parmeta = ps.getParameterMetaData()
        for i in range(len(names)):
            types.append(parmeta.getParameterType(i+1))

        self.__prepstmts[sql] = (ps, names, types)

    def __executejdbcstmt(self, sql, args):
        if self.__resultset:
            self.__resultset.close()

        if sql not in self.__prepstmts:
            self.__preparejdbcstmt(sql)
        (ps, names, types) = self.__prepstmts[sql]

        for pos in range(len(names)): # Not very Pythonic, but we're doing Java
            if args[names[pos]] is None:
                ps.setNull(pos + 1, types[pos])
            else:
                ps.setObject(pos + 1, args[names[pos]], types[pos])

        if ps.execute():
            self.__resultset = ps.getResultSet()
            if sql not in self.__resultmeta:
                self.__resultmeta[sql] = \
                    self.__extractresultmetadata(self.__resultset)
            (self.__resultnames, self.__resulttypes) = self.__resultmeta[sql]
        else:
            self.__resultset = None
            (self.__resultnames, self.__resulttypes) = (None, None)

    def __extractresultmetadata(self, resultset):
        # Get jdbc resultset metadata. extract names and types
        # and add it to self.__resultmeta
        meta = resultset.getMetaData()
        names = []
        types = []
        for col in range(meta.getColumnCount()):
            names.append(meta.getColumnName(col+1))
            types.append(meta.getColumnType(col+1))
        return (names, types)

    def __readresultrow(self):
        if self.__resultset is None:
            return None
        result = []
        for i in range(len(self.__resulttypes)):
            e = self.__resulttypes[i] # Not Pythonic, but we need i for JDBC
            if e in (jdbc.Types.CHAR, jdbc.Types.VARCHAR, 
                     jdbc.Types.LONGVARCHAR):
                result.append(self.__resultset.getString(i+1))
            elif e in (jdbc.Types.BIT, jdbc.Types.BOOLEAN):
                result.append(self.__resultset.getBool(i+1))
            elif e in (jdbc.Types.TINYINT, jdbc.Types.SMALLINT, 
                       jdbc.Types.INTEGER):
                result.append(self.__resultset.getInt(i+1))
            elif e in (jdbc.Types.BIGINT, ):
                result.append(self.__resultset.getLong(i+1))
            elif e in (jdbc.Types.DATE, ):
                result.append(self.__resultset.getDate(i+1))
            elif e in (jdbc.Types.TIMESTAMP, ):
                result.append(self.__resultset.getTimestamp(i+1))
            elif e in (jdbc.Types.TIME, ):
                result.append(self.__resultset.getTime(i+1))
            else:
                # Try this and hope for the best...
                result.append(self.__resultset.getString(i+1))
        return tuple(result)

    def execute(self, stmt, arguments=None, namemapping=None, ignored=None):
        """Execute a statement.

           Arguments:
           - stmt: the statement to execute
           - arguments: a mapping with the arguments. Default: None.
           - namemapping: a mapping of names such that if stmt uses %(arg)s
             and namemapping[arg]=arg2, the value arguments[arg2] is used 
             instead of arguments[arg]
           - ignored: An ignored argument only present to accept the same 
             number of arguments as ConnectionWrapper.execute
        """
        if namemapping and arguments:
            arguments = pygrametl.copy(arguments, **namemapping)
        self.__executejdbcstmt(stmt, arguments)

    def executemany(self, stmt, params, ignored=None):
        """Execute a sequence of statements.

           Arguments:
           - stmt: the statement to execute
           - params: a sequence of arguments
           - ignored: An ignored argument only present to accept the same 
             number of arguments as ConnectionWrapper.executemany
        """
        for paramset in params:
            self.__executejdbcstmt(stmt, paramset)

    def rowfactory(self, names=None):
        """Return a generator object returning result rows (i.e. dicts)."""
        if names is None:
            if self.__resultnames is None:
                return
            else:
                names = [self.nametranslator(t[0]) for t in self.__resultnames]
        empty = (None, ) * len(self.__resultnames)
        while True:
            tuple = self.fetchonetuple()
            if tuple == empty:
                return
            yield dict(zip(names, tuple))

    def fetchone(self, names=None):
        """Return one result row (i.e. dict)."""
        if self.__resultset is None:
            return {}
        if names is None:
            names = [self.nametranslator(t[0]) for t in self.__resultnames]
        values = self.fetchonetuple()
        return dict(zip(names, values))

    def fetchonetuple(self):
        """Return one result tuple."""
        if self.__resultset is None:
            return ()
        if not self.__resultset.next():
            return (None, ) * len(self.__resultnames)
        else:
            return self.__readresultrow()

    def fetchmanytuples(self, cnt):
        """Return cnt result tuples."""
        if self.__resultset is None:
            return []
        empty = (None, ) * len(self.__resultnames)
        result = []
        for i in range(cnt):
            tmp = self.fetchonetuple()
            if tmp == empty:
                break
            result.append(tmp)
        return result

    def fetchalltuples(self):
        """Return all result tuples"""
        if self.__resultset is None:
            return []
        result = []
        empty = (None, ) * len(self.__resultnames)
        while True:
            tmp = self.fetchonetuple()
            if tmp == empty:
                return result
            result.append(tmp)

    def rowcount(self):
        """Not implemented. Return 0. Should return the size of the result."""
        return 0 

    def getunderlyingmodule(self):
        """Return a reference to the underlying connection's module."""
        return modules[self.__class__.__module__]

    def commit(self):
        """Commit the transaction."""
        pygrametl.endload()
        self.__jdbcconn.commit()

    def close(self):
        """Close the connection to the database,"""
        self.__jdbcconn.close()

    def rollback(self):
        """Rollback the transaction."""
        self.__jdbcconn.rollback()

    def setasdefault(self):
        """Set this ConnectionWrapper as the default connection."""
        pygrametl._defaulttargetconnection = self

    def cursor(self):
        """Not implemented for this JDBC connection wrapper!"""
        raise NotImplementedError, ".cursor() not supported"

    def resultnames(self):
        if self.__resultnames is None:
            return None
        else:
            return tuple(self.__resultnames)

# BackgroundJDBCConnectionWrapper is added for experiments. It is quite similar
# to JDBCConnectionWrapper and one of them may be removed.

class BackgroundJDBCConnectionWrapper(object):
    """Wrap a JDBC Connection and do all DB communication in the background.

       All Dimension and FactTable communicate with the data warehouse using 
       a ConnectionWrapper. In this way, the code for loading the DW does not
       have to care about which parameter format is used.
       This ConnectionWrapper is a special one for JDBC in Jython and does DB 
       communication from a Thread.

       .. Note::
          BackgroundJDBCConnectionWrapper is added for experiments. 
          It is quite similar to JDBCConnectionWrapper and one of them may be 
          removed.
    """

    def __init__(self, jdbcconn, stmtcachesize=20):
        """Create a ConnectionWrapper around the given JDBC connection 

           Arguments:
           - jdbcconn: An open JDBC Connection (not a PEP249 Connection)
           - stmtcachesize: The maximum number of PreparedStatements kept
             open. Default: 20.
        """
        self.__jdbcconn = jdbcconn
        # Add a finalizer to __prepstmts to close PreparedStatements when
        # they are pushed out
        self.__prepstmts = FIFODict(stmtcachesize, lambda k, v: v[0].close())
        self.__resultmeta = FIFODict(stmtcachesize)
        self.__resultset = None
        self.__resultnames = None
        self.__resulttypes = None
        self.nametranslator = lambda s: s
        self.__jdbcconn.setAutoCommit(False)
        self.__queue = Queue(5000)
        t = Thread(target=self.__worker)
        t.setDaemon(True)  # NB: "t.daemon = True" does NOT work...
        t.setName('BackgroundJDBCConnectionWrapper')
        t.start()

    def __worker(self):
        while True:
            (sql, args) = self.__queue.get()
            self.__executejdbcstmt(sql, args)
            self.__queue.task_done()

    def __preparejdbcstmt(self, sql):
        # Find pyformat arguments and change them to question marks while
        # appending the attribute names to a list
        names = []
        newsql = sql
        while True:
            start = newsql.find('%(')
            if start == -1:
                break
            end = newsql.find(')s', start)
            if end == -1: 
                break
            name = newsql[start+2 : end]
            names.append(name)
            newsql = newsql.replace(newsql[start:end+2], '?', 1)

        ps = self.__jdbcconn.prepareStatement(newsql)

        # Find parameter types
        types = []
        parmeta = ps.getParameterMetaData()
        for i in range(len(names)):
            types.append(parmeta.getParameterType(i+1))

        self.__prepstmts[sql] = (ps, names, types)

    def __executejdbcstmt(self, sql, args):
        if self.__resultset:
            self.__resultset.close()

        if sql not in self.__prepstmts:
            self.__preparejdbcstmt(sql)
        (ps, names, types) = self.__prepstmts[sql]

        for pos in range(len(names)): # Not very Pythonic, but we're doing Java
            if args[names[pos]] is None:
                ps.setNull(pos + 1, types[pos])
            else:
                ps.setObject(pos + 1, args[names[pos]], types[pos])

        if ps.execute():
            self.__resultset = ps.getResultSet()
            if sql not in self.__resultmeta:
                self.__resultmeta[sql] = \
                    self.__extractresultmetadata(self.__resultset)
            (self.__resultnames, self.__resulttypes) = self.__resultmeta[sql]
        else:
            self.__resultset = None
            (self.__resultnames, self.__resulttypes) = (None, None)

    def __extractresultmetadata(self, resultset):
        # Get jdbc resultset metadata. extract names and types
        # and add it to self.__resultmeta
        meta = resultset.getMetaData()
        names = []
        types = []
        for col in range(meta.getColumnCount()):
            names.append(meta.getColumnName(col+1))
            types.append(meta.getColumnType(col+1))
        return (names, types)

    def __readresultrow(self):
        if self.__resultset is None:
            return None
        result = []
        for i in range(len(self.__resulttypes)):
            e = self.__resulttypes[i] # Not Pythonic, but we need i for JDBC
            if e in (jdbc.Types.CHAR, jdbc.Types.VARCHAR, 
                     jdbc.Types.LONGVARCHAR):
                result.append(self.__resultset.getString(i+1))
            elif e in (jdbc.Types.BIT, jdbc.Types.BOOLEAN):
                result.append(self.__resultset.getBool(i+1))
            elif e in (jdbc.Types.TINYINT, jdbc.Types.SMALLINT, 
                       jdbc.Types.INTEGER):
                result.append(self.__resultset.getInt(i+1))
            elif e in (jdbc.Types.BIGINT, ):
                result.append(self.__resultset.getLong(i+1))
            elif e in (jdbc.Types.DATE, ):
                result.append(self.__resultset.getDate(i+1))
            elif e in (jdbc.Types.TIMESTAMP, ):
                result.append(self.__resultset.getTimestamp(i+1))
            elif e in (jdbc.Types.TIME, ):
                result.append(self.__resultset.getTime(i+1))
            else:
                # Try this and hope for the best...
                result.append(self.__resultset.getString(i+1))
        return tuple(result)

    def execute(self, stmt, arguments=None, namemapping=None, ignored=None):
        """Execute a statement.

           Arguments:
           - stmt: the statement to execute
           - arguments: a mapping with the arguments. Default: None.
           - namemapping: a mapping of names such that if stmt uses %(arg)s
             and namemapping[arg]=arg2, the value arguments[arg2] is used 
             instead of arguments[arg]
           - ignored: An ignored argument only present to accept the same 
             number of arguments as ConnectionWrapper.execute
        """
        if namemapping and arguments:
            arguments = pygrametl.copy(arguments, **namemapping)
        else:
            arguments = pcopy(arguments)
        self.__queue.put((stmt, arguments))

    def executemany(self, stmt, params, ignored=None):
        """Execute a sequence of statements.

           Arguments:
           - stmt: the statement to execute
           - params: a sequence of arguments
           - ignored: An ignored argument only present to accept the same 
             number of arguments as ConnectionWrapper.executemany
        """
        for paramset in params:
            self.__queue.put((stmt, paramset))

    def rowfactory(self, names=None):
        """Return a generator object returning result rows (i.e. dicts)."""
        self.__queue.join()
        if names is None:
            if self.__resultnames is None:
                return
            else:
                names = [self.nametranslator(t[0]) for t in self.__resultnames]
        empty = (None, ) * len(self.__resultnames)
        while True:
            tuple = self.fetchonetuple()
            if tuple == empty:
                return
            yield dict(zip(names, tuple))

    def fetchone(self, names=None):
        """Return one result row (i.e. dict)."""
        self.__queue.join()
        if self.__resultset is None:
            return {}
        if names is None:
            names = [self.nametranslator(t[0]) for t in self.__resultnames]
        values = self.fetchonetuple()
        return dict(zip(names, values))

    def fetchonetuple(self):
        """Return one result tuple."""
        self.__queue.join()
        if self.__resultset is None:
            return ()
        if not self.__resultset.next():
            return (None, ) * len(self.__resultnames)
        else:
            return self.__readresultrow()

    def fetchmanytuples(self, cnt):
        """Return cnt result tuples."""
        self.__queue.join()
        if self.__resultset is None:
            return []
        empty = (None, ) * len(self.__resultnames)
        result = []
        for i in range(cnt):
            tmp = self.fetchonetuple()
            if tmp == empty:
                break
            result.append(tmp)
        return result

    def fetchalltuples(self):
        """Return all result tuples"""
        self.__queue.join()
        if self.__resultset is None:
            return []
        result = []
        empty = (None, ) * len(self.__resultnames)
        while True:
            tmp = self.fetchonetuple()
            if tmp == empty:
                return result
            result.append(tmp)

    def rowcount(self):
        """Not implemented. Return 0. Should return the size of the result."""
        return 0 

    def getunderlyingmodule(self):
        """Return a reference to the underlying connection's module."""
        return modules[self.__class__.__module__]

    def commit(self):
        """Commit the transaction."""
        pygrametl.endload()
        self.__queue.join()
        self.__jdbcconn.commit()

    def close(self):
        """Close the connection to the database,"""
        self.__queue.join()
        self.__jdbcconn.close()

    def rollback(self):
        """Rollback the transaction."""
        self.__queue.join()
        self.__jdbcconn.rollback()

    def setasdefault(self):
        """Set this ConnectionWrapper as the default connection."""
        pygrametl._defaulttargetconnection = self

    def cursor(self):
        """Not implemented for this JDBC connection wrapper!"""
        raise NotImplementedError, ".cursor() not supported"

    def resultnames(self):
        self.__queue.join()
        if self.__resultnames is None:
            return None
        else:
            return tuple(self.__resultnames)

def Date(year, month, day):
    date = '%s-%s-%s' % \
        (str(year).zfill(4), str(month).zfill(2), str(day).zfill(2)) 
    return jdbc.Date.valueOf(date)


def Timestamp(year, month, day, hour, minute, second):
    date = '%s-%s-%s %s:%s:%s' % \
        (str(year).zfill(4), str(month).zfill(2), str(day).zfill(2),
         str(hour).zfill(2), str(minute).zfill(2), str(second).zfill(2))
    return jdbc.Timestamp.valueOf(date)
