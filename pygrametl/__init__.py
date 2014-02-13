"""A package for creating Extract-Transform-Load (ETL) programs in Python.

   The package contains a number of classes for filling fact tables
   and dimensions (including snowflaked and slowly changing dimensions), 
   classes for extracting data from different sources, classes for defining
   'steps' in an ETL flow, and convenient functions for often-needed ETL
   functionality.

   The package's modules are:

   - datasources for access to different data sources
   - tables for giving easy and abstracted access to dimension and fact tables
   - parallel for parallelizing ETL operations
   - JDBCConnectionWrapper and jythonmultiprocessing for support of Jython
   - aggregators for aggregating data
   - steps for defining steps in an ETL flow
   - FIFODict for providing a dict with a limited size and where elements are 
     removed in first-in first-out order
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

import copy as pcopy
import exceptions
import types
from datetime import date, datetime
from Queue import Queue
from sys import modules
from threading import Thread

import FIFODict

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '2.2'
__all__ = ['project', 'copy', 'renamefromto', 'rename', 'renametofrom', 
           'getint', 'getlong', 'getfloat', 'getstr', 'getstrippedstr', 
           'getstrornullvalue', 'getbool', 'getdate', 'gettimestamp', 
           'getvalue', 'getvalueor', 'setdefaults', 'rowfactory', 'endload', 
           'today', 'now', 'ymdparser', 'ymdhmsparser', 'datereader', 
           'datetimereader', 'datespan', 'toupper', 'tolower', 'keepasis', 
           'getdefaulttargetconnection', 'ConnectionWrapper']


_alltables = []

def project(atts, row, renaming={}):
    """Create a new dictionary with a subset of the attributes.

       Arguments:
       - atts is a sequence of attributes in row that should be copied to the
         new result row.
       - row is the original dictionary to copy data from.
       - renaming is a mapping of names such that for each k in atts, 
         the following holds:

         - If k in renaming then result[k] = row[renaming[k]]. 
         - If k not in renaming then result[k] = row[k].
         - renaming defaults to {}
    """
    res = {}
    for c in atts:
        if c in renaming:
            res[c] = row[renaming[c]]
        else:
            res[c] = row[c]
    return res


def copy(row, **renaming):
    """Create a copy of a dictionary, but allow renamings.

       Arguments:
       - row the dictionary to copy
       - **renaming allows renamings to be specified in the form
         newname=oldname meaning that in the result, oldname will be
         renamed to newname. The key oldname must exist in the row argument, 
         but it can be assigned to several newnames in the result as in
         x='repeated', y='repeated'.
    """
    if not renaming:
        return row.copy()

    tmp = row.copy()
    res = {}
    for k,v in renaming.items():
        res[k] = row[v]
        if v in tmp: #needed for renamings like {'x':'repeated', 'y':'repeated'}
            del tmp[v]
    res.update(tmp)
    return res


def renamefromto(row, renaming):
    """Rename keys in a dictionary.

       For each (oldname, newname) in renaming.items(): rename row[oldname] to 
       row[newname].
    """
    if not renaming:
        return row

    for old, new in renaming.items():
        row[new] = row[old]
        del row[old]

rename = renamefromto # for backwards compatibility


def renametofrom(row, renaming):    
    """Rename keys in a dictionary.

       For each (newname, oldname) in renaming.items(): rename row[oldname] to 
       row[newname].
    """
    if not renaming:
        return row

    for new, old in renaming.items():
        row[new] = row[old]
        del row[old]

    
def getint(value, default=None):
    """getint(value[, default]) -> int(value) if possible, else default."""
    try:
        return int(value)
    except Exception:
        return default


def getlong(value, default=None):
    """getlong(value[, default]) -> long(value) if possible, else default."""
    try:
        return long(value)
    except Exception:
        return default

def getfloat(value, default=None):
    """getfloat(value[, default]) -> float(value) if possible, else default."""
    try:
        return float(value)
    except Exception:
        return default

def getstr(value, default=None):
    """getstr(value[, default]) -> str(value) if possible, else default."""
    try:
        return str(value)
    except Exception:
        return default

def getstrippedstr(value, default=None):
    """Convert given value to a string and use .strip() on the result.

       If the conversion fails, the given default value is returned.
    """
    try:
        s = str(value)
        return s.strip()
    except Exception:
        return default

def getstrornullvalue(value, nullvalue='None'):
    """Convert a given value different from None to a string.

       If the given value is None, nullvalue (default: 'None') is returned.
    """
    if value is None:
        return nullvalue
    else:
        return str(value)

def getbool(value, default=None, 
            truevalues=set(( True,  1, '1', 't', 'true',  'True' )), 
            falsevalues=set((False, 0, '0', 'f', 'false', 'False'))):
    """Convert a given value to True, False, or a default value.

       If the given value is in the given truevalues, True is returned.
       If the given value is in the given falsevalues, False is returned.
       Otherwise, the default value is returned.
    """
    if value in truevalues:
        return True
    elif value in falsevalues:
        return False
    else:
        return default



def getdate(targetconnection, ymdstr, default=None):
    """Convert a string of the form 'yyyy-MM-dd' to a Date object.

       The returned Date is in the given targetconnection's format.

       Arguments:
       - targetconnection: a ConnectionWrapper whose underlying module's
         Date format is used
       - ymdstr: the string to convert
       - default: The value to return if the conversion fails
    """
    try:
        (year, month, day) = ymdstr.split('-')
        modref = targetconnection.getunderlyingmodule()
        return modref.Date(int(year), int(month), int(day))
    except Exception:
        return default

def gettimestamp(targetconnection, ymdhmsstr, default=None):
    """Converts a string of the form 'yyyy-MM-dd HH:mm:ss' to a Timestamp.
    
       The returned Timestamp is in the given targetconnection's format.

       Arguments:
       - targetconnection: a ConnectionWrapper whose underlying module's
         Timestamp format is used
       - ymdhmsstr: the string to convert
       - default: The value to return if the conversion fails
    """
    try:
        (datepart, timepart) = ymdhmsstr.strip().split(' ')
        (year, month, day) = datepart.split('-')
        (hour, minute, second) = timepart.split(':')
        modref = targetconnection.getunderlyingmodule()
        return modref.Timestamp(int(year), int(month), int(day),\
                                int(hour), int(minute), int(second))
    except Exception:
        return default

def getvalue(row, name, mapping={}):
    """If name in mapping, return row[mapping[name]], else return row[name]."""
    if name in mapping:
        return row[mapping[name]]
    else:
        return row[name]

def getvalueor(row, name, mapping={}, default=None):
    """Return the value of name from row using a mapping and a default value."""
    if name in mapping:
        return row.get(mapping[name], default)
    else:
        return row.get(name, default)

def setdefaults(row, attributes, defaults=None):
    """Set default values for attributes not present in a dictionary.

       Default values are set for "missing" values, existing values are not 
       updated. 

       Arguments:
       - row is the dictionary to set default values in
       - attributes is either
           A) a sequence of attribute names in which case defaults must
              be an equally long sequence of these attributes default values or
           B) a sequence of pairs of the form (attribute, defaultvalue) in 
              which case the defaults argument should be None
       - defaults is a sequence of default values (see above)
    """
    if defaults and len(defaults) != len(attributes):
        raise ValueError, "Lists differ in length"

    if defaults:
        seqlist = zip(attributes, defaults)
    else:
        seqlist = attributes

    for att, defval in seqlist:
        if att not in row:
            row[att] = defval


def rowfactory(source, names, close=True):
    """Generate dicts with key values from names and data values from source.
    
       The given source should provide either next() or fetchone() returning
       a tuple or fetchall() returning a sequence of tuples. For each tuple,
       a dict is constructed such that the i'th element in names maps to 
       the i'th value in the tuple.

       If close=True (the default), close will be called on source after
       fetching all tuples.
    """
    nextfunc = getattr(source, 'next', None)
    if nextfunc is None:
        nextfunc = getattr(source, 'fetchone', None)

    try:
        if nextfunc is not None:
            try:
                tmp = nextfunc()
                if tmp is None:
                    return
                else:
                    yield dict(zip(names, tmp))
            except (StopIteration, IndexError):
                return
        else:
            for row in source.fetchall():
                yield dict(zip(names, row))
    finally:
        if close:
            try:
                source.close()
            except:
                return

def endload():
    """Signal to all Dimension and FactTable objects that all data is loaded."""
    global _alltables
    for t in _alltables:
        method = getattr(t, 'endload', None)
        if callable(method):
            method()

_today = None
def today(ignoredtargetconn=None, ignoredrow=None, ignorednamemapping=None):
    """Return the date of the first call this method as a datetime.date object.
    """
    global _today
    if _today is not None:
        return _today
    _today = date.today()
    return _today

_now = None
def now(ignoredtargetconn=None, ignoredrow=None, ignorednamemapping=None):
    """Return the time of the first call this method as a datetime.datetime.
    """
    global _now
    if _now is not None:
        return _now
    _now = datetime.now()
    return _now

def ymdparser(ymdstr):
    """Convert a string of the form 'yyyy-MM-dd' to a datetime.date. 

       If the input is None, the return value is also None.
    """
    if ymdstr is None:
        return None
    (year, month, day) = ymdstr.split('-')
    return date(int(year), int(month), int(day))

def ymdhmsparser(ymdhmsstr):
    """Convert a string 'yyyy-MM-dd HH:mm:ss' to a datetime.datetime. 

       If the input is None, the return value is also None.
    """
    if ymdhmsstr is None:
        return None
    (datepart, timepart) = ymdhmsstr.strip().split(' ')
    (year, month, day) = datepart.split('-')
    (hour, minute, second) = timepart.split(':')
    return datetime(int(year), int(month), int(day),\
                    int(hour), int(minute), int(second))


def datereader(dateattribute, parsingfunction=ymdparser):
    """Return a function that converts a certain dict member to a datetime.date

       When setting, fromfinder for a tables.SlowlyChangingDimension, this
       method can be used for generating a function that picks the relevant
       dictionary member from each row and converts it.

       Arguments:
       - dateattribute: the attribute the generated function should read
       - parsingfunction: the parsing function that converts the string
         to a datetime.date
    """
    def readerfunction(targetconnection, row, namemapping = {}):
        atttouse = (namemapping.get(dateattribute) or dateattribute)
        return parsingfunction(row[atttouse]) # a datetime.date
    
    return readerfunction
    

def datetimereader(datetimeattribute, parsingfunction=ymdhmsparser):
    """Return a function that converts a certain dict member to a datetime

       When setting, fromfinder for a tables.SlowlyChangingDimension, this
       method can be used for generating a function that picks the relevant
       dictionary member from each row and converts it.

       Arguments:
       - datetimeattribute: the attribute the generated function should read
       - parsingfunction: the parsing function that converts the string
         to a datetime.datetime
    """
    def readerfunction(targetconnection, row, namemapping = {}):
        atttouse = (namemapping.get(datetimeattribute) or datetimeattribute)
        return parsingfunction(row[atttouse]) # a datetime.datetime

    return readerfunction


def datespan(fromdate, todate, fromdateincl=True, todateincl=True,
             key='dateid', 
             strings={'date':'%Y-%m-%d', 'monthname':'%B', 'weekday':'%A'},
             ints={'year':'%Y', 'month':'%m', 'day':'%d'},
             expander=None):
    """Return a generator yielding dicts for all dates in an interval.

       Arguments:
       - fromdate: The lower bound for the date interval. Should be a
         datetime.date or a YYYY-MM-DD formatted string.
       - todate: The upper bound for the date interval. Should be a
         datetime.date or a YYYY-MM-DD formatted string.
       - fromdateincl: Decides if fromdate is included. Default: True
       - todateincl: Decides if todate is included. Default: True
       - key: The name of the attribute where an int (YYYYMMDD) that uniquely
         identifies the date is stored. Default: 'dateid'.
       - strings: A dict mapping attribute names to formatting directives (as
         those used by strftime). The returned dicts will have the specified
         attributes as strings.
         Default: {'date':'%Y-%m-%d', 'monthname':'%B', 'weekday':'%A'}
       - ints: A dict mapping attribute names to formatting directives (as
         those used by strftime). The returned dicts will have the specified
         attributes as ints.
         Default: {'year':'%Y', 'month':'%m', 'day':'%d'}
       - expander: A callable f(date, dict) that is invoked on each created
         dict. Not invoked if None. Default: None
    """

    for arg in (fromdate, todate):
        if not ((type(arg) in types.StringTypes and arg.count('-') == 2)\
                    or isinstance(arg, date)):
            raise ValueError, \
            "fromdate and today must be datetime.dates or " + \
            "YYYY-MM-DD formatted strings"

    (year, month, day) = fromdate.split('-')
    fromdate = date(int(year), int(month), int(day))

    (year, month, day) = todate.split('-')
    todate = date(int(year), int(month), int(day))

    start = fromdate.toordinal()
    if not fromdateincl:
        start += 1

    end = todate.toordinal()
    if todateincl:
        end += 1

    for i in xrange(start, end):
        d = date.fromordinal(i)
        res = {}
        res[key] = int(d.strftime('%Y%m%d'))
        for (att, format) in strings.iteritems():
            res[att] = d.strftime(format)
        for (att, format) in ints.iteritems():
            res[att] = int(d.strftime(format))
        if expander is not None:
            expander(d, res)
        yield res


toupper  = lambda s: s.upper()
tolower  = lambda s: s.lower()
keepasis = lambda s: s

_defaulttargetconnection = None

def getdefaulttargetconnection():
    """Return the default target connection"""
    global _defaulttargetconnection
    return _defaulttargetconnection

class ConnectionWrapper(object):
    """Provide a uniform representation of different database connection types.

       All Dimensions and FactTables communicate with the data warehouse using 
       a ConnectionWrapper. In this way, the code for loading the DW does not
       have to care about which parameter format is used.  
       
       pygrametl's code uses the 'pyformat' but the ConnectionWrapper performs
       translations of the SQL to use 'named', 'qmark', 'format', or 'numeric'
       if the user's database connection needs this. Note that the
       translations are simple and naive. Escaping as in %%(name)s is not
       taken into consideration. These simple translations are enough for
       pygrametl's code which is the important thing here; we're not trying to
       make a generic, all-purpose tool to get rid of the problems with
       different parameter formats. It is, however, possible to disable the
       translation of a statement to execute such that 'problematic'
       statements can be executed anyway.
    """

    def __init__(self, connection, stmtcachesize=1000, paramstyle=None):
        """Create a ConnectionWrapper around the given PEP 249 connection 

           If no default ConnectionWrapper already exists, the new 
           ConnectionWrapper is set as the default.

           Arguments:
           - connection: An open PEP 249 connection to the database
           - stmtcachesize: A number deciding how many translated statements to
             cache. A statement needs to be translated when the connection
             does not use 'pyformat' to specify parameters. When 'pyformat' is
             used, stmtcachesize is ignored as no statements need to be 
             translated. 
           - paramstyle: A string holding the name of the PEP 249 connection's
             paramstyle. If None, pygrametl will try to find the paramstyle
             automatically (an AttributeError can be raised if that fails).
        """
        self.__connection = connection
        self.__cursor = connection.cursor()
        self.nametranslator = lambda s: s

        if paramstyle is None:
            try:
                paramstyle = \
                    modules[self.__connection.__class__.__module__].paramstyle
            except AttributeError:
                # Note: This is probably a better way to do this, but to avoid
                # to break anything that worked before this fix, we only do it
                # this way if the first approach didn't work
                try:
                    paramstyle = \
                        modules[self.__connection.__class__.__module__.\
                                    split('.')[0]].paramstyle
                except AttributeError:
                    # To support, e.g., mysql.connector connections
                    paramstyle = \
                        modules[self.__connection.__class__.__module__.\
                                    rsplit('.', 1)[0]].paramstyle

        if not paramstyle == 'pyformat':
            self.__translations = FIFODict.FIFODict(stmtcachesize)
            try:
                self.__translate = getattr(self, '_translate2' + paramstyle)
            except AttributeError:
                raise InterfaceError, "The paramstyle '%s' is not supported" %\
                    paramstyle
        else:
            self.__translate = None

        global _defaulttargetconnection
        if _defaulttargetconnection is None:
            _defaulttargetconnection = self


    def execute(self, stmt, arguments=None, namemapping=None, translate=True):
        """Execute a statement.

           Arguments:
           - stmt: the statement to execute
           - arguments: a mapping with the arguments (default: None)
           - namemapping: a mapping of names such that if stmt uses %(arg)s
             and namemapping[arg]=arg2, the value arguments[arg2] is used 
             instead of arguments[arg]
           - translate: decides if translation from 'pyformat' to the
             undlying connection's format should take place. Default: True
        """
        if namemapping and arguments:
            arguments = copy(arguments, **namemapping)
        if self.__translate and translate:
            (stmt, arguments) = self.__translate(stmt, arguments)
        self.__cursor.execute(stmt, arguments)

    def executemany(self, stmt, params, translate=True):
        """Execute a sequence of statements."""
        if self.__translate and translate:
            # Idea: Translate the statement for the first parameter set. Then
            # reuse the statement (but create new attribute sequences if needed)
            # for the remaining paramter sets
            newstmt = self.__translate(stmt, params[0])[0]
            if type(self.__translations[stmt]) == str:
                # The paramstyle is 'named' in this case and we don't have to
                # put parameters into sequences
                self.__cursor.executemany(newstmt, params)
            else:
                # We need to extract attributes and put them into sequences
                names = self.__translations[stmt][1] # The attributes to extract
                newparams = [[p[n] for n in names] for p in params]
                self.__cursor.executemany(newstmt, newparams)
        else:
            # for pyformat when no translation is necessary
            self.__cursor.executemany(stmt, params)

    def _translate2named(self, stmt, row=None):
        # Translate %(name)s to :name. No need to change row.
        # Cache only the translated SQL.
        res = self.__translations.get(stmt, None)
        if res:
            return (res, row)
        res = stmt
        while True:
            start = res.find('%(')
            if start == -1: 
                break
            end = res.find(')s', start)
            if end == -1: 
                break
            name = res[start+2 : end]
            res = res.replace(res[start:end+2], ':' + name)
        self.__translations[stmt] = res
        return (res, row)

    def _translate2qmark(self, stmt, row=None):
        # Translate %(name)s to ? and build a list of attributes to extract
        # from row. Cache both.
        (newstmt, names) = self.__translations.get(stmt, (None, None))
        if newstmt:
            return (newstmt, [row[n] for n in names])
        names = []
        newstmt = stmt
        while True:
            start = newstmt.find('%(')
            if start == -1:
                break
            end = newstmt.find(')s', start)
            if end == -1: 
                break
            name = newstmt[start+2 : end]
            names.append(name)
            newstmt = newstmt.replace(newstmt[start:end+2], '?',1)#Replace once!
        self.__translations[stmt] = (newstmt, names)
        return (newstmt, [row[n] for n in names])

    def _translate2numeric(self, stmt, row=None):
        # Translate %(name)s to 1,2,... and build a list of attributes to
        # extract from row. Cache both.
        (newstmt, names) = self.__translations.get(stmt, (None, None))
        if newstmt:
            return (newstmt, [row[n] for n in names])
        names = []
        cnt = 0
        newstmt = stmt
        while True:
            start = newstmt.find('%(')
            if start == -1:
                break
            end = newstmt.find(')s', start)
            if end == -1: 
                break
            name = newstmt[start+2 : end]
            names.append(name)
            newstmt = newstmt.replace(newstmt[start:end+2], ':' + str(cnt))
            cnt += 1
        self.__translations[stmt] = (newstmt, names)
        return (newstmt, [row[n] for n in names])
        

    def _translate2format(self, stmt, row=None):
        # Translate %(name)s to %s and build a list of attributes to extract
        # from row. Cache both.
        (newstmt, names) = self.__translations.get(stmt, (None, None))
        if newstmt:
            return (newstmt, [row[n] for n in names])
        names = []
        newstmt = stmt
        while True:
            start = newstmt.find('%(')
            if start == -1:
                break
            end = newstmt.find(')s', start)
            if end == -1: 
                break
            name = newstmt[start+2 : end]
            names.append(name)
            newstmt = newstmt.replace(newstmt[start:end+2],'%s',1)#Replace once!
        self.__translations[stmt] = (newstmt, names)
        return (newstmt, [row[n] for n in names])


    def rowfactory(self, names=None):
        """Return a generator object returning result rows (i.e. dicts)."""
        rows = self.__cursor
        self.__cursor = self.__connection.cursor()
        if names is None:
            if rows.description is None: # no query was executed ...
                return (nothing for nothing in []) # a generator with no rows
            else:
                names = [self.nametranslator(t[0]) for t in rows.description]
        return rowfactory(rows, names, True)

    def fetchone(self, names=None):
        """Return one result row (i.e. dict)."""
        if self.__cursor.description is None:
            return {}
        if names is None:
            names = [self.nametranslator(t[0]) \
                         for t in self.__cursor.description]
        values = self.__cursor.fetchone()
        if values is None:
            return dict([(n, None) for n in names])#A row with each att = None
        else:
            return dict(zip(names, values))

    def fetchonetuple(self):
        """Return one result tuple."""
        if self.__cursor.description is None:
            return ()
        values = self.__cursor.fetchone()
        if values is None:
            return (None, ) * len(self.__cursor.description)
        else:
            return values

    def fetchmanytuples(self, cnt):
        """Return cnt result tuples."""
        if self.__cursor.description is None:
            return []
        return self.__cursor.fetchmany(cnt)

    def fetchalltuples(self):
        """Return all result tuples"""
        if self.__cursor.description is None:
            return []
        return self.__cursor.fetchall()

    def rowcount(self):
        """Return the size of the result."""
        return self.__cursor.rowcount

    def getunderlyingmodule(self):
        """Return a reference to the underlying connection's module."""
        return modules[self.__connection.__class__.__module__]

    def commit(self):
        """Commit the transaction."""
        endload()
        self.__connection.commit()

    def close(self):
        """Close the connection to the database,"""
        self.__connection.close()

    def rollback(self):
        """Rollback the transaction."""
        self.__connection.rollback()

    def setasdefault(self):
        """Set this ConnectionWrapper as the default connection."""
        global _defaulttargetconnection
        _defaulttargetconnection = self

    def cursor(self):
        """Return a cursor object. Optional method."""
        return self.__connection.cursor()

    def resultnames(self):
        if self.__cursor.description is None:
            return None
        else:
            return tuple([t[0] for t in self.__cursor.description])

    def __getstate__(self):
        # In case the ConnectionWrapper is pickled (to be sent to another
        # process), we need to create a new cursor when it is unpickled.
        res = self.__dict__.copy()
        del res['_ConnectionWrapper__cursor'] # a dirty trick, but...
        return res

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self.__cursor = self.__connection.cursor()


class BackgroundConnectionWrapper(object):
    """An alternative implementation of the ConnectionWrapper for experiments.
       This implementation communicates with the database by using a
       separate thread.

       It is likely better to use ConnectionWrapper og a shared 
       ConnectionWrapper (see pygrametl.parallel).

       This class offers the same methods as ConnectionWrapper. The 
       documentation is not repeated here.
    """
    _SINGLE = 1
    _MANY = 2

    # Most of this class' code was just copied from ConnectionWrapper
    # as we just want to do experiments with this class.

    def __init__(self, connection, stmtcachesize=1000, paramstyle=None):
        self.__connection = connection
        self.__cursor = connection.cursor()
        self.nametranslator = lambda s: s

        if paramstyle is None:
            try:
                paramstyle = \
                    modules[self.__connection.__class__.__module__].paramstyle
            except AttributeError:
                # Note: This is probably a better way to do this, but to avoid
                # to break anything that worked before this fix, we only do it
                # this way if the first approach didn't work
                try:
                    paramstyle = \
                        modules[self.__connection.__class__.__module__.\
                                    split('.')[0]].paramstyle
                except AttributeError:
                    # To support, e.g., mysql.connector connections
                    paramstyle = \
                        modules[self.__connection.__class__.__module__.\
                                    rsplit('.', 1)[0]].paramstyle

        if not paramstyle == 'pyformat':
            self.__translations = FIFODict.FIFODict(stmtcachesize)
            try:
                self.__translate = getattr(self, '_translate2' + paramstyle)
            except AttributeError:
                raise InterfaceError, "The paramstyle '%s' is not supported" %\
                    paramstyle
        else:
            self.__translate = None

        # Thread-stuff
        self.__cursor = connection.cursor()
        self.__queue = Queue(5000)
        t = Thread(target=self.__worker)
        t.daemon = True
        t.start()


    def execute(self, stmt, arguments=None, namemapping=None, translate=True):
        if namemapping and arguments:
            arguments = copy(arguments, **namemapping)
        if self.__translate and translate:
            (stmt, arguments) = self.__translate(stmt, arguments)
        self.__queue.put((self._SINGLE, self.__cursor, stmt, arguments)) 


    def executemany(self, stmt, params, translate=True):
        if self.__translate and translate:
            # Idea: Translate the statement for the first parameter set. Then
            # reuse the statement (but create new attribute sequences if needed)
            # for the remaining paramter sets
            newstmt = self.__translate(stmt, params[0])[0]
            if type(self.__translations[stmt]) == str:
                # The paramstyle is 'named' in this case and we don't have to
                # put parameters into sequences
                self.__queue.put((self._MANY, self.__cursor, newstmt, params))
            else:
                # We need to extract attributes and put them into sequences
                names = self.__translations[stmt][1] # The attributes to extract
                newparams = [[p[n] for n in names] for p in params]
                self.__queue.put((self._MANY,self.__cursor, newstmt, newparams))
        else:
            # for pyformat when no translation is necessary
            self.__queue.put((self._MANY, self.__cursor, stmt, params))

    def _translate2named(self, stmt, row=None):
        # Translate %(name)s to :name. No need to change row.
        # Cache only the translated SQL.
        res = self.__translations.get(stmt, None)
        if res:
            return (res, row)
        res = stmt
        while True:
            start = res.find('%(')
            if start == -1:
                break
            end = res.find(')s', start)
            name = res[start+2 : end]
            res = res.replace(res[start:end+2], ':' + name)
        self.__translations[stmt] = res
        return (res, row)

    def _translate2qmark(self, stmt, row=None):
        # Translate %(name)s to ? and build a list of attributes to extract
        # from row. Cache both.
        (newstmt, names) = self.__translations.get(stmt, (None, None))
        if newstmt:
            return (newstmt, [row[n] for n in names])
        names = []
        newstmt = stmt
        while True:
            start = newstmt.find('%(')
            if start == -1:
                break
            end = newstmt.find(')s', start)
            name = newstmt[start+2 : end]
            names.append(name)
            newstmt = newstmt.replace(newstmt[start:end+2], '?',1)#Replace once!
        self.__translations[stmt] = (newstmt, names)
        return (newstmt, [row[n] for n in names])

    def _translate2numeric(self, stmt, row=None):
        # Translate %(name)s to 1,2,... and build a list of attributes to
        # extract from row. Cache both.
        (newstmt, names) = self.__translations.get(stmt, (None, None))
        if newstmt:
            return (newstmt, [row[n] for n in names])
        names = []
        cnt = 0
        newstmt = stmt
        while True:
            start = newstmt.find('%(')
            if start == -1:
                break
            end = newstmt.find(')s', start)
            name = newstmt[start+2 : end]
            names.append(name)
            newstmt = newstmt.replace(newstmt[start:end+2], ':' + str(cnt))
            cnt += 1
        self.__translations[stmt] = (newstmt, names)
        return (newstmt, [row[n] for n in names])
        

    def _translate2format(self, stmt, row=None):
        # Translate %(name)s to %s and build a list of attributes to extract
        # from row. Cache both.
        (newstmt, names) = self.__translations.get(stmt, (None, None))
        if newstmt:
            return (newstmt, [row[n] for n in names])
        names = []
        newstmt = stmt
        while True:
            start = newstmt.find('%(')
            if start == -1:
                break
            end = newstmt.find(')s', start)
            name = newstmt[start+2 : end]
            names.append(name)
            newstmt = newstmt.replace(newstmt[start:end+2],'%s',1)#Replace once!
        self.__translations[stmt] = (newstmt, names)
        return (newstmt, [row[n] for n in names])


    def rowfactory(self, names=None):
        self.__queue.join()
        rows = self.__cursor
        self.__cursor = self.__connection.cursor()
        if names is None:
            if rows.description is None: # no query was executed ...
                return (nothing for nothing in []) # a generator with no rows
            else:
                names = [self.nametranslator(t[0]) for t in rows.description]
        return rowfactory(rows, names, True)

    def fetchone(self, names=None):
        self.__queue.join()
        if self.__cursor.description is None:
            return {}
        if names is None:
            names = [self.nametranslator(t[0]) \
                         for t in self.__cursor.description]
        values = self.__cursor.fetchone()
        if values is None:
            return dict([(n, None) for n in names])#A row with each att = None
        else:
            return dict(zip(names, values))

    def fetchonetuple(self):
        self.__queue.join()
        if self.__cursor.description is None:
            return ()
        values = self.__cursor.fetchone()
        if values is None:
            return (None, ) * len(self.__cursor.description)
        else:
            return values

    def fetchmanytuples(self, cnt):
        self.__queue.join()
        if self.__cursor.description is None:
            return []
        return self.__cursor.fetchmany(cnt)

    def fetchalltuples(self):
        self.__queue.join()
        if self.__cursor.description is None:
            return []
        return self.__cursor.fetchall()

    def rowcount(self):
        self.__queue.join()
        return self.__cursor.rowcount

    def getunderlyingmodule(self):
        # No need to join the queue here
        return modules[self.__connection.__class__.__module__]

    def commit(self):
        endload()
        self.__queue.join()
        self.__connection.commit()

    def close(self):
        self.__queue.join()
        self.__connection.close()

    def rollback(self):
        self.__queue.join()
        self.__connection.rollback()

    def setasdefault(self):
        global _defaulttargetconnection
        _defaulttargetconnection = self

    def cursor(self):
        self.__queue.join()
        return self.__connection.cursor()

    def resultnames(self):
        self.__queue.join()
        if self.__cursor.description is None:
            return None
        else:
            return tuple([t[0] for t in self.__cursor.description])

    def __getstate__(self):
        # In case the ConnectionWrapper is pickled (to be sent to another
        # process), we need to create a new cursor when it is unpickled.
        res = self.__dict__.copy()
        del res['_ConnectionWrapper__cursor'] # a dirty trick, but...

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self.__cursor = self.__connection.cursor()

    def __worker(self):
        while True:
            (op, curs, stmt, args) = self.__queue.get()
            if op == self._SINGLE:
                curs.execute(stmt, args)
            elif op == self._MANY:
                curs.executemany(stmt, args)
            self.__queue.task_done()


class Error(exceptions.StandardError):
    pass

class InterfaceError(Error):
    pass
