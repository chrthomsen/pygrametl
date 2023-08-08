"""This module contains classes for looking up rows, inserting rows
   and updating rows in dimensions and fact tables. Rows are represented
   as dictionaries mapping between attribute names and attribute values.

   Note that named arguments should be used when instantiating classes. This
   improves readability and guards against errors in case of future API changes.

   Many of the methods take an optional 'namemapping' argument which is
   explained here, but not repeated in the documentation for the individual
   methods: Consider a method m which is given a row r and a namemapping n.
   Assume that the method m uses the attribute a in r (i.e., r[a]). If the
   attribute a is not in the namemapping, m will just use r[a] as expected.
   But if the attribute a is in the namemapping, the name a is mapped to
   another name and the other name is used. That means that m then uses
   r[n[a]].  This is practical if attribute names in the considered rows and
   DW tables differ. If, for example, data is inserted into an order dimension
   in the DW that has the attribute order_date, but the source data uses the
   attribte name date, we can use a name mapping from order_date to date:
   dim.insert(row=..., namemapping={'order_date':'date'})
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

import locale
from operator import ge, gt, le, lt
from os import path
from subprocess import Popen, PIPE
from sys import version_info
import tempfile
from time import sleep
import types

import pygrametl
from pygrametl.FIFODict import FIFODict
import pygrametl.parallel


try:
    from functools import reduce
except ImportError:
    # Jython 2.5.X specific code
    pass

__all__ = ['definequote', 'Dimension', 'CachedDimension', 'BulkDimension',
           'CachedBulkDimension', 'TypeOneSlowlyChangingDimension',
           'SlowlyChangingDimension', 'SnowflakedDimension', 'FactTable',
           'BatchFactTable', 'BulkFactTable', 'AccumulatingSnapshotFactTable',
           'SubprocessFactTable', 'DecoupledDimension', 'DecoupledFactTable',
           'BasePartitioner', 'DimensionPartitioner', 'FactTablePartitioner']


def _quote(x): return x


def definequote(quotechar):
    """Defines the global quote function, for wrapping identifiers with quotes.


       Arguments:

       - quotechar: If None, do not wrap identifier. If a string, prepend and
         append quotechar to identifier. If a tuple of two strings, prepend with
         first element and append with last.
    """
    global _quote
    if quotechar is None:
        def _quote(x): return x
    elif isinstance(quotechar, str):
        def _quote(x): return quotechar + x + quotechar
    elif isinstance(quotechar, tuple) and len(quotechar) == 2:
        def _quote(x): return quotechar[0] + x + quotechar[1]
    else:
        raise AttributeError(
            "Expected either a string or a tuple of two strings")


class Dimension(object):

    """A class for accessing a dimension. Does no caching."""

    def __init__(self, name, key, attributes, lookupatts=(),
                 idfinder=None, defaultidvalue=None, rowexpander=None,
                 targetconnection=None):
        """Arguments:

           - name: the name of the dimension table in the DW
           - key: the name of the primary key in the DW
           - attributes: a sequence of the attribute names in the dimension
             table. Should not include the name of the primary key which is
             given in the key argument.
           - lookupatts: A subset of the attributes that uniquely identify
             a dimension members. These attributes are thus used for looking
             up members. If not given, it is assumed that
             lookupatts = attributes
           - idfinder: A function(row, namemapping) -> key value that assigns
             a value to the primary key attribute based on the content of the
             row and namemapping. If not given, it is assumed that the primary
             key is an integer, and the assigned key value is then the current
             maximum plus one.
           - defaultidvalue: An optional value to return when a lookup fails.
             This should thus be the ID for a preloaded "Unknown" member.
           - rowexpander: A function(row, namemapping) -> row. This function
             is called by ensure before insertion if a lookup of the row fails.
             This is practical if expensive calculations only have to be done
             for rows that are not already present. For example, for a date
             dimension where the full date is used for looking up rows, a
             rowexpander can be set such that week day, week number, season,
             year, etc. are only calculated for dates that are not already
             represented. If not given, no automatic expansion of rows is
             done.
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.
        """
        if not type(key) in pygrametl._stringtypes:
            raise ValueError("Key argument must be a string")
        if not len(attributes):
            raise ValueError("No attributes given")

        if targetconnection is None:
            targetconnection = pygrametl.getdefaulttargetconnection()
            if targetconnection is None:
                raise ValueError("No target connection available")
        self.targetconnection = targetconnection
        self.name = name
        self.attributes = attributes
        self.key = key
        self.all = [key, ]
        self.all.extend(attributes)
        if lookupatts == ():
            lookupatts = attributes
        elif not len(lookupatts):
            raise ValueError("Lookupatts contain no attributes")
        elif not set(lookupatts) <= set(self.all):
            raise ValueError("Lookupatts is not a subset of attributes")

        self.lookupatts = lookupatts
        self.defaultidvalue = defaultidvalue
        self.rowexpander = rowexpander
        self.quote = _quote
        self.quotelist = lambda x: [self.quote(xn) for xn in x]
        pygrametl._alltables.append(self)

        # Now create the SQL that we will need...

        # This gives "SELECT key FROM name WHERE lookupval1 = %(lookupval1)s
        #             AND lookupval2 = %(lookupval2)s AND ..."
        self.keylookupsql = "SELECT " + self.quote(key) + " FROM " + name + \
            " WHERE " + " AND ".join(["%s = %%(%s)s" % (self.quote(lv), lv)
                                      for lv in lookupatts])

        # This gives "SELECT key, att1, att2, ... FROM NAME WHERE key =
        # %(key)s"
        self.rowlookupsql = "SELECT " + ", ".join(self.quotelist(self.all))  \
            + " FROM %s WHERE %s = %%(%s)s" % (name, self.quote(key), key)

        # This gives "INSERT INTO name(key, att1, att2, ...)
        #             VALUES (%(key)s, %(att1)s, %(att2)s, ...)"
        self.insertsql = "INSERT INTO " + name + "(%s" % (self.quote(key),) + \
            (attributes and ", " or "") + \
            ", ".join(self.quotelist(attributes)) + ") VALUES (" + \
            ", ".join(["%%(%s)s" % (att,) for att in self.all]) + ")"

        if idfinder is not None:
            self.idfinder = idfinder
        else:
            self.targetconnection.execute("SELECT MAX(%s) FROM %s" %
                                          (self.quote(key), name))
            self.__maxid = self.targetconnection.fetchonetuple()[0]
            if self.__maxid is None:
                self.__maxid = 0
            self.idfinder = self._getnextid

    def lookup(self, row, namemapping={}):
        """Find the key for the row with the given values.

           Arguments:

           - row: a dict which must contain at least the lookup attributes
           - namemapping: an optional namemapping (see module's documentation)
        """
        key = self._before_lookup(row, namemapping)
        if key is not None:
            return key

        self.targetconnection.execute(self.keylookupsql, row, namemapping)

        keyvalue = self.targetconnection.fetchonetuple()[0]
        if keyvalue is None:
            keyvalue = self.defaultidvalue  # most likely also None...

        self._after_lookup(row, namemapping, keyvalue)
        return keyvalue

    def _before_lookup(self, row, namemapping):
        return None

    def _after_lookup(self, row, namemapping, resultkeyvalue):
        pass

    def getbykey(self, keyvalue):
        """Lookup and return the row with the given key value.

           If no row is found in the dimension table, the function returns
           a row where all values (including the key) are None.
        """
        if isinstance(keyvalue, dict):
            keyvalue = keyvalue[self.key]

        row = self._before_getbykey(keyvalue)
        if row is not None:
            return row
        self.targetconnection.execute(self.rowlookupsql, {self.key: keyvalue})
        row = self.targetconnection.fetchone(self.all)
        self._after_getbykey(keyvalue, row)
        return row

    def _before_getbykey(self, keyvalue):
        return None

    def _after_getbykey(self, keyvalue, resultrow):
        pass

    def getbyvals(self, values, namemapping={}):
        """Return a list of all rows with values identical to the given.

           Arguments:

           - values: a dict which must hold a subset of the tables attributes.
             All rows that have identical values for all attributes in this
             dict are returned.
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self._before_getbyvals(values, namemapping)
        if res is not None:
            return res

        # select all attributes from the table. The attributes available from
        # the values dict are used in the WHERE clause.
        attstouse = [a for a in self.attributes
                     if a in values or a in namemapping]
        sql = "SELECT " + ", ".join(self.quotelist(self.all)) + " FROM " +\
            self.name + " WHERE " + \
            " AND ".join(["%s = %%(%s)s" % (self.quote(att), att)
                          for att in attstouse])

        self.targetconnection.execute(sql, values, namemapping)

        res = [r for r in self.targetconnection.rowfactory(self.all)]
        self._after_getbyvals(values, namemapping, res)
        return res

    def _before_getbyvals(self, values, namemapping):
        return None

    def _after_getbyvals(self, values, namemapping, resultrows):
        pass

    def update(self, row, namemapping={}):
        """Update a single row in the dimension table.

           Arguments:

           - row: a dict which must contain the key for the dimension.
             The row with this key value is updated such that it takes
             the value of row[att] for each attribute att which is also in
             row.
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self._before_update(row, namemapping)
        if res:
            return

        if self.key not in row:
            raise KeyError("The key value (%s) is missing in the row" %
                           (self.key,))

        attstouse = [a for a in self.attributes
                     if a in row or a in namemapping]
        if not attstouse:
            # Only the key was there - there are no attributes to update
            return

        sql = "UPDATE " + self.name + " SET " + \
            ", ".join(["%s = %%(%s)s" % (self.quote(att), att) for att
                       in attstouse]) + \
            " WHERE %s = %%(%s)s" % (self.quote(self.key), self.key)
        self.targetconnection.execute(sql, row, namemapping)
        self._after_update(row, namemapping)

    def _before_update(self, row, namemapping):
        return None

    def _after_update(self, row, namemapping):
        pass

    def ensure(self, row, namemapping={}):
        """Lookup the given row. If that fails, insert it. Return the key value.

           If the lookup fails and a rowexpander was set when creating the
           instance, this rowexpander is called before the insert takes place.

           Arguments:

           - row: the row to lookup or insert. Must contain the lookup
             attributes. Key is not required to be present but will be added
             using idfinder if missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self.lookup(row, namemapping)
        if res is not None and res != self.defaultidvalue:
            return res
        else:
            if self.rowexpander:
                row = self.rowexpander(row, namemapping)
                if not isinstance(row, dict):
                    raise TypeError("The rowexpander %s did not return a row"
                                    % self.rowexpander.__qualname__)
            return self.insert(row, namemapping)

    def insert(self, row, namemapping={}):
        """Insert the given row. Return the new key value.

           Arguments:

           - row: the row to insert. The dict is not updated. It must contain
             all attributes, and is allowed to contain more attributes than
             that. Key is not required to be present but will be added using
             idfinder if missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self._before_insert(row, namemapping)
        if res is not None:
            return res

        key = (namemapping.get(self.key) or self.key)
        if row.get(key) is None:
            keyval = self.idfinder(row, namemapping)
            row = dict(row)  # Make a copy to change
            row[key] = keyval
        else:
            keyval = row[key]

        self.targetconnection.execute(self.insertsql, row, namemapping)
        self._after_insert(row, namemapping, keyval)
        return keyval

    def _before_insert(self, row, namemapping):
        return None

    def _after_insert(self, row, namemapping, newkeyvalue):
        pass

    def _getnextid(self, ignoredrow, ignoredmapping):
        self.__maxid += 1
        return self.__maxid

    def endload(self):
        """Finalize the load."""
        pass


class CachedDimension(Dimension):

    """A class for accessing a dimension. Does caching.

       We assume that the DB doesn't change or add any attribute
       values that are cached.
       For example, a DEFAULT value in the DB or automatic type coercion can
       break this assumption.
    """

    def __init__(self, name, key, attributes, lookupatts=(),
                 idfinder=None, defaultidvalue=None, rowexpander=None,
                 size=10000, prefill=False, cachefullrows=False,
                 cacheoninsert=True, usefetchfirst=False,
                 targetconnection=None):
        """Arguments:

           - name: the name of the dimension table in the DW
           - key: the name of the primary key in the DW
           - attributes: a sequence of the attribute names in the dimension
             table. Should not include the name of the primary key which is
             given in the key argument.
           - lookupatts: A subset of the attributes that uniquely identify
             a dimension members. These attributes are thus used for looking
             up members. If not given, it is assumed that
             lookupatts = attributes
           - idfinder: A function(row, namemapping) -> key value that assigns
             a value to the primary key attribute based on the content of the
             row and namemapping. If not given, it is assumed that the primary
             key is an integer, and the assigned key value is then the current
             maximum plus one.
           - defaultidvalue: An optional value to return when a lookup fails.
             This should thus be the ID for a preloaded "Unknown" member.
           - rowexpander: A function(row, namemapping) -> row. This function
             is called by ensure before insertion if a lookup of the row fails.
             This is practical if expensive calculations only have to be done
             for rows that are not already present. For example, for a date
             dimension where the full date is used for looking up rows, a
             rowexpander can be set such that week day, week number, season,
             year, etc. are only calculated for dates that are not already
             represented. If not given, no automatic expansion of rows is
             done.
           - size: the maximum number of rows to cache. If less than or equal
             to 0, unlimited caching is used. Default: 10000
           - prefill: a flag deciding if the cache should be filled when
             initialized. Default: False
           - cachefullrows: a flag deciding if full rows should be
             cached. If not, the cache only holds a mapping from
             lookupattributes to key values. Default: False.
           - cacheoninsert: a flag deciding if the cache should be updated
             when insertions are done. Default: True
           - usefetchfirst: a flag deciding if the SQL:2008 FETCH FIRST
             clause is used when prefil is True. Depending on the used DBMS
             and DB driver, this can give significant savings wrt. to time and
             memory. Not all DBMSs support this clause yet. Default: False
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.
        """
        Dimension.__init__(self,
                           name=name,
                           key=key,
                           attributes=attributes,
                           lookupatts=lookupatts,
                           idfinder=idfinder,
                           defaultidvalue=defaultidvalue,
                           rowexpander=rowexpander,
                           targetconnection=targetconnection)

        self.cacheoninsert = cacheoninsert
        self.__prefill = prefill
        self.__size = size
        if size > 0:
            if cachefullrows:
                self.__key2row = FIFODict(size)
            self.__vals2key = FIFODict(size)
        else:
            # Use dictionaries as unlimited caches
            if cachefullrows:
                self.__key2row = {}
            self.__vals2key = {}

        self.cachefullrows = cachefullrows

        if prefill:
            if cachefullrows:
                positions = tuple([self.all.index(att)
                                   for att in self.lookupatts])
                # select the key and all attributes
                sql = "SELECT %s FROM %s" % (
                    ", ".join(self.quotelist(self.all)), name)
            else:
                # select the key and the lookup attributes
                sql = "SELECT %s FROM %s" % \
                    (", ".join(
                        self.quotelist([key] + [l for l in self.lookupatts])),
                     name)
                positions = range(1, len(self.lookupatts) + 1)
            if size > 0 and usefetchfirst:
                sql += " FETCH FIRST %d ROWS ONLY" % size

            self.targetconnection.execute(sql)

            if size <= 0:
                data = self.targetconnection.fetchalltuples()
            else:
                data = self.targetconnection.fetchmanytuples(size)

            for rawrow in data:
                if cachefullrows:
                    self.__key2row[rawrow[0]] = rawrow
                t = tuple([rawrow[i] for i in positions])
                self.__vals2key[t] = rawrow[0]

    def lookup(self, row, namemapping={}):
        if self.__prefill and self.cacheoninsert and \
                (self.__size <= 0 or len(self.__vals2key) < self.__size):
            # Everything is cached. We don't have to look in the DB
            res = self._before_lookup(row, namemapping)
            if res is not None:
                return res
            else:
                return self.defaultidvalue
        else:
            # Something is not cached so we have to use the classical lookup.
            # (We may still benefit from the cache due to a call of
            # _before_lookup)
            return Dimension.lookup(self, row, namemapping)

    def _before_lookup(self, row, namemapping):
        namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
        searchtuple = tuple([row[n] for n in namesinrow])
        return self.__vals2key.get(searchtuple, None)

    def _after_lookup(self, row, namemapping, resultkey):
        if resultkey is not None and resultkey != self.defaultidvalue:
            namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
            searchtuple = tuple([row[n] for n in namesinrow])
            self.__vals2key[searchtuple] = resultkey

    def _before_getbykey(self, keyvalue):
        if self.cachefullrows:
            res = self.__key2row.get(keyvalue)
            if res is not None:
                return dict(zip(self.all, res))
        return None

    def _after_getbykey(self, keyvalue, resultrow):
        if self.cachefullrows and resultrow[self.key] is not None:
            # if resultrow[self.key] is None, no result was found in the db
            self.__key2row[keyvalue] = tuple([resultrow[a] for a in self.all])

    def _before_update(self, row, namemapping):
        """ """
        # We have to remove old values from the caches.
        key = (namemapping.get(self.key) or self.key)
        for att in self.lookupatts:
            if (att in namemapping and namemapping[att] in row) or att in row:
                # A lookup attribute is about to be changed and we should make
                # sure that the cache does not map from the old value.  Here,
                # we can only see the new value, but we can get the old lookup
                # values by means of the key:
                oldrow = self.getbykey(row[key])
                searchtuple = tuple([oldrow[n] for n in self.lookupatts])
                if searchtuple in self.__vals2key:
                    del self.__vals2key[searchtuple]
                break

        if self.cachefullrows:
            if row[key] in self.__key2row:
                # The cached row is now incorrect. We must make sure it is
                # not in the cache.
                del self.__key2row[row[key]]

        return None

    def _after_update(self, row, namemapping):
        """ """
        if self.__prefill and self.cacheoninsert and \
                (self.__size <= 0 or len(self.__vals2key) < self.__size):
            # Everything is cached and we sometimes avoid looking in the DB.
            # Therefore, we have to update the cache now. In _before_update,
            # we deleted the cached data.
            keyval = row[(namemapping.get(self.key) or self.key)]
            newrow = self.getbykey(keyval)  # This also updates __key2row
            self._after_lookup(newrow, {}, keyval)  # Updates __vals2key

    def _after_insert(self, row, namemapping, newkeyvalue):
        """ """
        # After the insert, we can look the row up. Pretend that we
        # did that. Then we get the new data cached.
        # NB: Here we assume that the DB doesn't change or add anything.
        # For example, a DEFAULT value in the DB or automatic type coercion can
        # break this assumption.
        if self.cacheoninsert:
            self._after_lookup(row, namemapping, newkeyvalue)
            if self.cachefullrows:
                tmp = pygrametl.project(self.all, row, namemapping)
                tmp[self.key] = newkeyvalue
                self._after_getbykey(newkeyvalue, tmp)


class TypeOneSlowlyChangingDimension(CachedDimension):

    """A class for accessing a slowly changing dimension of "type 1".

       Caching is used. We assume that the DB doesn't change or add any
       attribute values that are cached.
       For example, a DEFAULT value in the DB or automatic type coercion can
       break this assumption.
    """

    def __init__(self, name, key, attributes, lookupatts, type1atts=(),
                 cachesize=10000, prefill=False, idfinder=None,
                 usefetchfirst=False, cachefullrows=False,
                 targetconnection=None):
        """Arguments:

           - name: the name of the dimension table in the DW
           - key: the name of the primary key in the DW
           - attributes: a sequence of the attribute names in the dimension
             table. Should not include the name of the primary key which is
             given in the key argument.
           - lookupatts: A subset of the attributes that uniquely identify
             a dimension members. These attributes are thus used for looking
             up members.
           - type1atts: A sequence of attributes that should have type1 updates
             applied, it cannot intersect with lookupatts. If not given, it is
             assumed that type1atts = attributes - lookupatts
           - cachesize: the maximum number of rows to cache. If less than or
             equal to 0, unlimited caching is used. Default: 10000
           - prefill: a flag deciding if the cache should be filled when
             initialized. Default: False
           - idfinder: A function(row, namemapping) -> key value that assigns
             a value to the primary key attribute based on the content of the
             row and namemapping. If not given, it is assumed that the primary
             key is an integer, and the assigned key value is then the current
             maximum plus one.
           - usefetchfirst: a flag deciding if the SQL:2008 FETCH FIRST
             clause is used when prefil is True. Depending on the used DBMS
             and DB driver, this can give significant savings wrt. to time and
             memory. Not all DBMSs support this clause yet. Default: False
           - cachefullrows: a flag deciding if full rows should be cached. If
             not, the cache only holds a mapping from lookupattributes to key
             values, and from key to the type 1 slowly changing attributes.
             Default: False.
           - targetconnection: The ConnectionWrapper to use. If not given, the
             default target connection is used.
        """
        CachedDimension.__init__(self,
                                 name=name,
                                 key=key,
                                 attributes=attributes,
                                 lookupatts=lookupatts,
                                 idfinder=idfinder,
                                 defaultidvalue=None,
                                 rowexpander=None,
                                 size=cachesize,
                                 prefill=prefill,
                                 cachefullrows=cachefullrows,
                                 cacheoninsert=True,
                                 usefetchfirst=usefetchfirst,
                                 targetconnection=targetconnection)

        if type1atts == ():
            type1atts = list(set(attributes) - set(lookupatts))
        elif not set(type1atts) < set(attributes):
            raise ValueError("Type1atts is not a subset of attributes")
        elif set(lookupatts) & set(type1atts):
            raise ValueError("Intersection between lookupatts and type1atts")

        # Ensures "lookupatts != attributes" as it prevents type 1 updates
        if not len(type1atts):
            raise ValueError("Type1atts contain no attributes")
        self.type1atts = type1atts

        # If entire rows are cached then we do not need a cache for type1atts
        if not cachefullrows:
            if cachesize > 0:
                self.__key2sca = FIFODict(cachesize)
            else:
                self.__key2sca = {}
            if prefill:
                sql = "SELECT %s FROM %s" % \
                    (", ".join(self.quotelist([key] + self.type1atts)), name)
                if cachesize > 0 and usefetchfirst:
                    sql += " FETCH FIRST %d ROWS ONLY" % cachesize
                self.targetconnection.execute(sql)

                if cachesize <= 0:
                    data = self.targetconnection.fetchalltuples()
                else:
                    data = self.targetconnection.fetchmanytuples(cachesize)

                for row in data:
                    self.__key2sca[row[0]] = row[1:]

    def scdensure(self, row, namemapping={}):
        """Lookup or insert a version of a slowly changing dimension member.

           .. Note:: Has side-effects on the given row.

           Arguments:

           - row: a dict containing the attributes for the table. It must
             contain all attributes if it is the first version of the row to be
             inserted, updates of existing rows need only contain lookupatts
             and a subset of type1atts as a missing type1atts is ignored and
             the existing value left as is in the database. Key is not required
             to be present but will be added using idfinder if missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        # NOTE: the "vals2key" cache is kept coherent by "scdensure", as it
        # only contains "lookupatts" which "scdensure" is prohibited from
        # changing

        keyval = self.lookup(row, namemapping)
        key = (namemapping.get(self.key) or self.key)
        if keyval is None:
            # The first version of the row is inserted
            keyval = self.insert(row, namemapping)
            row[key] = keyval
        else:
            # The row did exist so we update the type1atts provided
            row[key] = keyval

            # Checks if the new row contains any type1atts
            type1atts = set()
            for att in self.type1atts:
                if (namemapping.get(att) or att) in row:
                    type1atts.add(att)
            if not type1atts:
                return row[key]

            # Checks if any of the type1atts in the row are different
            if not self.cachefullrows and keyval in self.__key2sca:
                oldrow = dict(zip(self.type1atts, self.__key2sca[keyval]))
            else:
                oldrow = self.getbykey(keyval)

            tmptype1atts = set()
            for att in type1atts:
                rowatt = row[(namemapping.get(att) or att)]
                if oldrow[att] != rowatt:
                    tmptype1atts.add(att)
                    oldrow[att] = rowatt  # Saved for updating the cache
            type1atts = tmptype1atts
            if not type1atts:
                return row[key]

            # Updates only the type1atts that were changed in the row, the
            # update method is not used as lookupatts can never change
            updatesql = "UPDATE " + self.name + " SET " + \
                        ", ".join(["%s = %%(%s)s" %
                                   (self.quote(att), att)
                                   for att in type1atts]) + \
                " WHERE %s = %%(%s)s" % (self.quote(key), key)

            self.targetconnection.execute(updatesql, row, namemapping)

            # Updates the caches that could be invalidated by scdensure
            if self.cachefullrows:
                self._after_getbykey(keyval, oldrow)
            else:
                self.__key2sca[keyval] = tuple([oldrow.get(att) for att in
                                                self.type1atts])
        return row[key]

    def _after_getbykey(self, keyvalue, resultrow):
        if self.cachefullrows:
            CachedDimension._after_getbykey(self, keyvalue, resultrow)
        elif resultrow[self.key] is not None:
            self.__key2sca[keyvalue] = \
                tuple([resultrow[a] for a in self.type1atts])

    def _after_update(self, row, namemapping):
        CachedDimension._after_update(self, row, namemapping)

        # Checks if changes were performed to the locally cached type1atts
        keyvalue = row[(namemapping.get(self.key) or self.key)]
        if self.cachefullrows or keyvalue not in self.__key2sca:
            return

        # A cached value could have been changed so the local cache is updated,
        # as the value is changed in place no rows are moved in the cache.
        oldtype1vals = dict(zip(self.type1atts, self.__key2sca[keyvalue]))
        self.__key2sca[keyvalue] = tuple([row.get(namemapping.get(a, a)) or
                                          oldtype1vals[a] for a in
                                          self.type1atts])

    def _after_insert(self, row, namemapping, newkeyvalue):
        CachedDimension._after_insert(self, row, namemapping, newkeyvalue)
        # NB: Here we assume that the DB doesn't change or add anything. For
        # example, a DEFAULT value in the DB or automatic type coercion can
        # break this assumption.
        if not self.cachefullrows:
            tmp = pygrametl.project(self.type1atts, row, namemapping)
            self.__key2sca[newkeyvalue] = \
                tuple([tmp[a] for a in self.type1atts])


class SlowlyChangingDimension(Dimension):

    """A class for accessing a slowly changing dimension of "type 2".

       "Type 1" updates can also be applied for a subset of the attributes.

       Caching is used. We assume that the DB doesn't change or add any
       attribute values that are cached.
       For example, a DEFAULT value in the DB or automatic type coercion can
       break this assumption.
    """

    def __init__(self, name, key, attributes, lookupatts, orderingatt=None,
                 versionatt=None,
                 fromatt=None, fromfinder=None,
                 toatt=None, tofinder=None, minfrom=None, maxto=None,
                 srcdateatt=None, srcdateparser=pygrametl.ymdparser,
                 type1atts=(), cachesize=10000, prefill=False, idfinder=None,
                 usefetchfirst=False, useorderby=True, targetconnection=None):
        """Arguments:

           - name: the name of the dimension table in the DW
           - key: the name of the primary key in the DW
           - attributes: a sequence of the attribute names in the dimension
             table. Should not include the name of the primary key which is
             given in the key argument, but should include versionatt,
             fromatt, and toatt.
           - lookupatts: a sequence with a subset of the attributes that
             uniquely identify a dimension members. These attributes are thus
             used for looking up members.
           - orderingatt: the name of the attribute used to identify the
             newest version. The version holding the greatest value is
             considered to be the newest. If orderingatt is None, versionatt
             is used. If versionatt is also None, toatt is used and NULL is
             considered as the greatest value. If toatt is also None, fromatt
             is used and NULL is considered as the smallest values. If
             orderingatt, versionatt, toatt, and fromatt are all None, an
             error is raised.
           - versionatt: the name of the attribute holding the version number
           - fromatt: the name of the attribute telling from when the version
             becomes valid. Not used if None. Default: None
           - fromfinder: a function(targetconnection, row, namemapping)
             returning a value for the fromatt for a new version (the function
             is first used when it is determined that a new version must be
             added; it is not applied to determine this).
             If fromfinder is None and srcdateatt is also None,
             pygrametl.today is used as fromfinder. If fromfinder is None
             and srcdateatt is not None,
             pygrametl.datereader(srcdateatt, srcdateparser) is used.
             In other words, if no date attribute and no special
             date function are given, new versions get the date of the current
             day. If a date attribute is given (but no date function), the
             date attribute's value is converted (by means of srcdateparser)
             and a new version gets the result of this as the date it is valid
             from. Default: None
           - toatt: the name of the attribute telling until when the version
             is valid. Not used if None. Default: None
           - tofinder: a function(targetconnection, row, namemapping)
             returning a value for the toatt. If not set, fromfinder is used
             (note that if fromfinder is None, it is set to a default
             function -- see the comments about fromfinder. The possibly
             modified value is used here.) Default: None
           - minfrom: the value to use for fromatt for the 1st version of a
             member if fromatt is not already set. If None, the value is
             found in the same way as for other new versions, i.e., as
             described for fromfinder. If fromatt should take the value
             NULL for the 1st version, set minfrom to a tuple holding a single
             element which is None: (None,). Note that minto affects the 1st
             version, not any following versions. Note also that if the member
             to insert already contains a value for fromatt, minfrom is
             ignored.
             Default: None.
           - maxto: the value to use for toatt for new members. Default: None
           - srcdateatt: the name of the attribute in the source data that
             holds a date showing when a version is valid from. The data is
             converted to a datetime by applying srcdateparser on it.
             If not None, the date attribute is also used when comparing
             a potential new version to the newest version in the DB.
             If None, the date fields are not compared. Default: None
           - srcdateparser: a function that takes one argument (a date in the
             format scrdateatt has) and returns a datetime.datetime.
             If srcdateatt is None, srcdateparser is not used.
             Default: pygrametl.ymdparser (i.e., the default value is a
             function that parses a string of the form 'yyyy-MM-dd')
           - type1atts: a sequence of attributes that should have type1 updates
             applied. Default: ()
           - cachesize: the maximum size of the cache. 0 disables caching
             and values smaller than 0 allows unlimited caching
           - prefill: decides if the cache should be prefilled with the newest
             versions. Default: False.
           - idfinder: a function(row, namemapping) -> key value that assigns
             a value to the primary key attribute based on the content of the
             row and namemapping. If not given, it is assumed that the primary
             key is an integer, and the assigned key value is then the current
             maximum plus one.
           - usefetchfirst: a flag deciding if the SQL:2008 FETCH FIRST
             clause is used when prefil is True. Depending on the used DBMS
             and DB driver, this can give significant savings wrt. to time and
             memory. Not all DBMSs support this clause yet. Default: False
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.
           - useorderby: a flag deciding if ORDER BY is used in the SQL to
             select the newest version. If True, the DBMS thus does the
             sorting. If False, all versions are fetched and the highest
             version is found in Python. For some systems, this can lead to
             significant performance improvements. Default: True

        """
        # TODO: Should scdensure just override ensure instead of being a new
        #       method?

        Dimension.__init__(self,
                           name=name,
                           key=key,
                           attributes=attributes,
                           lookupatts=lookupatts,
                           idfinder=idfinder,
                           defaultidvalue=None,
                           rowexpander=None,
                           targetconnection=targetconnection)

        if orderingatt is not None:
            self.orderingatt = orderingatt
        elif versionatt is not None:
            self.orderingatt = versionatt
        elif toatt is not None:
            self.orderingatt = toatt
        elif fromatt is not None:
            self.orderingatt = fromatt
        else:
            raise ValueError("If orderingatt is None, either versionatt, " +
                             "toatt, or fromatt must not be None")

        self.versionatt = versionatt
        self.fromatt = fromatt
        if fromfinder is not None:
            self.fromfinder = fromfinder
        elif srcdateatt is not None:  # and fromfinder is None
            self.fromfinder = pygrametl.datereader(srcdateatt, srcdateparser)
        else:  # fromfinder is None and srcdateatt is None
            self.fromfinder = pygrametl.today
        self.toatt = toatt
        if tofinder is None:
            tofinder = self.fromfinder
        self.tofinder = tofinder
        self.minfrom = minfrom
        self.maxto = maxto
        self.srcdateatt = srcdateatt
        self.srcdateparser = srcdateparser
        self.type1atts = type1atts
        self.useorderby = useorderby
        if cachesize > 0:
            self.rowcache = FIFODict(cachesize)
            self.keycache = FIFODict(cachesize)
        elif cachesize < 0:
            self.rowcache = {}
            self.keycache = {}
        # else cachesize == 0 and we do not create any caches
        self.__cachesize = cachesize
        self.__prefill = cachesize and prefill  # no prefilling if no caching

        # Check that versionatt, fromatt and toatt are also declared as
        # attributes
        for var in (orderingatt, versionatt, fromatt, toatt):
            if var and var not in attributes:
                raise ValueError("%s not present in attributes argument" %
                                 (var,))

        # Now extend the SQL from Dimension such that we use the versioning
        self.keylookupsql += " ORDER BY %s DESC" % \
                             (self.quote(self.orderingatt),)
        # There could be NULLs in toatt and fromatt
        if self.orderingatt == self.toatt:
            self.keylookupsql += " NULLS FIRST" 
        elif self.orderingatt == self.fromatt:
            self.keylookupsql += " NULLS LAST"

        # Now create SQL for looking up the key with a local sort
        # This gives "SELECT key, version FROM name WHERE
        # lookupval1 = %(lookupval1)s AND lookupval2 = %(lookupval2)s AND ..."
        self.keyversionlookupsql = "SELECT " + self.quote(key) + ", " + \
            self.quote(self.orderingatt) + " FROM " + name + \
            " WHERE " + " AND ".join(["%s = %%(%s)s" % (self.quote(lv), lv)
                                      for lv in lookupatts])


        if toatt:
            self.updatetodatesql = \
                "UPDATE %s SET %s = %%(%s)s WHERE %s = %%(%s)s" % \
                (name, self.quote(toatt), toatt, self.quote(key), key)

        if self.toatt or self.fromatt:
            # Set up SQL and functions for lookupasof
            keyvalidityatts = [self.key]
            if self.toatt:
                keyvalidityatts.append(self.toatt)
            if self.fromatt:
                keyvalidityatts.append(self.fromatt)
            # This gives "SELECT key, {fromatt and/or toatt} FROM name
            #             WHERE lookupval1 = %(lookupval1) AND
            #             lookupval2 = %(lookupval2)s AND ..."
            #             ORDER BY orderingatt ASC NULLS {LAST or FIRST}"
            self.keyvaliditylookupsql = "SELECT " +  \
                ", ".join(self.quotelist(keyvalidityatts)) + " FROM " + name +\
                " WHERE " + " AND ".join(["%s = %%(%s)s" % (self.quote(lv), lv)
                                      for lv in lookupatts]) +\
                " ORDER BY %s ASC" % (self.quote(self.orderingatt),)
            # There could be NULLs in toatt and fromatt. See the explanation for
            # the orderingatt argument above
            if self.orderingatt == self.toatt:
                self.keyvaliditylookupsql += " NULLS LAST" 
            elif self.orderingatt == self.fromatt:
                self.keyvaliditylookupsql += " NULLS FIRST"

        if self.__prefill:
            self.__prefillcaches(usefetchfirst)

    def __prefillcaches(self, usefetchfirst):
        args = None
        if self.toatt:
            # We can use the toatt to see if rows are still current.
            # Select all attributes from the rows where maxto is set to the
            # default value (which may be NULL)
            sql = 'SELECT %s FROM %s WHERE %s %s' % \
                  (', '.join(self.quotelist(self.all)), self.name,
                   self.quote(self.toatt),
                   'IS NULL' if self.maxto is None else '= %(maxto)s')
            if self.maxto is not None:
                args = {'maxto': self.maxto}
        else:
            # We have to find max(versionatt) for each group of lookupatts and
            # do a join to get the right rows.
            lookupattlist = ', '.join(self.quotelist(self.lookupatts))
            newestversions = ('SELECT %s, MAX(%s) AS %s FROM %s GROUP BY %s' %
                              (lookupattlist,
                               self.quote(self.orderingatt),
                               self.quote(self.orderingatt), self.name,
                               lookupattlist))
            joincond = ' AND '.join(['A.%s = B.%s' % (self.quote(att), att)
                                     for att in [l for l in self.lookupatts] +
                                     [self.orderingatt]
                                     ])
            sql = 'SELECT %s FROM (%s) AS A, %s AS B WHERE %s' %\
                (', '.join(['B.%s AS %s' % (self.quote(att), att)
                            for att in self.all]),
                 newestversions, self.name, joincond)

        # sql is a statement that fetches the newest versions from the database
        # in order to fill the caches, the FETCH FIRST clause is for a finite
        # cache, if the user set the flag that it is supported by the database.
        positions = [self.all.index(att) for att in self.lookupatts]
        if self.__cachesize > 0 and usefetchfirst:
            sql += ' FETCH FIRST %d ROWS ONLY' % self.__cachesize
        self.targetconnection.execute(sql, args)

        if self.__cachesize < 0:
            allrawrows = self.targetconnection.fetchalltuples()
        else:
            allrawrows = self.targetconnection.fetchmanytuples(
                self.__cachesize)

        for rawrow in allrawrows:
            self.rowcache[rawrow[0]] = rawrow
            t = tuple([rawrow[i] for i in positions])
            self.keycache[t] = rawrow[0]

    def lookup(self, row, namemapping={}):
        """Find the key for the newest version with the given values.

           Arguments:

           - row: a dict which must contain at least the lookup attributes
           - namemapping: an optional namemapping (see module's documentation)
        """
        if self.__prefill and (self.__cachesize < 0 or
                               len(self.keycache) < self.__cachesize):
            # Everything is cached. We don't have to look in the DB
            return self._before_lookup(row, namemapping)
        else:
            # Something is not cached so we have to use the classical lookup.
            # Note that __init__ updated self.keylookupsql to use ORDER BY ...
            if self.useorderby:
                return Dimension.lookup(self, row, namemapping)
            else:
                return self.__lookupnewestlocally(row, namemapping)

    def __lookupnewestlocally(self, row, namemapping):
        """Find the key for the newest version of the row with the given values.

           Arguments:

           - row: a dict which must contain at least the lookup attributes
           - namemapping: an optional namemapping (see module's documentation)
        """
        # Based on Dimension.lookup, but uses keyversionlookupsql and
        # finds the newest version locally (no sorting on the DBMS)
        key = self._before_lookup(row, namemapping)
        if key is not None:
            return key

        self.targetconnection.execute(self.keyversionlookupsql, row,
                                      namemapping)

        versions = [kv for kv in self.targetconnection.fetchalltuples()]
        if not versions:
            # There is no existing version
            keyvalue = None
        else:
            # Look in all (key, version) pairs and find the key for the newest
            # version
            keyvalue, versionvalue = versions[-1]
            for kv in versions:
                if kv[1] > versionvalue:
                    keyvalue, versionvalue = kv

        self._after_lookup(row, namemapping, keyvalue)
        return keyvalue


    def scdensure(self, row, namemapping={}):
        """Lookup or insert a version of a slowly changing dimension member.

           .. Note:: Has side-effects on the given row.

           Arguments:

           - row: a dict containing the attributes for the member.
             key, versionatt, fromatt, and toatt are not required to be
             present but will be added (if defined).
           - namemapping: an optional namemapping (see module's documentation)
        """
        key = (namemapping.get(self.key) or self.key)
        if self.versionatt:
            versionatt = (namemapping.get(self.versionatt) or self.versionatt)
        else:
            versionatt = None
        if self.fromatt:  # this protects us against None in namemapping.
            fromatt = (namemapping.get(self.fromatt) or self.fromatt)
        else:
            fromatt = None
        if self.toatt:
            toatt = (namemapping.get(self.toatt) or self.toatt)
        else:
            toatt = None
        if self.srcdateatt:
            srcdateatt = (namemapping.get(self.srcdateatt) or self.srcdateatt)
        else:
            srcdateatt = None

        # Get the newest version and compare to that
        keyval = self.lookup(row, namemapping)
        if keyval is None:
            # It is a new member. We add the first version.
            if versionatt:
                row[versionatt] = 1

            if fromatt and fromatt not in row:
                if self.minfrom is not None:
                    # We need the following hack to distinguish between
                    # 'not set' and 'use the value None'...
                    if self.minfrom == (None,):
                        row[fromatt] = None
                    else:
                        row[fromatt] = self.minfrom
                else:
                    row[fromatt] = self.fromfinder(self.targetconnection,
                                                   row, namemapping)
            if toatt and toatt not in row:
                row[toatt] = self.maxto
            row[key] = self.insert(row, namemapping)
            return row[key]
        else:
            # There is an existing version. Check if the attributes are
            # identical
            type1updates = {}  # for type 1
            addnewversion = False  # for type 2
            other = self.getbykey(keyval)  # the full existing version
            for att in self.all:
                # Special (non-)handling of versioning and key attributes:
                if att in (self.key, self.orderingatt, self.versionatt):
                    # Don't compare these - we don't expect them to have
                    # meaningful values in row
                    continue
                # We may have to compare the "from" and "to" dates
                elif att == self.toatt:
                    if other[self.toatt] != self.maxto:
                        # That version was closed manually or by closecurrent
                        # and we now have to add a new version
                        addnewversion = True
                elif att == self.fromatt:
                    if self.srcdateatt is None:  # We don't compare dates then
                        continue
                    else:
                        # We have to compare the dates in row[..] and other[..]
                        # We have to make sure that the dates are of comparable
                        # types.
                        rdt = self.srcdateparser(row[srcdateatt])
                        if rdt == other[self.fromatt]:
                            continue  # no change in the "from attribute"
                        elif isinstance(rdt, type(other[self.fromatt])):
                            # they are not equal but are of the same type, so
                            # we are dealing with a new date
                            addnewversion = True
                        else:
                            # They have different types (and are thus not
                            # equal). Try to convert to strings and see if they
                            # are equal.
                            modref = (self.targetconnection
                                      .getunderlyingmodule())
                            rowdate = modref.Date(rdt.year, rdt.month, rdt.day)
                            if str(rowdate).strip('\'"') != \
                                    str(other[self.fromatt]).strip('\'"'):
                                addnewversion = True
                # Handling of "normal" attributes:
                else:
                    mapped = (namemapping.get(att) or att)
                    if row[mapped] != other[att]:
                        if att in self.type1atts:
                            type1updates[att] = row[mapped]
                        else:
                            addnewversion = True
                if addnewversion and not self.type1atts:
                    # We don't have to look for possible type 1 updates
                    # and we already know that a type 2 update is needed.
                    break
                # else: continue

            if len(type1updates) > 0:
                # Some type 1 updates were found
                self.__performtype1updates(type1updates, other)

            if addnewversion:  # type 2
                # Make a new row version and insert it
                row.pop(key, None)
                if versionatt:
                    row[versionatt] = other[self.versionatt] + 1

                if fromatt:
                    row[fromatt] = self.fromfinder(self.targetconnection,
                                                   row, namemapping)
                if toatt:
                    row[toatt] = self.maxto
                row[key] = self.insert(row, namemapping)
                # Update the todate attribute in the old row version in the DB
                # if it has not been set manually or by closecurrent
                if toatt and other[self.toatt] == self.maxto:
                    toattval = self.tofinder(self.targetconnection, row,
                                             namemapping)
                    self.targetconnection.execute(
                        self.updatetodatesql, {
                            self.key: keyval, self.toatt: toattval})
                # Only cache the newest version
                if self.__cachesize and keyval in self.rowcache:
                    del self.rowcache[keyval]
            else:
                # Update the row dict by giving version and dates and the key
                row[key] = keyval
                if self.versionatt:
                    row[versionatt] = other[self.versionatt]
                if self.fromatt:
                    row[fromatt] = other[self.fromatt]
                if self.toatt:
                    row[toatt] = other[self.toatt]

            return row[key]

    def _before_lookup(self, row, namemapping):
        if self.__cachesize:
            namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
            searchtuple = tuple([row[n] for n in namesinrow])
            return self.keycache.get(searchtuple, None)
        return None

    def _after_lookup(self, row, namemapping, resultkey):
        if self.__cachesize and resultkey is not None:
            namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
            searchtuple = tuple([row[n] for n in namesinrow])
            self.keycache[searchtuple] = resultkey

    def _before_getbykey(self, keyvalue):
        if self.__cachesize:
            res = self.rowcache.get(keyvalue)
            if res is not None:
                return dict(zip(self.all, res))
        return None

    def _after_getbykey(self, keyvalue, resultrow):
        if self.__cachesize and resultrow[self.key] is not None:
            # if resultrow[self.key] is None, no result was found in the db
            self.rowcache[keyvalue] = tuple([resultrow[a] for a in self.all])

    def _before_update(self, row, namemapping):
        """ """
        # We have to remove old values from the caches.
        key = (namemapping.get(self.key) or self.key)
        for att in self.lookupatts:
            if (att in namemapping and namemapping[att] in row) or att in row:
                # A lookup attribute is about to be changed and we should make
                # sure that the cache does not map from the old value.  Here,
                # we can only see the new value, but we can get the old lookup
                # values by means of the key:
                oldrow = self.getbykey(row[key])
                searchtuple = tuple([oldrow[n] for n in self.lookupatts])
                if searchtuple in self.keycache:
                    del self.keycache[searchtuple]
                break

        if row[key] in self.rowcache:
            # The cached row is now incorrect. We must make sure it is
            # not in the cache.
            del self.rowcache[row[key]]

        return None

    def _after_insert(self, row, namemapping, newkeyvalue):
        """ """
        # After the insert, we can look it up. Pretend that we
        # did that. Then we get the new data cached.
        # NB: Here we assume that the DB doesn't change or add anything.
        # For example, a DEFAULT value in the DB or automatic type coercion can
        # break this assumption.
        # Note that we always cache inserted members (in CachedDimension
        # this is an option).
        if self.__cachesize:
            self._after_lookup(row, namemapping, newkeyvalue)
            tmp = pygrametl.project(self.all[1:], row, namemapping)
            tmp[self.key] = newkeyvalue
            self._after_getbykey(newkeyvalue, tmp)

    def __performtype1updates(self, updates, lookupvalues, namemapping={}):
        """ """
        # find the keys in the rows that should be updated
        self.targetconnection.execute(self.keylookupsql, lookupvalues,
                                      namemapping)
        updatekeys = [e[0] for e in self.targetconnection.fetchalltuples()]
        updatekeys.reverse()
        # Generate SQL for the update
        valparts = ", ".join(
            ["%s = %%(%s)s" % (self.quote(k), k) for k in updates])
        keyparts = ", ".join([str(k) for k in updatekeys])
        sql = "UPDATE %s SET %s WHERE %s IN (%s)" % \
            (self.name, valparts, self.quote(self.key), keyparts)
        self.targetconnection.execute(sql, updates)
        # Remove from our own cache
        for key in updatekeys:
            if key in self.rowcache:
                del self.rowcache[key]

    def closecurrent(self, row, namemapping={}, end=pygrametl.today()):
        """Close the current version by setting its toatt if it is maxto.

           The newest version will have its toatt set to the given end
           argument (default pygrametl.today(), i.e., the current date) only
           if the value for the newest row's toatt currently is maxto.
           Otherwise no update will be done.
           If toatt is not defined an exception is raised.

           Arguments:

           - row: a dict which must contain at least the lookup attributes
           - namemapping: an optional namemapping (see module's documentation)
           - end: the value to set for the newest version. Default: The current
             date as given by pygrametl.today()
        """
        if self.toatt is None:
            raise RuntimeError("A toatt must be defined")
        keyval = self.lookup(row, namemapping)
        if keyval is None:
            raise RuntimeError("Member does not exist and cannot be closed")
        existing = self.getbykey(keyval)
        if existing[self.toatt] == self.maxto:
            self.update({self.key: keyval, self.toatt: end})

    def lookupasof(self, row, when, inclusive, namemapping={}):
        """Find the key of the version that was valid at a given time.

           If both fromatt and toatt have been set, the method returns the key
           of the version where the given time is between them. If only toatt
           has been defined, the method returns the key of the first version
           where toatt is after the given time. If only fromatt is defined,
           the method returns the key of the most recent version where fromatt
           is before the given time. See also the description of the argument
           inclusive. For this to be possible, fromatt and/or toatt must have
           been set. If this is not the case, a RuntimeError is raised. If no
           valid version is found for the given time, None is returned.

           Note that this function cannot exploit the cache and always results
           in the execution of a SQL query.

           Arguments:

           - row: a dict which must contain at least the lookup attributes.
           - when: the time when the version should be valid. This argument
             must be of a type that can be compared (using <, <=, ==, =>, >)
             to values in fromatt and/or toatt.
           - inclusive: decides if the values of fromatt and/or toatt are
             allowed to be equal to the value of when in the version to find.
             If only one of fromatt and toatt has been set, the argument
             should be a single Boolean. If both fromatt and toatt have been
             set, the argument should be a tuple of two Booleans where the
             first element decides if the fromatt value can be equal to the
             the value of when and the second element decides the same for
             toatt. This tuple must not be (False, False).
           - namemapping: an optional namemapping (see module's documentation)

        """
        if self.fromatt and self.toatt:
            return self._lookupasofusingfromattandtoatt(row, when, inclusive,
                                                        namemapping)
        elif self.fromatt:
            return self._lookupasofusingfromatt(row, when, inclusive,
                                                namemapping)
        elif self.toatt:
            return self._lookupasofusingtoatt(row, when, inclusive,
                                              namemapping)
        else:
            raise RuntimeError(
                "fromatt and/or toatt must be set if lookupasof should be used"
            )

    def _getversions(self, row, namemapping):
        """Return an ordered list of all versions of a given member"""
        # The constructed SQL depends on what arguments the user
        # passed to __init__. 
        self.targetconnection.execute(self.keyvaliditylookupsql, row,
                                      namemapping)
        return [kv for kv in self.targetconnection.rowfactory()]
        
    def _lookupasofusingtoatt(self, row, when, inclusive, namemapping):
        """Helper function for lookupasof"""
        # _getversions gives back all versions [1st, 2nd, ...] sorted on
        # fromatt and with NULLS LAST in this case (see how
        # self.keyvaliditylookupsql is made in __init__).  We iterate over the
        # list and when we find a version where toatt > when (or >= if
        # inclusive is True), it is the version that was valid at "when",
        # i.e., the one we are looking for. If toatt is None, we're looking at
        # the last version and, since it doesn't have an explicit timestamp,
        # we must assume that it was/will be valid at time "when".
        op = ge if inclusive else gt
        versions = self._getversions(row, namemapping)
        for ver in versions:
            toattval = ver[self.toatt]
            if toattval == None or op(toattval, when):
                return ver[self.key]
        return None
        
    def _lookupasofusingfromatt(self, row, when, inclusive, namemapping):
        """Helper function for lookupasof"""
        # _getversions gives back all versions [1st, 2nd, ...] sorted on
        # fromatt and with NULLS FIRST in this case (see how
        # self.keyvaliditylookupsql is made in __init__).  We revert the list
        # and iterate over it. When we find a version where fromatt < when (or
        # <= if inclusive is True), it is the most recent version that became
        # valid before "when", i.e., the one we are looking for. If fromatt is
        # None, we're looking at the 1st version and, since it doesn't have an
        # explicit timestamp, we must assume that it was valid at time "when".
        op = le if inclusive else lt
        versions = self._getversions(row, namemapping)
        versions.reverse() 
        for ver in versions:
            fromattval = ver[self.fromatt]
            if fromattval == None or op(fromattval, when):
                return ver[self.key]
        return None

    def _lookupasofusingfromattandtoatt(self, row, when, inclusive, namemapping):
        """Helper function for lookupasof"""
        # At least one of the timestamps must be included as there otherwise
        # would be gaps between versions. In other words, [from, to), (from,
        # to], and [from, to] are possible, but (from, to) is not
        if not any(inclusive):
            raise ValueError(
                "At least one of inclusive[0] and inclusive[1] must be True"
            )
        # We do as in _lookupasofusingtoatt, but also have to check that the
        # version had become valid (i.e., fromatt < when (or <=))
        fromop = le if inclusive[0] else lt
        toop = ge if inclusive[1] else gt
        versions = self._getversions(row, namemapping)

        for ver in versions:
            toattval = ver[self.toatt]
            if toattval == None or toop(toattval, when):
                fromattval = ver[self.fromatt]
                if fromattval == None or fromop(fromattval, when):
                    return ver[self.key]
                else:
                    # Different versions don't overlap in the dimension, so no
                    # need to look further
                    break
        return None

SCDimension = SlowlyChangingDimension


# NB: SnowflakedDimension's methods may have side-effects:
# row[somedim.key] = someval.

class SnowflakedDimension(object):

    """A class for accessing a snowflaked dimension spanning several tables
       in the underlying database. Lookups and inserts are then automatically
       spread out over the relevant tables while the programmer only needs
       to interact with a single SnowflakedDimension instance.
    """

    def __init__(self, references, expectboguskeyvalues=False):
        """Arguments:

           - references: a sequence of pairs of Dimension objects
             [(a1,a2), (b1,b2), ...] meaning that a1 has a foreign key to a2
             etc. a2 may itself be a sequence of Dimensions:
             [(a1, [a21, a22, ...]), (b1, [b21, b22, ...]), ...].

             The first element of the first pair (a1 in the example above) must
             be the dimension table representing the lowest level in the
             hierarchy (i.e., the dimension table the closest to the fact
             table).

             Each dimension must be reachable in a unique way (i.e., the
             given dimensions form a tree).

             A foreign key must have the same name as the primary key it
             references.

           - expectboguskeyvalues: If expectboguskeyvalues is True, we allow a
             key that is used as lookup attribute in a lower level to hold a
             wrong value (which would typically be None). When ensure or
             insert is called, we find the correct value for the key in the
             higher level.  If expectboguskeyvalues, we again try a lookup on
             the lower level after this. If expectboguskeyvalues is False, we
             move directly on to do an insert. Default: False
        """
        self.root = references[0][0]
        self.targetconnection = self.root.targetconnection
        self.key = self.root.key
        self.lookupatts = self.root.lookupatts

        dims = set([self.root])
        self.refs = {}
        self.refkeys = {}
        self.all = self.root.all[:]
        for (dim, refeddims) in references:
            # Check that all dimensions use the same target connection.
            # Build the dict self.refs:
            #           {dimension -> set(refed dimensions)}
            # Build self.all from dimensions' lists
            # Keep track of seen dimensions by means of the set dims and
            # ensure that each table is only reachable once.
            if isinstance(refeddims, Dimension):
                # If there is only one dimension, then make a tuple with that
                refeddims = (refeddims, )
            for rd in refeddims:
                if rd.targetconnection is not self.targetconnection:
                    raise ValueError("Different connections used")
                if rd in dims:
                    raise ValueError("The tables do not form a tree")
                dims.add(rd)
                tmp = self.refs.get(dim, set())
                tmp.add(rd)
                self.refs[dim] = tmp
                # The key is alredy there as we assume FKs and PKs have
                # identical names
                self.all.extend(list(rd.attributes))

        # Check that all dimensions in dims are reachable from the root
        dimscopy = dims.copy()
        dimscopy.remove(self.root)
        for (_, targets) in self.refs.items():
            for target in targets:
                # It is safe to use remove as each dim is only referenced once
                dimscopy.remove(target)
        # Those dimensions that are left in dims at this point are unreachable
        if len(dimscopy) != 0:
            raise ValueError("Not every given dimension is reachable")

        # Construct SQL...

        self.keylookupsql = self.root.keylookupsql

        self.allnames = [self.root.key]
        for dim in dims:
            for att in dim.attributes:
                self.allnames.append(att)

        # Make sure that there are no duplicated names:
        if len(self.allnames) != len(set(self.allnames)):
            raise ValueError("Duplicated attribute names found")

        self.alljoinssql = "SELECT " + \
            ", ".join(self.root.quotelist(self.allnames)) + \
            " FROM " + " NATURAL JOIN ".join(map(lambda d: d.name, dims))
        self.rowlookupsql = self.alljoinssql + " WHERE %s.%s = %%(%s)s" % \
            (self.root.name, self.root.quote(self.root.key), self.root.key)

        self.levels = {}
        self.__buildlevels(self.root, 0)
        self.levellist = list(range(len(self.levels)))
        self.levellist.reverse()

        self.expectboguskeyvalues = expectboguskeyvalues

    def __buildlevels(self, node, level):
        tmp = self.levels.get(level, [])
        tmp.append(node)
        self.levels[level] = tmp
        for ref in self.refs.get(node, []):
            self.__buildlevels(ref, level + 1)

    def lookup(self, row, namemapping={}):
        """Find the key for the row with the given values.

           Arguments:

           - row: a dict which must contain at least the lookup attributes
             which all must come from the root (the table closest to the
             fact table).
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self._before_lookup(row, namemapping)
        if res:
            return res
        res = self.root.lookup(row, namemapping)
        self._after_lookup(row, namemapping, res)
        return res

    def _before_lookup(self, row, namemapping):
        return None

    def _after_lookup(self, row, namemapping, resultkeyvalue):
        pass

    def getbykey(self, keyvalue, fullrow=False):
        """Lookup and return the row with the given key value.

           If no row is found in the dimension table, the function returns
           a row where all values (including the key) are None.

           Arguments:

           - keyvalue: the key value of the row to lookup
           - fullrow: a flag deciding if the full row (with data from
             all tables in the snowflake) should be returned. If False,
             only data from the lowest level in the hierarchy (i.e., the table
             the closest to the fact table) is returned. Default: False
        """
        res = self._before_getbykey(keyvalue, fullrow)
        if res:
            return res
        if not fullrow:
            res = self.root.getbykey(keyvalue)
        else:
            self.targetconnection.execute(self.rowlookupsql,
                                          {self.root.key: keyvalue})
            res = self.targetconnection.fetchone(self.allnames)
        self._after_getbykey(keyvalue, res, fullrow)
        return res

    def _before_getbykey(self, keyvalue, fullrow=False):
        return None

    def _after_getbykey(self, keyvalue, resultrow, fullrow=False):
        pass

    def getbyvals(self, values, namemapping={}, fullrow=False):
        """Return a list of all rows with values identical to the given.

           Arguments:

           - values: a dict which must hold a subset of the tables attributes.
             All rows that have identical values for all attributes in this
             dict are returned.
           - namemapping: an optional namemapping (see module's documentation)
           - fullrow: a flag deciding if the full row (with data from
             all tables in the snowflake) should be returned. If False,
             only data from the lowest level in the hierarchy (i.e., the table
             the closest to the fact table) is returned. Default: False
        """
        res = self._before_getbyvals(values, namemapping)
        if res is not None:
            return res

        if not fullrow:
            res = self.root.getbyvals(values, namemapping)
        else:
            # select all attributes from the table.
            # The attributes available from the
            # values dict are used in the WHERE clause.
            attstouse = [a for a in self.allnames
                         if a in values or a in namemapping]
            sqlwhere = " WHERE " + \
                " AND ".join(["%s = %%(%s)s" % (self.root.quote(att), att)
                              for att in attstouse])
            self.targetconnection.execute(self.alljoinssql + sqlwhere,
                                          values, namemapping)
            res = [r for r in self.targetconnection.rowfactory(self.allnames)]

        self._after_getbyvals(values, namemapping, res)
        return res

    def _before_getbyvals(self, values, namemapping, fullrow=False):
        return None

    def _after_getbyvals(self, values, namemapping, resultrows, fullrow=False):
        pass

    def update(self, row, namemapping={}):
        """Update rows in the participating dimension tables.

           If the key of a participating dimension D is in the given row,
           D.update(...) is invoked.

           Note that this function is not good to use for updating a foreign
           key which here has the same name as the referenced primary key: The
           referenced table could then also get updated unless it is ensured
           that none of its attributes are present in the given row.

           In other words, it is often better to use the update function
           directly on the Dimensions that should be updated.

           Arguments:

           - row: a dict. If the key of a participating dimension D is in the
             dict, D.update(...) is invoked.
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self._before_update(row, namemapping)
        if res is not None:
            return

        for l in self.levellist:
            for t in self.levels[l]:
                if t.key in row or \
                        (t.key in namemapping and namemapping[t.key] in row):
                    t.update(row, namemapping)

        self._after_update(row, namemapping)

    def _before_update(self, row, namemapping):
        return None

    def _after_update(self, row, namemapping):
        pass

    def ensure(self, row, namemapping={}):
        """Lookup the given member. If that fails, insert it. Return key value.

           If the member must be inserted, data is automatically inserted in
           all participating tables where (part of) the member is not
           already represented.

           Key values for different levels may be added to the row. It is
           NOT guaranteed that key values for all levels exist in row
           afterwards.

           Arguments:

           - row: the row to lookup or insert. Must contain the lookup
             attributes. Key is not required to be present but will be added
             using idfinder if missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        (key, _) = self.__ensure_helper(self.root, row, namemapping, False)
        return key

    def insert(self, row, namemapping={}):
        """Insert the given member. If that fails, insert it. Return key value.

           Data is automatically inserted in all participating tables where
           (part of) the member is not already represented. If nothing is
           inserted at all, a ValueError is raised.

           Key values for different levels may be added to the row. It is
           NOT guaranteed that key values for all levels exist in row
           afterwards.

           Arguments:

           - row: the row to lookup or insert. Must contain the lookup
             attributes. Key is not required to be present but will be added
             using idfinder if missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        key = self._before_insert(row, namemapping)
        if key is not None:
            return key
        (key, insertdone) = self.__ensure_helper(self.root, row, namemapping,
                                                 False)
        if not insertdone:
            raise ValueError("Member already present - nothing inserted")
        self._after_insert(row, namemapping, key)
        return key

    def _before_insert(self, row, namemapping):
        return None

    def _after_insert(self, row, namemapping, newkeyvalue):
        pass

    def endload(self):
        """Finalize the load."""
        pass

    def __ensure_helper(self, dimension, row, namemapping, insertdone):
        """ """
        # NB: Has side-effects: Key values are set for all dimensions
        key = None
        retry = False
        try:
            key = dimension.lookup(row, namemapping)
        except KeyError:
            retry = True  # it can happen that the keys for the levels above
            # aren't there yet but should be used as lookup
            # attributes in dimension.
            # Below we find them and we should then try a
            # lookup again before we move on to do an insertion
        if key is not None:
            row[(namemapping.get(dimension.key) or dimension.key)] = key
            return (key, insertdone)
        # Else recursively get keys for refed tables and then insert
        for refed in self.refs.get(dimension, []):
            (key, insertdone) = self.__ensure_helper(refed, row, namemapping,
                                                     insertdone)
            # We don't need to set the key value in the row as this already
            # happened in the recursive step.

        # We set insertdone = True to know later that we actually
        # inserted something
        if retry or self.expectboguskeyvalues:
            # The following is similar to
            #   key = dimension.ensure(row, namemapping)
            # but we set insertdone here.
            key = dimension.lookup(row, namemapping)
            if key is None:
                key = dimension.insert(row, namemapping)
                insertdone = True
        else:
            # We don't need to lookup again since no attributes were
            # missing (no KeyError) and we don't expect bogus values.
            # So we can proceed directly to do an insert.
            key = dimension.insert(row, namemapping)
            insertdone = True

        row[(namemapping.get(dimension.key) or dimension.key)] = key
        return (key, insertdone)

    def scdensure(self, row, namemapping={}):
        """Lookup or insert a version of a slowly changing dimension member.

           .. Warning::
              Still experimental!!! For now we require that only the
              root is a SlowlyChangingDimension.

           .. Note:: Has side-effects on the given row.

           Arguments:

           - row: a dict containing the attributes for the member. Key is not
             required to be present but will be added using idfinder if
             missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        # Still experimental!!! For now we require that only the
        # root is a SlowlyChangingDimension.
        # If we were to allow other nodes to be SCDs, we should require
        # that those between those nodes and the root (incl.) were also
        # SCDs.
        for dim in self.levels.get(1, []):
            (keyval, _) = self.__ensure_helper(dim, row, namemapping,
                                               False)
            row[(namemapping.get(dim.key) or dim.key)] = keyval

        row[(namemapping.get(self.root.key) or self.root.key)] = \
            self.root.scdensure(row, namemapping)
        return row[(namemapping.get(self.root.key) or self.root.key)]


class FactTable(object):

    """A class for accessing a fact table in the DW."""

    def __init__(self, name, keyrefs, measures=(), targetconnection=None):
        """Arguments:
           - name: the name of the fact table in the DW
           - keyrefs: a sequence of attribute names that constitute the
             primary key of the fact tables (i.e., the dimension references)
           - measures: a possibly empty sequence of measure names. Default: ()
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.
        """
        if targetconnection is None:
            targetconnection = pygrametl.getdefaulttargetconnection()
        self.targetconnection = targetconnection
        self.name = name
        self.keyrefs = keyrefs
        self.measures = measures
        self.all = [k for k in keyrefs] + [m for m in measures]
        pygrametl._alltables.append(self)

        self.quote = _quote
        self.quotelist = lambda x: [self.quote(xn) for xn in x]
        # Create SQL

        # INSERT INTO name (key1, ..., keyn, meas1, ..., measn)
        # VALUES (%(key1)s, ..., %(keyn)s, %(meas1)s, ..., %(measn)s)
        self.insertsql = "INSERT INTO " + name + "(" + \
            ", ".join(self.quotelist(self.all)) + ") VALUES (" + \
            ", ".join(["%%(%s)s" % (att,) for att in self.all]) + ")"

        # SELECT key1, ..., keyn, meas1, ..., measn FROM name
        # WHERE key1 = %(key1)s AND ... keyn = %(keyn)s
        self.lookupsql = "SELECT " + ",".join(self.quotelist(self.all)) + \
            " FROM " + name + \
            " WHERE " + " AND ".join(["%s = %%(%s)s" % (self.quote(k), k)
                                      for k in self.keyrefs])

    def insert(self, row, namemapping={}):
        """Insert a fact into the fact table.

           Arguments:

           - row: a dict at least containing values for all the fact table's
             attributes (both keys/references and measures).
           - namemapping: an optional namemapping (see module's documentation)
        """
        tmp = self._before_insert(row, namemapping)
        if tmp:
            return
        self.targetconnection.execute(self.insertsql, row, namemapping)
        self._after_insert(row, namemapping)

    def _before_insert(self, row, namemapping):
        return None

    def _after_insert(self, row, namemapping):
        pass

    def _emptyfacttonone(self, argdict):
        """Return None if the given argument only contains None values,
           otherwise return the given argument
        """
        for k in self.keyrefs:
            if argdict[k] is not None:
                return argdict
        return None

    def lookup(self, keyvalues, namemapping={}):
        """Lookup a fact from the given key values. Return key and measure vals.

           Return None if no fact is found.

           Arguments:

           - keyvalues: a dict at least containing values for all keys
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self._before_lookup(keyvalues, namemapping)
        if res:
            return self._emptyfacttonone(res)
        self.targetconnection.execute(self.lookupsql, keyvalues, namemapping)
        res = self.targetconnection.fetchone(self.all)
        self._after_lookup(keyvalues, namemapping, res)
        return self._emptyfacttonone(res)

    def _before_lookup(self, keyvalues, namemapping):
        return None

    def _after_lookup(self, keyvalues, namemapping, resultrow):
        pass

    def ensure(self, row, compare=False, namemapping={}):
        """Ensure that a fact is present (insert it if it is not already there).

           Return True if a fact with identical values for keyrefs attributes
           was already present in the fact table; False if not.

           Arguments:

           - row: a dict at least containing the attributes of the fact table
           - compare: a flag deciding if measure vales from a fact that was
             looked up are compared to those in the given row. If True and
             differences are found, a ValueError is raised. Default: False
           - namemapping: an optional namemapping (see module's documentation)

        """
        res = self.lookup(row, namemapping)
        if not res:
            self.insert(row, namemapping)
            return False
        elif compare:
            for m in self.measures:
                mappedm = namemapping.get(m, m)
                if mappedm in row and row[mappedm] != res.get(m):
                    raise ValueError(
                        "The existing fact has a different measure value" +
                        " for '" + m + "': " + str(res.get(m)) +
                        " instead of " + str(row.get(mappedm)) +
                        " as in the given row"
                    )
        return True

    def endload(self):
        """Finalize the load."""
        pass


class BatchFactTable(FactTable):

    """A class for accessing a fact table in the DW. This class performs
       performs insertions in batches.
    """

    def __init__(self, name, keyrefs, measures=(), batchsize=10000,
                 usemultirow=False, targetconnection=None):
        """Arguments:

           - name: the name of the fact table in the DW
           - keyrefs: a sequence of attribute names that constitute the
             primary key of the fact tables (i.e., the dimension references)
           - measures: a possibly empty sequence of measure names. Default: ()
           - batchsize: an int deciding how many insert operations should be
             done in one batch. Default: 10000
           - usemultirow: load batches with an INSERT INTO name VALUES statement
             instead of executemany(). WARNING: single quotes are automatically
             escaped. Other forms of sanitization must be manually performed.
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.

        """
        FactTable.__init__(self,
                           name=name,
                           keyrefs=keyrefs,
                           measures=measures,
                           targetconnection=targetconnection)

        self.__batchsize = batchsize
        self.__batch = []
        if usemultirow:
            self.__insertnow = self.__insertmultirow
            self.__basesql = self.insertsql[:self.insertsql.find(' (') + 1]
            self.__rowtovalue = lambda row: '(' + ','.join(map(
                lambda c: pygrametl.getsqlfriendlystr(row[c]), self.all)) + ')'
        else:
            self.__insertnow = self.__insertexecutemany

    def _before_insert(self, row, namemapping):
        self.__batch.append(pygrametl.project(self.all, row, namemapping))
        if len(self.__batch) == self.__batchsize:
            self.__insertnow()
        return True  # signal that we did something

    def _before_lookup(self, keyvalues, namemapping):
        self.__insertnow()

    def endload(self):
        """Finalize the load."""
        self.__insertnow()

    def __insertmultirow(self):
        if self.__batch:
            values = map(self.__rowtovalue, self.__batch)
            insertsql = self.__basesql + ','.join(values)
            self.targetconnection.execute(insertsql)
            self.__batch = []

    def __insertexecutemany(self):
        if self.__batch:
            self.targetconnection.executemany(self.insertsql, self.__batch)
            self.__batch = []

    @property
    def awaitingrows(self):
        """Return the amount of rows awaiting to be loaded into the table"""
        return len(self.__batch)


class AccumulatingSnapshotFactTable(FactTable):

    """A class for accessing and updating an accumulating fact table in the DW.
       Facts in an accumulating fact table can be updated. The class does
       no caching and all lookups and updates are sent to the DBMS (and an
       index on the keyrefs should thus be considered).
    """

    def __init__(self, name, keyrefs, otherrefs, measures=(),
                 ignorenonerefs=True, ignorenonemeasures=True,
                 factexpander=None, targetconnection=None):
        """Arguments:

           - name: the name of the fact table in the DW
           - keyrefs: a sequence of attribute names that constitute the
             primary key of the fact tables. This is a subset of the dimension
             references and these references are not allowed to be updated.
           - otherrefs: a sequence of dimension references that can be updated.
           - measures: a possibly empty sequence of measure names. Default: ()
           - ignorenonerefs: A flag deciding if None values for attributes in
             otherrefs are ignored when doing an update. If True (default), the
             existing value in the database will not be overwritten by a None.
           - ignorenonemeasures: A flag deciding if None values for attributes
             in measures are ignored when doing an update. If True (default),
             the existing value in the database will not be overwritten by a
             None.
           - factexpander: A function(row, namemapping, set of names of updated
             attributes). This function is called by the ensure method before
             it calls the update method if a row has been changed. This is,
             e.g., practical if lag measures should be computed before the row
             in the fact table is updated. The function should make its
             changes directly on the passed row.
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.
        """

        FactTable.__init__(self,
                           name=name,
                           keyrefs=keyrefs,
                           measures=otherrefs + measures,
                           targetconnection=targetconnection)

        self.measures = measures#FactTable.__init__ set it to otherrefs+measures
        self.otherrefs = otherrefs
        self.ignorenonerefs = ignorenonerefs
        self.ignorenonemeasures = ignorenonemeasures
        self.factexpander = factexpander

    # insert and lookup are inherited from FactTable

    def ensure(self, row, namemapping={}):
        """Lookup the given row. If that fails, insert it. If found, see
           if values for attributes in otherrefs or measures have changed and
           update the found row if necessary (note that values for attributes
           in keyrefs are not allowed to change). If an update is necessary
           and a factexpander is defined, the row will first be updated with
           any missing otherrefs/measures and the factexpander will be run on
           it. Return nothing.

           Arguments:

           - row: the row to insert or update if needed. Must contain the
             keyrefs attributes. For missing attributes from otherrefs and
             measures, the value is set to None if the row has to be
             inserted.
           - namemapping: an optional namemapping (see module's documentation)
        """
        oldrow = self.lookup(row, namemapping)
        if not oldrow:
            if (self.otherrefs + self.measures) - row.keys():
                # some keys are missing in row; fix it in a copy and use that
                row = pygrametl.copy(row, **namemapping)
                namemapping = {}
                for att in (self.otherrefs + self.measures):
                    if att not in row:
                        row[att] = None
            self.insert(row, namemapping)
            return
        else:
            updated = self.__differences(oldrow, row, namemapping)
            if updated:
                if self.factexpander:
                    self.__addmissingkeys(row, namemapping, oldrow)
                    self.factexpander(row, namemapping, updated)
                    updated = self.__differences(oldrow, row, namemapping)
                self.__doupdates(row, namemapping, updated)

    def __differences(self, oldrow, newrow, namemapping):
        # finds differences between oldrow and newrow wrt. otherrefs and
        # measures (namemapping is used for newrow only)
        differences = set()
        self.__diffhelper(oldrow, newrow, namemapping, self.otherrefs, \
                           self.ignorenonerefs, differences)
        self.__diffhelper(oldrow, newrow, namemapping, self.measures, \
                          self.ignorenonemeasures, differences)
        return differences

    def __diffhelper(self, oldrow, newrow, namemapping, atts, ignorenone, res):
        for a in atts:
            newa = namemapping.get(a) or a
            if newrow.get(newa) != oldrow.get(a):
                if newrow.get(newa) is not None or not ignorenone:
                    res.add(a)

    def __addmissingkeys(self, row, namemappingfornew, oldrow):
        for key in self.all:
            mappedkey = namemappingfornew.get(key) or key
            if mappedkey not in row:
                row[mappedkey] = oldrow[key]

    def update(self, row, namemapping={}):
        oldrow = self.lookup(row, namemapping)
        updated = self.__differences(oldrow, row, namemapping)
        if updated:
            self.__doupdates(row, namemapping, updated)

    def __doupdates(self, newrow, namemapping, updated):
        updatesql = "UPDATE " + self.name + " SET " + \
                    ",".join(["%s = %%(%s)s" %
                              (self.quote(a), a) for a in updated]) + \
                    " WHERE " + " AND ".join(["%s = %%(%s)s" %
                                              (self.quote(k), k) \
                                              for k in self.keyrefs])
        self.targetconnection.execute(updatesql, newrow, namemapping)


class _BaseBulkloadable(object):

    """Common functionality for bulkloadable tables"""

    def __init__(self, name, atts, bulkloader,
                 fieldsep='\t', rowsep='\n', nullsubst=None,
                 tempdest=None, bulksize=500000, usefilename=False,
                 strconverter=pygrametl.getdbfriendlystr, encoding=None,
                 dependson=()):
        r"""Arguments:

           - name: the name of the table in the DW
           - atts: a sequence of the bulkloadable tables' attribute names
           - bulkloader: A method
             m(name, attributes, fieldsep, rowsep, nullsubst, tempdest) that
             is called to load data from a temporary file into the DW. The
             argument "attributes" is a list of the names of the columns to
             insert values into and show the order in which the attribute
             values appear in the temporary file.  The rest of the arguments
             are similar to those arguments with identical names that are given
             to _BaseBulkloadable.__init__ as described here. The argument
             "tempdest" can, however, be 1) a string with a filename or
             2) a file object. This is determined by the usefilename argument
             to _BaseBulkloadable.__init__ (see below).
           - fieldsep: a string used to separate fields in the temporary
             file. Default: '\t'
           - rowsep: a string used to separate rows in the temporary file.
             Default: '\n'
           - nullsubst: an optional string used to replace None values.
             If nullsubst=None, no substitution takes place. Default: None
           - tempdest: a file object or None. If None a named temporary file
             is used. Default: None
           - bulksize: an int deciding the number of rows to load in one
             bulk operation. Default: 500000
           - usefilename: a value deciding if the file should be passed to the
             bulkloader by its name instead of as a file-like object.
             Default: False
           - strconverter: a method m(value, nullsubst) -> str to convert
             values into strings that can be written to the temporary file and
             eventually bulkloaded. Default: pygrametl.getdbfriendlystr
           - encoding: a string with the encoding to use. If None,
             locale.getpreferredencoding() is used. This argument is
             ignored under Python 2! Default: None
           - dependson: a sequence of other bulkloadble tables that should
             be loaded before this instance does bulkloading (e.g., if
             a fact table has foreign keys to some bulkloaded dimension
             tables). Default: ()
        """

        self.name = name
        self.atts = atts
        self.__close = False
        if tempdest is None:
            self.__close = True
            self.__namedtempfile = tempfile.NamedTemporaryFile()
            tempdest = self.__namedtempfile.file
            self.__filename = self.__namedtempfile.name
        else:
            if usefilename and not path.exists(tempdest.name):
                raise ValueError("Usefilename cannot be used with invalid "
                                 "tempdest path '%s'" % tempdest.name)
            self.__filename = tempdest.name
        self.fieldsep = fieldsep
        self.rowsep = rowsep
        self.nullsubst = nullsubst
        self.bulkloader = bulkloader
        self.tempdest = tempdest

        self.bulksize = bulksize
        self.usefilename = usefilename
        self.strconverter = strconverter
        if encoding is not None:
            self.encoding = encoding
        else:
            self.encoding = locale.getpreferredencoding()

        # Ensure objects that are not tables, are not silently removed
        if not (set(dependson) <= set(pygrametl._alltables)):
            raise ValueError("Dependson must contain tables only")
        self.dependson = filter(
            lambda b: hasattr(b, '_bulkloadnow'), dependson)

        if version_info[0] == 2:
            # Python 2: We ignore the specified encoding
            self._tobytes = lambda data, encoding: data
        else:
            # Python 3: We make _tobytes use the specified encoding:
            self._tobytes = lambda data, encoding: bytes(data, encoding)

        self.__count = 0
        self.__ready = True

    def __preparetempfile(self):
        self.__namedtempfile = tempfile.NamedTemporaryFile()
        self.tempdest = self.__namedtempfile.file
        self.__filename = self.__namedtempfile.name
        self.__ready = True

    @property
    def awaitingrows(self):
        """Return the amount of rows awaiting to be loaded into the table"""
        return self.__count

    def insert(self, row, namemapping={}):
        """Insert (eventually) a row into the table.

           Arguments:

           - row: a dict at least containing values for each of the tables'
             attributes.
           - namemapping: an optional namemapping (see module's documentation)
        """

        if not self.__ready:
            self.__preparetempfile()
        rawdata = [row[namemapping.get(att) or att] for att in self.atts]
        data = [self.strconverter(val, self.nullsubst) for val in rawdata]
        try:
            line = self.fieldsep.join(data)
        except TypeError as e:
            expl = 'Could not join values into a single string'
            if self.nullsubst is None and None in rawdata:
                expl += '. A nullsubst must be defined.'
            raise TypeError(expl, e)
        self.__count += 1
        self.tempdest.write(
            self._tobytes("%s%s" % (line, self.rowsep), self.encoding))
        if self.__count == self.bulksize:
            self._bulkloadnow()

    def _bulkloadnow(self):
        if self.__count == 0:
            return

        for b in self.dependson:
            b._bulkloadnow()

        self.tempdest.flush()
        self.tempdest.seek(0)
        self.bulkloader(self.name, self.atts,
                        self.fieldsep, self.rowsep, self.nullsubst,
                        self.usefilename and self.__filename or self.tempdest)
        self.tempdest.seek(0)
        self.tempdest.truncate(0)
        self.__count = 0

    def endload(self):
        """Finalize the load."""
        self._bulkloadnow()
        if self.__close:
            try:
                self.__namedtempfile.close()
            except OSError:
                pass  # may happen if the instance was decoupled
            self.__ready = False

    def _decoupled(self):
        if self.__close:
            # We need to make a private tempfile
            self.__namedtempfile = tempfile.NamedTemporaryFile()
            self.tempdest = self.__namedtempfile.file
            self.__filename = self.__namedtempfile.name


class BulkFactTable(_BaseBulkloadable):

    """Class for addition of facts to a fact table. Reads are not supported."""

    def __init__(self, name, keyrefs, measures, bulkloader,
                 fieldsep='\t', rowsep='\n', nullsubst=None,
                 tempdest=None, bulksize=500000, usefilename=False,
                 strconverter=pygrametl.getdbfriendlystr,
                 encoding=None, dependson=()):
        r"""Arguments:

           - name: the name of the fact table in the DW
           - keyrefs: a sequence of attribute names that constitute the
             primary key of the fact tables (i.e., the dimension references)
           - measures: a possibly empty sequence of measure names.
           - bulkloader: A method
             m(name, attributes, fieldsep, rowsep, nullsubst, tempdest) that
             is called to load data from a temporary file into the DW. The
             argument "attributes" is the combination of keyrefs and measures
             (i.e., a list of the names of the columns to insert values into)
             and show the order in which the attribute values appear in the
             temporary file.  The rest of the arguments are similar to those
             arguments with identical names that are given to
             BulkFactTable.__init__ as described here. The argument "tempdest"
             can, however, be 1) a string with a filename or 2) a file
             object. This is determined by the usefilename argument to
             BulkFactTable.__init__ (see below).
           - fieldsep: a string used to separate fields in the temporary
             file. Default: '\t'
           - rowsep: a string used to separate rows in the temporary file.
             Default: '\n'
           - nullsubst: an optional string used to replace None values.
             If nullsubst=None, no substitution takes place. Default: None
           - tempdest: a file object or None. If None a named temporary file
             is used. Default: None
           - bulksize: an int deciding the number of rows to load in one
             bulk operation. Default: 500000
           - usefilename: a value deciding if the file should be passed to the
             bulkloader by its name instead of as a file-like object. This is,
             e.g., necessary when the bulk loading is invoked through SQL
             (instead of directly via a method on the PEP249 driver). It is
             also necessary if the bulkloader runs in another process
             (for example, when if the BulkFactTable is wrapped by a
             DecoupledFactTable and invokes the bulkloader on a shared
             connection wrapper). Default: False
           - strconverter: a method m(value, nullsubst) -> str to convert
             values into strings that can be written to the temporary file and
             eventually bulkloaded. Default: pygrametl.getdbfriendlystr
           - encoding: a string with the encoding to use. If None,
             locale.getpreferredencoding() is used. This argument is
             ignored under Python 2! Default: None
           - dependson: a sequence of other bulkloadble tables that should
             be bulkloaded before this instance does bulkloading (e.g., if
             the fact table has foreign keys to some bulk-loaded dimension
             table). Default: ()
        """
        self.keyrefs = keyrefs
        self.measures = measures
        self.all = [k for k in keyrefs] + [m for m in measures]
        
        # This class does not do much in itself, but looks like a FactTable with
        # keyrefs and measures.
        _BaseBulkloadable.__init__(self,
                                   name=name,
                                   atts=[k for k in keyrefs] +
                                   [m for m in measures],
                                   bulkloader=bulkloader,
                                   fieldsep=fieldsep,
                                   rowsep=rowsep,
                                   nullsubst=nullsubst,
                                   tempdest=tempdest,
                                   bulksize=bulksize,
                                   usefilename=usefilename,
                                   strconverter=strconverter,
                                   encoding=encoding,
                                   dependson=dependson)

        pygrametl._alltables.append(self)


class BulkDimension(_BaseBulkloadable, CachedDimension):

    """A class for accessing a dimension table. Does caching and bulk loading.

       Unlike CachedBulkDimension, this class always caches all dimension data.

       The class caches all dimension members in memory. Newly inserted
       dimension members are also put into the cache. The class does not
       INSERT new dimension members into the underlying database table
       immediately when insert or ensure is invoked. Instead, the class does
       bulk loading of new members. When a certain amount of new dimension
       members have been inserted (configurable through __init__'s bulksize
       argument), a user-provided bulkloader method is called.

       Calls of lookup and ensure will only use the cache and does not invoke
       any database operations. It is also possible to use the update and
       getbyvals methods, but calls of these will invoke the bulkloader first
       (and performance can degrade). If the dimension table's full rows
       are cached (by setting __init__'s cachefullrow argument to True), a
       call of getbykey will only use the cache, but if cachefullrows==False
       (which is the default), the bulkloader is again invoked first.

       We assume that the DB doesn't change or add any attribute
       values that are cached.
       For example, a DEFAULT value in the DB or automatic type coercion can
       break this assumption.
    """

    def __init__(self, name, key, attributes, bulkloader, lookupatts=(),
                 idfinder=None, defaultidvalue=None, rowexpander=None,
                 cachefullrows=False, fieldsep='\t', rowsep='\n',
                 nullsubst=None, tempdest=None, bulksize=500000,
                 usefilename=False, strconverter=pygrametl.getdbfriendlystr,
                 encoding=None, dependson=(), targetconnection=None):
        r"""Arguments:

           - name: the name of the dimension table in the DW
           - key: the name of the primary key in the DW
           - attributes: a sequence of the attribute names in the dimension
             table. Should not include the name of the primary key which is
             given in the key argument.
           - bulkloader: A method
             m(name, attributes, fieldsep, rowsep, nullsubst, tempdest) that
             is called to load data from a temporary file into the DW. The
             argument "attributes" is a list of the names of the columns to
             insert values into and show the order in which the attribute
             values appear in the temporary file.  The rest of the arguments
             are similar to those arguments with identical names that are
             described below. The argument "tempdest" can, however, be
             1) a string with a filename or 2) a file object. This is
             determined by the usefilename argument (see below).
           - lookupatts: A subset of the attributes that uniquely identify
             a dimension members. These attributes are thus used for looking
             up members. If not given, it is assumed that
             lookupatts = attributes
           - idfinder: A function(row, namemapping) -> key value that assigns
             a value to the primary key attribute based on the content of the
             row and namemapping. If not given, it is assumed that the primary
             key is an integer, and the assigned key value is then the current
             maximum plus one.
           - defaultidvalue: An optional value to return when a lookup fails.
             This should thus be the ID for a preloaded "Unknown" member.
           - rowexpander: A function(row, namemapping) -> row. This function
             is called by ensure before insertion if a lookup of the row fails.
             This is practical if expensive calculations only have to be done
             for rows that are not already present. For example, for a date
             dimension where the full date is used for looking up rows, a
             rowexpander can be set such that week day, week number, season,
             year, etc. are only calculated for dates that are not already
             represented. If not given, no automatic expansion of rows is
             done.
           - cachefullrows: a flag deciding if full rows should be
             cached. If not, the cache only holds a mapping from
             lookupattributes to key values. Default: False.
           - fieldsep: a string used to separate fields in the temporary
             file. Default: '\t'
           - rowsep: a string used to separate rows in the temporary file.
             Default: '\n'
           - nullsubst: an optional string used to replace None values.
             If nullsubst=None, no substitution takes place. Default: None
           - tempdest: a file object or None. If None a named temporary file
             is used. Default: None
           - bulksize: an int deciding the number of rows to load in one
             bulk operation. Default: 500000
           - usefilename: a value deciding if the file should be passed to the
             bulkloader by its name instead of as a file-like object. This is,
             e.g., necessary when the bulk loading is invoked through SQL
             (instead of directly via a method on the PEP249 driver). It is
             also necessary if the bulkloader runs in another process.
             Default: False
           - strconverter: a method m(value, nullsubst) -> str to convert
             values into strings that can be written to the temporary file and
             eventually bulkloaded. Default: pygrametl.getdbfriendlystr
           - encoding: a string with the encoding to use. If None,
             locale.getpreferredencoding() is used. This argument is
             ignored under Python 2! Default: None
           - dependson: a sequence of other bulkloadble tables that should
             be loaded before this instance does bulkloading. Default: ()
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.
        """
        _BaseBulkloadable.__init__(self,
                                   name=name,
                                   atts=[key] + [a for a in attributes],
                                   bulkloader=bulkloader,
                                   fieldsep=fieldsep,
                                   rowsep=rowsep,
                                   nullsubst=nullsubst,
                                   tempdest=tempdest,
                                   bulksize=bulksize,
                                   usefilename=usefilename,
                                   strconverter=strconverter,
                                   encoding=encoding,
                                   dependson=dependson)

        CachedDimension.__init__(self,
                                 name=name,
                                 key=key,
                                 attributes=attributes,
                                 lookupatts=lookupatts,
                                 idfinder=idfinder,
                                 defaultidvalue=defaultidvalue,
                                 rowexpander=rowexpander,
                                 size=0,
                                 prefill=True,
                                 cachefullrows=cachefullrows,
                                 cacheoninsert=True,
                                 usefetchfirst=False,
                                 targetconnection=targetconnection)

        self.emptyrow = dict(zip(self.atts, len(self.atts) * (None,)))

    def _before_getbyvals(self, values, namemapping):
        self._bulkloadnow()
        return None

    def _before_update(self, row, namemapping):
        self._bulkloadnow()
        return None

    def getbykey(self, keyvalue):
        """Lookup and return the row with the given key value.

           If no row is found in the dimension table, the function returns
           a row where all values (including the key) are None.
        """

        if not self.cachefullrows:
            self._bulkloadnow()
            return CachedDimension.getbykey(self, keyvalue)

        # else we do cache full rows and all rows are cached...
        if isinstance(keyvalue, dict):
            keyvalue = keyvalue[self.key]

        row = self._before_getbykey(keyvalue)
        if row is not None:
            return row
        else:
            # Do not look in the DB; we cache everything
            return self.emptyrow.copy()

    def insert(self, row, namemapping={}):
        """Insert the given row. Return the new key value.

           Arguments:

           - row: the row to insert. The dict is not updated. It must contain
             all attributes, and is allowed to contain more attributes than
             that. Key is not required to be present but will be added using
             idfinder if missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        res = self._before_insert(row, namemapping)
        if res is not None:
            return res

        key = (namemapping.get(self.key) or self.key)
        if row.get(key) is None:
            keyval = self.idfinder(row, namemapping)
            row = dict(row)  # Make a copy to change
            row[key] = keyval
        else:
            keyval = row[key]

        _BaseBulkloadable.insert(self, row, namemapping)

        self._after_insert(row, namemapping, keyval)
        return keyval


class CachedBulkDimension(_BaseBulkloadable, CachedDimension):

    """A class for accessing a dimension table. Does caching and bulk loading.

       Unlike BulkDimension, the cache size is configurable and lookups may
       thus lead to database operations.

       The class caches dimension members in memory. Newly inserted
       dimension members are also put into the cache. The class does not
       INSERT new dimension members into the underlying database table
       immediately when insert or ensure is invoked. Instead, the class does
       bulk loading of new members. When a certain amount of new dimension
       members have been inserted (configurable through __init__'s bulksize
       argument), a user-provided bulkloader method is called.

       It is also possible to use the update and getbyvals methods, but calls
       of these will invoke the bulkloader first (and performance can
       degrade). If the dimension table's full rows are cached (by setting
       __init__'s cachefullrow argument to True), a call of getbykey will only
       use the cache, but if cachefullrows==False (which is the default), the
       bulkloader is again invoked first.

       We assume that the DB doesn't change or add any attribute
       values that are cached.
       For example, a DEFAULT value in the DB or automatic type coercion can
       break this assumption.
    """

    def __init__(self, name, key, attributes, bulkloader, lookupatts=(),
                 idfinder=None, defaultidvalue=None, rowexpander=None,
                 usefetchfirst=False, cachefullrows=False,
                 fieldsep='\t', rowsep='\n', nullsubst=None,
                 tempdest=None, bulksize=5000, cachesize=10000,
                 usefilename=False, strconverter=pygrametl.getdbfriendlystr,
                 encoding=None, dependson=(), targetconnection=None):
        r"""Arguments:

           - name: the name of the dimension table in the DW
           - key: the name of the primary key in the DW
           - attributes: a sequence of the attribute names in the dimension
             table. Should not include the name of the primary key which is
             given in the key argument.
           - bulkloader: A method
             m(name, attributes, fieldsep, rowsep, nullsubst, tempdest) that
             is called to load data from a temporary file into the DW. The
             argument "attributes" is a list of the names of the columns to
             insert values into and show the order in which the attribute
             values appear in the temporary file.  The rest of the arguments
             are similar to those arguments with identical names that are
             described below. The argument "tempdest" can, however, be
             1) a string with a filename or 2) a file object. This is
             determined by the usefilename argument (see below).
           - lookupatts: A subset of the attributes that uniquely identify
             a dimension members. These attributes are thus used for looking
             up members. If not given, it is assumed that
             lookupatts = attributes
           - idfinder: A function(row, namemapping) -> key value that assigns
             a value to the primary key attribute based on the content of the
             row and namemapping. If not given, it is assumed that the primary
             key is an integer, and the assigned key value is then the current
             maximum plus one.
           - defaultidvalue: An optional value to return when a lookup fails.
             This should thus be the ID for a preloaded "Unknown" member.
           - rowexpander: A function(row, namemapping) -> row. This function
             is called by ensure before insertion if a lookup of the row fails.
             This is practical if expensive calculations only have to be done
             for rows that are not already present. For example, for a date
             dimension where the full date is used for looking up rows, a
             rowexpander can be set such that week day, week number, season,
             year, etc. are only calculated for dates that are not already
             represented. If not given, no automatic expansion of rows is
             done.
           - usefetchfirst: a flag deciding if the SQL:2008 FETCH FIRST
             clause is used when prefil is True. Depending on the used DBMS
             and DB driver, this can give significant savings wrt. to time and
             memory. Not all DBMSs support this clause yet. Default: False
           - cachefullrows: a flag deciding if full rows should be
             cached. If not, the cache only holds a mapping from
             lookupattributes to key values. Default: False.
           - fieldsep: a string used to separate fields in the temporary
             file. Default: '\t'
           - rowsep: a string used to separate rows in the temporary file.
             Default: '\n'
           - nullsubst: an optional string used to replace None values.
             If nullsubst=None, no substitution takes place. Default: None
           - tempdest: a file object or None. If None a named temporary file
             is used. Default: None
           - bulksize: an int deciding the number of rows to load in one
             bulk operation. Default: 5000
           - cachesize: the maximum number of rows to cache. If less than or
             equal to 0, unlimited caching is used. Default: 10000
           - usefilename: a value deciding if the file should be passed to the
             bulkloader by its name instead of as a file-like object. This is,
             e.g., necessary when the bulk loading is invoked through SQL
             (instead of directly via a method on the PEP249 driver). It is
             also necessary if the bulkloader runs in another process.
             Default: False
           - strconverter: a method m(value, nullsubst) -> str to convert
             values into strings that can be written to the temporary file and
             eventually bulkloaded. Default: pygrametl.getdbfriendlystr
           - encoding: a string with the encoding to use. If None,
             locale.getpreferredencoding() is used. This argument is
             ignored under Python 2! Default: None
           - dependson: a sequence of other bulkloadble tables that should
             be loaded before this instance does bulkloading. Default: ()
           - targetconnection: The ConnectionWrapper to use. If not given,
             the default target connection is used.
        """
        _BaseBulkloadable.__init__(self,
                                   name=name,
                                   atts=[key] + [a for a in attributes],
                                   bulkloader=bulkloader,
                                   fieldsep=fieldsep,
                                   rowsep=rowsep,
                                   nullsubst=nullsubst,
                                   tempdest=tempdest,
                                   bulksize=bulksize,
                                   usefilename=usefilename,
                                   strconverter=strconverter,
                                   encoding=encoding,
                                   dependson=dependson)

        CachedDimension.__init__(self,
                                 name=name,
                                 key=key,
                                 attributes=attributes,
                                 lookupatts=lookupatts,
                                 idfinder=idfinder,
                                 defaultidvalue=defaultidvalue,
                                 rowexpander=rowexpander,
                                 size=cachesize,
                                 prefill=True,
                                 cachefullrows=cachefullrows,
                                 cacheoninsert=True,
                                 usefetchfirst=usefetchfirst,
                                 targetconnection=targetconnection)

        self.emptyrow = dict(zip(self.atts, len(self.atts) * (None,)))

        self.__localcache = {}
        self.__localkeys = {}

    def _before_lookup(self, row, namemapping):
        namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
        searchtuple = tuple([row[n] for n in namesinrow])

        if searchtuple in self.__localcache:
            return self.__localcache[searchtuple][self.key]
        return CachedDimension._before_lookup(self, row, namemapping)

    def _before_getbyvals(self, values, namemapping):
        self._bulkloadnow()
        return None

    def _before_update(self, row, namemapping):
        self._bulkloadnow()
        return None

    def _bulkloadnow(self):
        emptydict = {}
        for key, row in self.__localkeys.items():
            self._after_insert(row, emptydict, key)
        self.__localcache.clear()
        self.__localkeys.clear()
        _BaseBulkloadable._bulkloadnow(self)
        return

    def getbykey(self, keyvalue):
        """Lookup and return the row with the given key value.

           If no row is found in the dimension table, the function returns
           a row where all values (including the key) are None.
        """
        if isinstance(keyvalue, dict):
            keyvalue = keyvalue[self.key]

        if keyvalue in self.__localkeys:
            return self.__localkeys[keyvalue].copy()
        return CachedDimension.getbykey(self, keyvalue)

    def lookup(self, row, namemapping={}):
        return CachedDimension.lookup(self, row, namemapping=namemapping)

    def insert(self, row, namemapping={}):
        """Insert the given row. Return the new key value.

           Arguments:

           - row: the row to insert. The dict is not updated. It must contain
             all attributes, and is allowed to contain more attributes than
             that. Key is not required to be present but will be added using
             idfinder if missing.
           - namemapping: an optional namemapping (see module's documentation)
        """
        row = pygrametl.copy(row, **namemapping)
        searchtuple = tuple([row[n] for n in self.lookupatts])
        res = self._before_insert(row, {})
        if res is not None:
            return res

        if row.get(self.key) is None:
            keyval = self.idfinder(row, {})
            row[self.key] = keyval
        else:
            keyval = row[self.key]

        if searchtuple in self.__localcache:
            return self.__localcache[searchtuple]

        _BaseBulkloadable.insert(self, row, {})
        self.__localcache[searchtuple] = row
        self.__localkeys[keyval] = row
        return keyval


class SubprocessFactTable(object):

    """Class for addition of facts to a subprocess.

       The subprocess can, e.g., be a logger or bulkloader. Reads are not
       supported.

       Note that a created instance can not be used when endload() has been
       called (and endload() is called from pygrametl.commit()).
    """

    def __init__(self, keyrefs, measures, executable,
                 initcommand=None, endcommand=None, terminateafter=-1,
                 fieldsep='\t', rowsep='\n', nullsubst=None,
                 strconverter=pygrametl.getdbfriendlystr,
                 buffersize=16384):
        r"""Arguments:

           - keyrefs: a sequence of attribute names that constitute the
             primary key of the fact table (i.e., the dimension references)
           - measures: a possibly empty sequence of measure names. Default: ()
           - executable: The subprocess to start.
           - initcommand: If not None, this command is written to the
             subprocess before any data.
           - endcommand: If not None, this command is written to the subprocess
             after all data has been written.
           - terminateafter: If greater than or equal to 0, the subprocess
             is terminated after this amount of seconds after the pipe to
             the subprocess is closed.
           - fieldsep: a string used to separate fields in the output
             sent to the subprocess. Default: '\t
           - rowsep: a string used to separate rows in the output sent to the
             subprocess. Default: '\n'
           - nullsubst: an optional string used to replace None values.
             If nullsubst=None, no substitution takes place. Default: None
           - strconverter: a method m(value, nullsubst) -> str to convert
             values into strings that can be written to the subprocess.
             Default: pygrametl.getdbfriendlystr
        """

        self.all = [k for k in keyrefs] + [m for m in measures]
        self.keyrefs = keyrefs
        self.measures = measures
        self.endcommand = endcommand
        self.terminateafter = terminateafter
        self.fieldsep = fieldsep
        self.rowsep = rowsep
        self.strconverter = strconverter
        self.nullsubst = nullsubst
        self.process = Popen(executable, bufsize=buffersize, shell=True,
                             stdin=PIPE)
        self.pipe = self.process.stdin

        if initcommand is not None:
            self.pipe.write(initcommand)

        pygrametl._alltables.append(self)


    def insert(self, row, namemapping={}):
        """Insert a fact into the fact table.

           Arguments:

           - row: a dict at least containing values for the keys and measures.
           - namemapping: an optional namemapping (see module's documentation)
        """
        rawdata = [row[namemapping.get(att) or att] for att in self.all]
        data = [self.strconverter(val, self.nullsubst) for val in rawdata]
        self.pipe.write("%s%s" % (self.fieldsep.join(data), self.rowsep))


    def endload(self):
        """Finalize the load."""
        if self.endcommand is not None:
            self.pipe.write(self.endcommand)

        self.pipe.close()
        if self.terminateafter >= 0:
            sleep(self.terminateafter)
            self.process.terminate()
        else:
            self.process.wait()

    def _decoupling(self):
        """Raise a TypeError to avoid decoupling (does not happen in Jython)"""
        import sys
        if sys.platform.startswith('java'):
            # In Jython, we use threads for decoupling and we do not have
            # to prevent it.
            return
        raise TypeError('A SubProcessFactTable cannot be decoupled')


class DecoupledDimension(pygrametl.parallel.Decoupled):

    """A Dimension-like class that enables parallelism by executing all
       operations on a given Dimension in a separate, dedicated process
       (that Dimension is said to be "decoupled").
    """

    def __init__(self, dim, returnvalues=True, consumes=(), attstoconsume=(),
                 batchsize=500, queuesize=200):
        """Arguments:

           - dim: the Dimension object to use in a separate process
           - returnvalues: decides if return values from method calls on dim
             should be kept such that they can be fetched by the caller or
             another Decoupled instance
           - consumes: a sequence of Decoupled objects from which to fetch
             returnvalues (that are used to replace FutureResults in
             arguments).
             Default: ()
           - attstoconsume: a sequence of the attribute names in rows that
             should have FutureResults replaced by actual return values. Does
             not have to be given, but may improve performance when given.
             Default: ()
           - batchsize: the size of batches (grouped method calls) transferred
             between the processes. NB: Large values do not necessarily give
             good performance
             Default: 500
           - queuesize: the maximum amount of waiting batches. Infinite if
             less than or equal to 0. NB: Large values do not necessarily give
             good performance.
             Default: 200
        """
        pygrametl.parallel.Decoupled.__init__(self,
                                              obj=dim,
                                              returnvalues=returnvalues,
                                              consumes=consumes,
                                              directupdatepositions=tuple(
                                                  [(0, a) for a in
                                                   attstoconsume]),
                                              batchsize=batchsize,
                                              queuesize=queuesize,
                                              autowrap=False)

        if dim in pygrametl._alltables:
            pygrametl._alltables.remove(dim)  # We add self instead...
        pygrametl._alltables.append(self)

    def lookup(self, row, namemapping={}):
        """Invoke lookup on the decoupled Dimension in the separate process"""
        return self._enqueue('lookup', row, namemapping)

    def getbykey(self, keyvalue):
        """Invoke getbykey on the decoupled Dimension in the separate
           process"""
        return self._enqueue('getbykey', keyvalue)

    def getbyvals(self, row, namemapping={}):
        "Invoke betbycals on the decoupled Dimension in the separate process"
        return self._enqueue('getbyvals', row, namemapping)

    def insert(self, row, namemapping={}):
        """Invoke insert on the decoupled Dimension in the separate process"""
        return self._enqueue('insert', row, namemapping)

    def ensure(self, row, namemapping={}):
        """Invoke ensure on the decoupled Dimension in the separate process"""
        return self._enqueue('ensure', row, namemapping)

    def endload(self):
        """Invoke endload on the decoupled Dimension in the separate process and
           return when all waiting method calls have been executed
        """
        # first add 'endload' to the batch and then send the batch
        self._enqueuenoreturn('endload')
        self._endbatch()
        self._join()
        return None

    def scdensure(self, row, namemapping={}):
        "Invoke scdensure on the decoupled Dimension in the separate process"
        if hasattr(self._obj, 'scdensure'):
            return self._enqueue('scdensure', row, namemapping)
        else:
            raise AttributeError('The object does not support scdensure')


class DecoupledFactTable(pygrametl.parallel.Decoupled):

    """A FactTable-like class that enables parallelism by executing all
       operations on a given FactTable in a separate, dedicated process
       (that FactTable is said to be "decoupled").
    """

    def __init__(self, facttbl, returnvalues=True, consumes=(),
                 attstoconsume=(), batchsize=500, queuesize=200):
        """Arguments:

           - facttbl: the FactTable object to use in a separate process
           - returnvalues: decides if return values from method calls on
             facttbl should be kept such that they can be fetched by the caller
             or another Decoupled instance
           - consumes: a sequence of Decoupled objects from which to fetch
             returnvalues (that are used to replace FutureResults in
             arguments).
             Default: ()
           - attstoconsume: a sequence of the attribute names in rows that
             should have FutureResults replaced by actual return values. Does
             not have to be given, but may improve performance when given.
             Default: ()
           - batchsize: the size of batches (grouped method calls) transferred
             between the processes. NB: Large values do not necessarily give
             good performance
             Default: 500
           - queuesize: the maximum amount of waiting batches. Infinite if
             less than or equal to 0. NB: Large values do not necessarily give
             good performance.
             Default: 200
        """
        pygrametl.parallel.Decoupled.__init__(self,
                                              obj=facttbl,
                                              returnvalues=returnvalues,
                                              consumes=consumes,
                                              directupdatepositions=tuple(
                                                  [(0, a) for a in
                                                   attstoconsume]),
                                              batchsize=batchsize,
                                              queuesize=queuesize,
                                              autowrap=False)

        if facttbl in pygrametl._alltables:
            pygrametl._alltables.remove(facttbl)  # We add self instead
        pygrametl._alltables.append(self)

    def insert(self, row, namemapping={}):
        """Invoke insert on the decoupled FactTable in the separate process"""
        return self._enqueue('insert', row, namemapping)

    def endload(self):
        """Invoke endload on the decoupled FactTable in the separate process and
           return when all waiting method calls have been executed
        """
        self._enqueuenoreturn('endload')
        self._endbatch()
        self._join()
        return None

    def lookup(self, row, namemapping={}):
        """Invoke lookup on the decoupled FactTable in the separate process"""
        if hasattr(self._obj, 'lookup'):
            return self._enqueue('lookup', row, namemapping)
        else:
            raise AttributeError('The object does not support lookup')

    def ensure(self, row, namemapping={}):
        """Invoke ensure on the decoupled FactTable in the separate process"""
        if hasattr(self._obj, 'ensure'):
            return self._enqueue('ensure', row, namemapping)
        else:
            raise AttributeError('The object does not support ensure')


#######

class BasePartitioner(object):

    """A base class for partitioning between several parts.

       See also DimensionPartitioner and FactTablePartitioner.
    """

    def __init__(self, parts):
        self.parts = list(parts)
        self.__nextpart = 0

    def parts(self):
        """Return the parts the partitioner works on"""
        return self.parts[:]

    def addpart(self, part):
        """Add a part"""
        self.parts.append(part)

    def droppart(self, part=None):
        """Drop a part. If an argument is given, it must be a part of the
           patitioner and it will then be removed. If no argument is given,
           the first part is removed."""
        if part is None:
            self.parts.pop()
        else:
            self.parts.remove(part)

    def getpart(self, row, namemapping={}):
        """Find  the part that should handle the given row. The provided
           implementation in BasePartitioner does only use round robin
           partitioning, but subclasses apply other methods """
        part = self.parts[self.__nextpart]
        self.__nextpart = (self.__nextpart + 1) % len(self.parts)
        return part

    def endload(self):
        """Call endload on all parts"""
        for part in self.parts:
            part.endload()


class DimensionPartitioner(BasePartitioner):

    """A Dimension-like class that handles partitioning.

       Partitioning is done between a number of Dimension objects called the
       parts. The class offers the interface of Dimensions (incl. scdensure
       from SlowlyChangingDimension). When a method is called, the
       corresponding method on one of the parts (chosen by a user-definable
       partitioner function) will be invoked. The parts can operate on a
       single physical dimension table or different physical tables.
    """

    def __init__(self, parts, getbyvalsfromall=False, partitioner=None):
        """Arguments:

           - parts: a sequence of Dimension objects.
           - getbyvalsfromall: determines if getbyvals should be answered by
             means of all parts (when getbyvalsfromall = True) or only the
             first part, i.e., parts[0] (when getbybalsfromall = False).
             Default: False
           - partitioner: None or a callable p(dict) -> int where the argument
             is a dict mapping from the names of the lookupatts to the values
             of the lookupatts. The resulting int is used to determine which
             part a given row should be handled by.
             When partitioner is None, a default partitioner is used. This
             partitioner computes the hash value of each value of the
             lookupatts and adds them together.
        """
        BasePartitioner.__init__(self, parts=parts)
        self.getbyvalsfromall = getbyvalsfromall
        self.lookupatts = parts[0].lookupatts
        self.key = parts[0].key
        for p in parts:
            if not p.lookupatts == self.lookupatts:
                raise ValueError('The parts must have the same lookupatts')
            if not p.key == self.key:
                raise ValueError('The parts must have the same key')
        if partitioner is not None:
            self.partitioner = partitioner
        else:
            # A partitioner that takes the hash of each attribute value in
            # row and adds them all together:
            # Reading from right to left: get the values, use hash() on each
            # of them, and add all the hash values
            self.partitioner = lambda row: reduce((lambda x, y: x + y),
                                                  map(hash, row.values()))

    def getpart(self, row, namemapping={}):
        """Return the part that should handle the given row"""
        vals = {}
        for att in self.lookupatts:
            vals[att] = row[namemapping.get(att) or att]
        return self.parts[self.partitioner(vals) % len(self.parts)]

    # Below this, methods like those in Dimensions:

    def lookup(self, row, namemapping={}):
        """Invoke lookup on the relevant Dimension part"""
        part = self.getpart(row, namemapping)
        return part.lookup(row, namemapping)

    def __getbykeyhelper(self, keyvalue):
        # Returns (rowresult, part). part is None if no result was found.
        for part in self.parts:
            row = part.getbykey(keyvalue)
            if row[self.key] is not None:
                return (row, part)
        return (row, None)

    def getbykey(self, keyvalue):
        """Invoke getbykey on the relevant Dimension part"""
        return self.__getbykeyhelper(keyvalue)[0]

    def getbyvals(self, values, namemapping={}):
        """Invoke getbyvals on the first part or all parts (depending on the
           value of the instance's getbyvalsfromall)"""
        if not self.getbyvalsfromall:
            return self.parts[0].getbyvals(values, namemapping)
        res = []
        for part in self.parts:
            res += part.getbyvals(values, namemapping)
        return res

    def update(self, row, namemapping={}):
        """Invoke update on the relevant Dimension part"""
        keyval = row[namemapping.get(self.key) or self.key]
        part = self.__getbykeyhelper(keyval)[1]
        if part is not None:
            part.update(row, namemapping)

    def ensure(self, row, namemapping={}):
        """Invoke ensure on the relevant Dimension part"""
        part = self.getpart(row, namemapping)
        return part.ensure(row, namemapping)

    def insert(self, row, namemapping={}):
        """Invoke insert on the relevant Dimension part"""
        part = self.getpart(row, namemapping)
        return part.insert(row, namemapping)

    def scdensure(self, row, namemapping={}):
        """Invoke scdensure on the relevant Dimension part"""
        part = self.getpart(row, namemapping)
        return part.scdensure(row, namemapping)


class FactTablePartitioner(BasePartitioner):

    """A FactTable-like class that handles partitioning.

       Partitioning is done between a number of FactTable objects called the
       parts. The class offers the interface of FactTable. When a method is
       called, the corresponding method on one of the parts (chosen by a
       user-definable partitioner function) will be invoked. The parts can
       operate on a single physical fact table or different physical
       tables.
    """

    def __init__(self, parts, partitioner=None):
        """
        Arguments:

        - parts: a sequence of FactTable objects.
        - partitioner: None or a callable p(dict) -> int where the argument
          is a dict mapping from the names of the keyrefs to the values of
          the keyrefs. The resulting int is used to determine which part
          a given row should be handled by.
          When partitioner is None, a default partitioner is used. This
          partitioner computes the sum of all the keyrefs values.
        """
        BasePartitioner.__init__(self, parts=parts)
        if partitioner is not None:
            self.partitioner = partitioner
        else:
            self.partitioner = lambda row: reduce((lambda x, y: x + y),
                                                  row.values())
        self.all = parts[0].all
        self.keyrefs = parts[0].keyrefs
        self.measures = parts[0].measures
        for ft in parts:
            if not (self.keyrefs == ft.keyrefs and
                    self.measures == ft.measures):
                raise ValueError(
                    'The parts must have the same measures and keyrefs')

    def getpart(self, row, namemapping={}):
        """Return the relevant part for the given row """
        vals = {}
        for att in self.keyrefs:
            vals[att] = row[namemapping.get(att) or att]
        return self.parts[self.partitioner(vals) % len(self.parts)]

    def insert(self, row, namemapping={}):
        """Invoke insert on the relevant part """
        part = self.getpart(row, namemapping)
        part.insert(row, namemapping)

    def lookup(self, row, namemapping={}):
        """Invoke lookup on the relevant part """
        part = self.getpart(row, namemapping)
        return part.lookup(row, namemapping)

    def ensure(self, row, namemapping={}):
        """Invoke ensure on the relevant part """
        part = self.getpart(row, namemapping)
        return part.ensure(row, namemapping)
