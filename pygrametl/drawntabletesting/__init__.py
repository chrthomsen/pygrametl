"""This module contains classes and functions for defining preconditions and
   postconditions for database state. The conditions can be used in unit tests
   to efficiently evaluate the expected database state.
"""

# Copyright (c) 2021, Aalborg University (pygrametl@cs.aau.dk)
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

import copy
import sqlite3
import collections
import pygrametl

__all__ = ['connectionwrapper', 'Table']


def connectionwrapper(connection=None):
    """Create a new connection wrapper for use with unit tests.

       Arguments:

       - connection: A PEP249 connection to use. If None (the default),
         a connection to a temporary SQLite in-memory database is created.
    """
    if not connection:
        connection = sqlite3.connect(":memory:")
        connection.execute("PRAGMA foreign_keys = ON;")
    # If the wrapper is not the default tests implicitly use two wrappers
    cw = pygrametl.ConnectionWrapper(connection)
    cw.setasdefault()
    return cw


class Table:
    """A class representing a concrete database table.

       Note that the asserts assume that the Table instance and the database
       table do not have duplicate rows, if they do the asserts raise an error.
    """
    __createdTables = collections.OrderedDict()

    def __init__(self, name, table, nullsubst='NULL', variableprefix='$',
                 loadFrom=None, testconnection=None):
        """Arguments:

           - name: the name of the table in the database.
           - table: the contents of the table as an ASCII drawing.
           - nullsubst: a string that represents NULL in the drawn table.
           - variableprefix: a string all variables must have as a prefix.
           - loadFrom: additional rows to be loaded as a path to a file that
             continues the table argument or as an iterable producing dicts.
           - testconnection: The connection wrapper to use for the unit tests.
             If None pygrametl's current default connection wrapper is used.
        """
        # If a ConnectionWrapper is not provided the default wrapper is used
        if testconnection is None:
            testconnection = pygrametl.getdefaulttargetconnection()
            if testconnection is None:
                raise ValueError("No target connection available")
        self.__testconnection = testconnection

        # If a file to load rows from is provided its content is a continuation
        # of the drawn table, so users control how to split the drawn table
        if type(loadFrom) is str:
            with open(loadFrom, 'r') as f:
                if not table.endswith('\n'):
                    table += '\n'
                table += f.read()

        # To simplify parsing the table is split and whitespace is ignored
        lines = table.strip().splitlines()

        # Simple verification of the tables structure
        delim = ['-', '|', ' ']
        if '|' not in table or ':' not in lines[0] or \
           (len(lines) > 1 and not all(ch in delim for ch in lines[1])) \
           or all(len(lines[0]) != len(line) for line in lines):
            table = '\n'.join(map(lambda l: l.strip(), table.split('\n')))
            raise ValueError("Malformed table\n{}".format(table))

        # Mapping of database types to Python types
        self.__casts = {
            'smallint': int,
            'int': int,
            'integer': int,
            'bigint': int,
            'decimal': int,
            'numeric': int,
            'real': float,
            'double precision': float,
            'char': str,
            'varchar': str,
            'text': str,
            'date': str,
            'timestamp': str,
        }

        # References to the variables are stored so they can be iterated over
        self.__variables = []

        # The header is parsed separately to extract the column names and types
        self.name = name
        self.__nullsubst = nullsubst
        self.__prefix = variableprefix
        (self.__keyrefs, self.attributes, self.__types,
         self.__localConstraints, self.__globalConstraints) = \
            self.__header(lines[0])
        self.__columns = self.__keyrefs + self.attributes
        self.__rows = []
        for line in lines[2:]:
            self.__rows.append(self.__row(line, True))

        # External rows from data sources are parsed to catch errors
        if loadFrom and type(loadFrom) is not str:
            for row in loadFrom:
                parsed = []
                for index, key in enumerate(self.__columns):
                    parsed.append(self.__parse(index, row[key], True))
                self.__rows.append(tuple(parsed))

        # The indexes of updated rows are stored so they can be returned
        self.__additions = set()

    # Public methods
    def __str__(self):
        """Return a string version of the table in the input format."""
        return self.__table2str(self.__rows, False, indention=0)

    def __iter__(self):
        """Return an iterator of the rows as dicts."""
        return map(lambda row: dict(zip(self.__columns, row)), self.__rows)

    def __add__(self, lines):
        """Create a new instance with the new rows provide appended.

           Arguments:

           - lines: a string representation of the rows to be added.
        """
        table = self.__copy()
        nextRow = len(self.__rows)

        # Ensure that the line is split despite the user not adding newlines
        lines = lines.replace('||', '|\n|')
        for index, line in enumerate(lines.split('\n')):
            row = table.__row(line, True)
            table.__rows.append(row)
            table.__additions.add(nextRow + index)
        return table

    def key(self):
        """Return the primary key.

           For a simple primary key, the name is returned. For a composite
           primary key, all names are returned in a tuple.
        """
        if not self.__keyrefs:
            raise ValueError("No primary key specified in this drawn table")

        if len(self.__keyrefs) == 1:
            return self.__keyrefs[0]
        else:
            return tuple(self.__keyrefs)

    def getSQLToCreate(self):
        """Return a string of SQL that creates the table."""
        sql = 'CREATE TABLE {}('.format(self.name) + ', '.join(
            [n + ' ' + t + c for n, t, c in
             zip(self.__columns, self.__types, self.__localConstraints)])

        # Add global constraints
        sql += ')' if not self.__globalConstraints else \
            self.__globalConstraints
        return sql

    def getSQLToInsert(self):
        """Return a string of SQL that inserts all rows into the table."""
        if self.__variables:
            raise ValueError(self.name + " contains variables")

        if not self.__rows:
            raise ValueError("No rows are specified in this drawn table")

        return 'INSERT INTO {}({}) VALUES'.\
            format(self.name, ', '.join(self.__columns)) + \
            ", ".join(map(self.__tuple2str, self.__rows))

    def assertEqual(self, verbose=True):
        """Return a Boolean indicating if the rows in this object and the rows
           in the database table match. If verbose=True an AssertionError is
           also raised if the rows don't match.

           Arguments:

           - verbose: if True an ASCII representation of the rows violating the
             assertion is printed as part of the AssertionError.
        """
        return self.__compareAndMaybeRaise(
            self.name + "'s rows differ from the rows in the database.",
            lambda rowSet, dbSet: rowSet == dbSet,
            lambda rowSet, dbSet: rowSet - dbSet,
            lambda rowSet, dbSet: dbSet - rowSet, True, verbose)

    def assertDisjoint(self, verbose=True):
        """Return a Boolean indicating if none of the rows in this object occur
           in the database table. If verbose=True an AssertionError is also
           raised if the row sets are not disjoint.

           Arguments:

           - verbose: if True an ASCII representation of the rows violating the
             assertion is printed as part of the AssertionError.
        """
        return self.__compareAndMaybeRaise(
            self.name + " and the database contain equivalent rows.",
            lambda rowSet, dbSet: len(rowSet & dbSet) == 0,
            lambda rowSet, dbSet: [],
            lambda rowSet, dbSet: rowSet & dbSet, True, verbose)

    def assertSubset(self, verbose=True):
        """Return a Boolean stating if the rows in this object are a subset of
           those in the database table. If verbose=True an AssertionError is
           also raised if the rows are not a subset of those in the database
           table.

           Arguments:

           - verbose: if True an ASCII representation of the rows violating the
             assertion is printed as part of the AssertionError.
        """
        return self.__compareAndMaybeRaise(
            self.name + "'s rows are not a subset of the database's rows",
            lambda rowSet, dbSet: rowSet <= dbSet,
            lambda rowSet, dbSet: rowSet - dbSet,
            lambda rowSet, dbSet: [], True, verbose)

    def create(self):
        """Create the table if it does not exist without adding any rows."""
        self.__testconnection.execute(self.getSQLToCreate())
        self.__testconnection.commit()
        type(self).__createdTables[self.name] = self

    @classmethod
    def clear(cls):
        """Drop all tables and variables without checking their contents."""
        # The latest table is dropped first as other tables cannot refer to it
        for name, table in reversed(cls.__createdTables.items()):
            try:
                # A table might not use the default connection wrapper
                table.__testconnection.execute('DROP TABLE ' + name)
            except Exception:
                # The exception raised for a missing table is driver dependent
                pass
        cls.__createdTables.clear()
        Variable.clear()

    def reset(self):
        """Forcefully create a new table and add the provided rows."""
        try:
            self.drop()
        except Exception:
            # The exception raised for a missing table depends on the driver
            pass
        self.ensure()

    def ensure(self):
        """Create the table if it does not exist, otherwise verify the rows.

           If the table does exist but does not contain the expected set of
           rows an exception is raised to prevent overriding existing data.
        """
        if self.__variables:
            raise ValueError(self.name + " contains variables")

        # Use a hack to check if the table is available in portable manner
        try:
            self.__testconnection.execute('SELECT 1 FROM ' + self.name)
        except Exception:
            # The exception raised for a missing table depends on the driver
            self.create()

            # If the table was drawn without any rows there are none to add
            if self.__rows:
                self.__testconnection.execute(self.getSQLToInsert())
                self.__testconnection.commit()
            return

        # If the table exists and contain the correct rows we just use it as is
        if not self.assertEqual(verbose=False):
            raise ValueError(self.name + " contain other rows")

    def update(self, index, line):
        """Create a new instance with the row specified by the index
           updated with the values included in the provided line.

           Arguments:

           - index: the index of the row to be updated.
           - line: an ASCII representation of the new values.
        """
        if index < len(self.__rows):
            table = self.__copy()
            newRow = table.__row(line, False)
            newRow = tuple(map(lambda tp: tp[1] if tp[1] else tp[0],
                               zip(self.__rows[index], newRow)))
            table.__rows[index] = newRow
            table.__additions.add(index)
            return table
        raise ValueError("{} index out of bounds {} > {}".
                         format(self.name, index, len(self.__rows)))

    def additions(self, withKey=False):
        """Return all rows added or updated since the original drawn table.

           Arguments:

           - withKey: if True the primary keys are included in the rows.
        """
        if withKey:
            return list(map(lambda i: dict(zip(
                self.__columns, self.__rows[i])), self.__additions))
        else:
            return list(map(lambda i: dict(zip(
                self.attributes, self.__rows[i][len(self.__keyrefs):])),
                self.__additions))

    def drop(self):
        """Drop the table in the database without checking the contents."""
        if self.name in type(self).__createdTables:
            self.__testconnection.execute('DROP TABLE ' + self.name)
            del type(self).__createdTables[self.name]
        else:
            raise ValueError(self.name + " is not created by a Table instance")

    # Private Methods
    def __header(self, line):
        """Parse the header of the drawn table."""
        keyrefs = []
        attributes = []
        types = []
        localConstraints = []
        globalConstraints = []
        afterKeyrefs = False
        for cs in [c.strip() for c in line.split('|') if c]:
            column = cs.split(':')
            if len(column) != 2:
                raise ValueError("Malformed column definition: " + cs)
            name = column[0].strip()
            primaryKey = False

            # Constraints are parsed first so primary keys can be extracted
            columnConstraints = []
            startOfConstraints = column[1].find('(')
            if startOfConstraints > -1:
                line = column[1][startOfConstraints + 1: -1]
                for constraint in line.split(','):
                    constraint = constraint.strip().lower()
                    if constraint == 'pk':
                        primaryKey = True
                    elif constraint == 'unique':
                        columnConstraints.append('UNIQUE')
                    elif constraint == 'not null':
                        columnConstraints.append('NOT NULL')
                    elif constraint.startswith('fk '):
                        reference = constraint.split(' ', 1)[1]
                        globalConstraints.append('FOREIGN KEY (' + name +
                                                 ') REFERENCES ' + reference)
                    else:
                        raise ValueError("Unknown constraint in {} for {}: {}"
                                         .format(self.name, name, constraint))
                column[1] = column[1][:startOfConstraints]
            localConstraints.append(' '.join(columnConstraints))

            # Primary keys must be listed in order and before other attributes
            if primaryKey:
                keyrefs.append(name)
                if afterKeyrefs:
                    raise ValueError("Primary key after other attributes: {}"
                                     + line)
            else:
                attributes.append(name)
                afterKeyrefs = True
            types.append(column[1].strip())

        # Formats both types of constraints for use with generated SQL
        localConstraints = list(map(lambda c: ' ' + c if c else c,
                                    localConstraints))
        if keyrefs:
            globalConstraints.insert(0, 'PRIMARY KEY (' +
                                     ', '.join(keyrefs) + ')')
            globalConstraints = ', ' + ', '.join(globalConstraints) + ')'
        return (keyrefs, attributes, types, localConstraints,
                globalConstraints)

    def __row(self, line, castempty):
        """ Parse a row in the drawn table.

            Casting of missing values can be toggled so this method
            can parse both full rows, new rows, and rows with updates.
        """
        result = []
        values = line.strip().split('|')[1:-1]
        for index, value in enumerate(values):
            result.append(self.__parse(index, value.strip(), castempty))
        return tuple(result)

    def __parse(self, index, value, castempty):
        """ Parse a field in the drawn table.

            Casting of missing values can be toggled so this method
            can be used when parsing full, new, and updated rows.
        """
        if type(value) is Variable:
            self.__variables.append(value)
            return value

        if type(value) is str:
            if value.startswith(self.__prefix):
                variable = Variable(value, self.__prefix, self.name, len(
                    self.__rows), index, self.__columns[index])
                self.__variables.append(variable)
                return variable

        baseType = self.__types[index].split('(', 1)[0].lower()
        cast = self.__casts[baseType]
        if value == self.__nullsubst:
            value = None
        elif value or castempty:
            value = cast(value)
        return value

    def __compareAndMaybeRaise(self, why, comparison, selfViolations,
                               dbViolations, shouldRaise, verbose):
        """Compare this table to the table in the database, and raise an
           AssertionError if asked by the caller as it already has all of the
           required information.

           Arguments:

           - why: a short description of why the assertion is violated.
           - comparison: a function that takes the rows in self and the rows in
             the database as input and then computes if the assertion holds.
           - selfViolations: a function that takes the rows in self and the
             database as input and returns those in self violating the assert.
           - dbViolations: a function taking the rows in self and the database
             as input and returns those in the database violating the assert.
           - shouldRaise: if True the function raises an error if the assertion
             does not hold instead of simply returning the value False.
           - verbose: if True an ASCII representation of the rows violating the
             assertion is printed when an error is raised instead of only why.
        """
        # Variables are always resolved to ensure they match the database
        for variable in self.__variables:
            self.__resolve(variable)

        # Sets are used for performance and to simplify having multiple asserts
        rowSet = set(self.__rows)
        if len(self.__rows) != len(rowSet):
            raise ValueError("The '{}' table instance contains duplicate rows"
                             .format(self.name))

        self.__testconnection.execute(
            'SELECT {} FROM {}'.format(', '.join(self.__columns), self.name))
        dbRows = list(self.__testconnection.fetchalltuples())  # Generator
        dbSet = set(dbRows)
        if len(dbRows) != len(dbSet):
            raise ValueError("The '{}' database table contains duplicate rows"
                             .format(self.name))

        success = comparison(rowSet, dbSet)
        if not success and shouldRaise:
            if not verbose:
                raise AssertionError(why)
            else:
                selfStr = self.__table2str(rowSet, False)
                dbStr = self.__table2str(dbSet, False)
                violations = list(selfViolations(rowSet, dbSet)) + \
                    [()] + list(dbViolations(rowSet, dbSet))
                vStr = self.__table2str(violations, True)
                raise AssertionError((why +
                                      "\nDrawn Table:\n{}"
                                      "\n\nDatabase Table:\n{}"
                                      "\n\nViolations:\n{}")
                                     .format(selfStr, dbStr, vStr))
        return success

    def __resolve(self, variable):
        """Ensure the value of the variable is resolved."""
        row = filter(lambda t: type(t[1]) is not Variable,
                     zip(self.__columns, self.__rows[variable.row]))
        query = "SELECT " + ", ".join(self.__columns) + " FROM " + self.name \
            + " WHERE " + " AND ".join([self.__pair2equals(p) for p in row])
        self.__testconnection.execute(query)
        dbRows = list(self.__testconnection.fetchalltuples())
        if len(dbRows) != 1:
            raise ValueError("No unambigiuous value for the variable {} in {}"
                             .format(variable.name, self.name))
        variable.set(dbRows[0][variable.column])

    def __pair2equals(self, columnAndValue):
        """Create an SQL string that checks if a column is equal to a value."""
        if columnAndValue[1] is None:
            return columnAndValue[0] + " IS NULL"
        else:
            return columnAndValue[0] + " = '{}'".format(columnAndValue[1])

    def __table2str(self, rows, violation, indention=2):
        """Format a table as a string."""
        # Determine the longest value of each column for formatting with (pk)
        header = list(map(lambda tp: tp[0] + ':' + tp[1],
                          zip(self.__columns, self.__types)))
        for i, _ in enumerate(self.__keyrefs):
            header[i] += ' (pk)'
        widths = list(map(len, header))
        for row in rows:
            for i, value in enumerate(row):
                widths[i] = max(widths[i], len(str(value)))

        # Format table with the column width determined by the widest cell
        prefix = indention * ' '
        fs = ('{{}}' + ('| {{: <{}}} ' * len(widths)) + '|').format(*widths)
        header = fs.format(prefix, *header)
        delimiter = fs.format(prefix, *map(lambda w: w * '-', widths))
        rows = list(map(lambda r: tuple(map(lambda v: 'NULL' if v is None
                                            else v, r)), rows))

        # The rows are formatted and a prefix is added to the rows violating
        # the assert. Expected rows are marked with an E while rows currently
        # in the database are marked with a D. All other rows have no prefix.
        violationPrefix = (indention - 2) * ' '
        prefix = violationPrefix + 'E ' if violation else indention * ' '
        for i, row in enumerate(rows):
            if violation and not row:
                prefix = violationPrefix + '  '
                rows[i] = fs.format(prefix,  *(('',) * len(self.__columns)))
                prefix = violationPrefix + 'D '
            else:
                rows[i] = fs.format(prefix, *row)
        return '\n'.join([header, delimiter] + rows)

    def __copy(self):
        """Make a deep copy of the table with a reference to the connection."""
        # HACK: a database connection cannot be copied so it is shared
        testconnection = self.__testconnection
        self.__testconnection = None
        table = copy.deepcopy(self)
        self.__testconnection = testconnection
        table.__testconnection = testconnection
        return table

    def __tuple2str(self, t):
        """Format a tuple as a string for use in SQL."""
        return "(" + ", ".join(map(self.__element2str, t)) + ")"

    def __element2str(self, e):
        """Format a value as a string for use in SQL."""
        if e is None:
            return 'NULL'
        elif type(e) is str:
            return "'" + e + "'"
        else:
            return str(e)


class Variable:
    __all = {}

    def __init__(self, definition, prefix, origin, row, column, column_name):
        self.name = definition[len(prefix):]
        self.definition = definition
        self.origin = origin

        # Both are stored with zero being the first element of the index Table
        self.row = row
        self.column = column
        self.column_name = column_name

        # A placeholder value is set to allow printing a variable without value
        self.value = ''

    def __hash__(self):
        # The values for variables defined as _ and __ are not stored in __all
        return hash(self.value)

    def __eq__(self, other):
        return self.value == other

    def __format__(self, format_spec):
        return format("{}({})"
                      .format(self.definition, self.value), format_spec)

    def __str__(self):
        return str(self.value)

    @classmethod
    def clear(cls):
        cls.__all.clear()

    def set(self, value):
        self.value = value

        # The values of underscore variables are ignored
        if self.name == '_':
            return

        # The value of variables named _! cannot be NULL
        if self.name == '_!':
            if value is None:
                raise AssertionError("Expected a NOT NULL value in {}(row {},"
                                     " column {} {}), found NULL in database"
                                     .format(self.origin, self.row,
                                             self.column, self.column_name))
            return

        # All variables with the same definition must also have the same values
        if self.definition in type(self).__all:
            existing = type(self).__all[self.definition]
            if not existing.value == value:
                raise AssertionError(("Ambiguous values for {}; {}(row {}, "
                                      "column {} {}) is {} and {}(row {}, "
                                      "column {} {}) is {}").format(
                                          self.definition, existing.origin,
                                          existing.row, existing.column,
                                          existing.column_name, existing.value,
                                          self.origin, self.row, self.column,
                                          self.column_name, self.value))
        else:
            type(self).__all[self.definition] = self
