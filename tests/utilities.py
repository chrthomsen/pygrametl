"""Shared functionality used by the unit tests"""

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

import os
import sys
import locale
import string
import random
import pygrametl


# Helper functions so options and transformations are used consistently
def get_os_encoding():
    """Gets the OS's encoding in the same manner as open() so they match"""
    # https://docs.python.org/3/library/functions.html#open
    return locale.getpreferredencoding(False)


# Functions for creating a connection to a specific database for testing
def select_connection(connection_module_name):
    """Sets connection creation function to use in unit tests."""
    global get_connection
    get_connection = globals()[connection_module_name + '_connection']


def get_connection():
    """Returns a new connection to the currently selected RDBMS."""
    # The unit tests defaults to SQLite as it has no dependencies
    return sqlite3_connection()


def ensure_default_connection_wrapper():
    """Ensures the default connection wrapper is ready for unit tests."""
    cw = pygrametl.getdefaulttargetconnection()
    try:
        cw.rollback()
    except Exception:
        # If cw is None or the connection is closed a new one is created
        global get_connection
        cw = pygrametl.ConnectionWrapper(get_connection())
        cw.setasdefault()

    # The database should be in a known good state before each test
    pygrametl.drawntabletesting.Table.clear()
    return cw


def sqlite3_connection():
    """Create a new sqlite3 connection for use with unit tests."""
    import sqlite3
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def psycopg2_connection():
    """Create a new psycopg2 connection for use with unit tests."""
    import psycopg2
    connection = psycopg2.connect(
        os.environ['PYGRAMETL_TEST_DATABASE_CONNECTIONSTRING'])
    return connection


# Experimental support for property-based testing without requiring Hypothesis
def get_dict_row_generator(table, types=None, count=None):
    try:
        cursor = table.targetconnection.cursor()
        atts = table.all
    except AttributeError:
        # HACK: BulkFactTable have no targetconnection or all attributes
        cursor = pygrametl.getdefaulttargetconnection().cursor()
        atts = table.atts
    cursor.execute(f"SELECT {','.join(atts)} FROM {table.name}")
    cursor.close()  # Cursor does not delete description when closed
    names = list(map(lambda column: column[0], cursor.description))
    if not types:
        # For some RDBMSes the types are returned with the result
        types = list(map(lambda column: column[1], cursor.description))
        if not all(types):
            raise ValueError("The RDBMs did not provide types for all column")
    generators = list(map(lambda ct: __get_value_generator(ct), types))

    def get_row(count):
        if not count:
            count = -1

        rows = 0
        while rows != count:
            row = {}
            for n, g in zip(names, generators):
                row[n] = g()
            rows += 1
            yield row
    return get_row(count)


def get_str_row_generator(table, types=None, count=None):
    generator = get_dict_row_generator(table, types, count)
    return map(lambda row: '|' + '|'.join(str(value) for value in row.values())
               + '|', generator)


def __get_value_generator(column_type):
    if column_type == "INTEGER":
        return lambda: random.randint(1, sys.maxsize)
    elif column_type == "TEXT":
        return lambda: ''.join(random.choice(string.ascii_letters)
                               for i in range(random.randint(1, 50)))
    else:
        raise ValueError("Unknown column type " + str(column_type))
