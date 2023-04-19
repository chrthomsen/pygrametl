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
import locale
import pygrametl


def get_os_encoding():
    """Get the OS's encoding in the same manner as open() so they match"""
    # https://docs.python.org/3/library/functions.html#open
    return locale.getpreferredencoding(False)

def get_connection():
    """Returns a new connection to the selected test database."""

    # The unit tests defaults to SQLite as it has no dependencies
    connection_type_and_string = \
        os.environ.get('PYGRAMETL_TEST_CONNECTIONSTRING', 'sqlite://:memory:')

    # Select the
    connection_type, connection_string = \
        connection_type_and_string.split('://')

    if connection_type == 'sqlite':
        return __sqlite3_connection(connection_string)
    elif connection_type == 'psycopg2':
        return __psycopg2_connection(connection_string)
    else:
        raise ValueError(
            'Expected sqlite:// or psycopg2:// and a connection string')


def ensure_default_connection_wrapper():
    """Ensure the default connection wrapper is ready for the next test."""
    connection_wrapper = pygrametl.getdefaulttargetconnection()
    try:
        connection_wrapper.rollback()
    except Exception:
        # The connection is closed so a new one is created
        global get_connection
        connection_wrapper = pygrametl.ConnectionWrapper(get_connection())
        connection_wrapper.setasdefault()

    # The database must be in a known good state before each test
    pygrametl.drawntabletesting.Table.clear()
    return connection_wrapper


def __sqlite3_connection(connection_string):
    """Create a new sqlite3 connection for use with unit tests."""
    import sqlite3
    connection = sqlite3.connect(connection_string)
    connection.execute('PRAGMA foreign_keys = ON;')
    return connection


def __psycopg2_connection(connection_string):
    """Create a new psycopg2 connection for use with unit tests."""
    import psycopg2
    return psycopg2.connect(connection_string)
