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
from enum import Enum

import pygrametl


class ConnectionType(Enum):
    SQLITE = 1
    PSYCOPG2 = 2


def get_os_encoding():
    """Get the OS's encoding in the same manner as open() so they match"""
    # https://docs.python.org/3/library/functions.html#open
    return locale.getpreferredencoding(False)


def set_connection(connection_type):
    """Set connection creation function to use in unit tests."""
    global get_connection
    if connection_type == ConnectionType.SQLITE:
        get_connection = __sqlite3_connection
    elif connection_type == ConnectionType.PSYCOPG2:
        get_connection = __psycopg2_connection
    else:
        ValueError("connection_type must be an instance of ConnectionType")


def get_connection():
    """Returns a new connection to the currently selected type."""
    # The unit tests defaults to SQLite as it has no dependencies
    return __sqlite3_connection()


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


def __sqlite3_connection():
    """Create a new sqlite3 connection for use with unit tests."""
    import sqlite3
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def __psycopg2_connection():
    """Create a new psycopg2 connection for use with unit tests."""
    import psycopg2
    connection_string = os.environ['PYGRAMETL_TEST_DATABASE_CONNECTIONSTRING']
    connection = psycopg2.connect(connection_string)
    return connection
