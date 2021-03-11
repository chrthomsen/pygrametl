"""This module is a test runner for tests defined using drawn tables."""

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

import os
import sys
import csv
import shlex
import types
import sqlite3
import argparse
from collections import namedtuple
from pathlib import Path

import pygrametl.drawntabletesting as dtt


# Types
ReaderError = namedtuple('ReaderError', 'path start end name cause')
PreCondition = namedtuple('PreCondition', 'path start end table')
PostCondition = namedtuple('PostCondition', 'path start end table assert_name')


class ExtendAction(argparse.Action):
    """Creates a list of arguments passed to a flag instead of one per flag."""

    def __call__(self, parser, namespace, values, option_string=None):
        items = getattr(namespace, self.dest) or []
        items.extend(values)
        setattr(namespace, self.dest, items)


# Constants
DEFAULT_CONNECTION_NAME = 'connection'


# Functions
def print_reason_for_failure(when, condition, reason):
    print("[{} {}({}-{})] {}".format(when, condition.path, condition.start,
                                     condition.end, reason), end='\n')


def print_reader_error(path, firstlinenumber, lastlinenumber, reader_name, e):
    reader_error = ReaderError(
        path, firstlinenumber, lastlinenumber, reader_name, e)
    reason = reader_error.name + ' - ' + str(reader_error.cause)
    print_reason_for_failure("(Reader)", reader_error, reason)


def read_csv(columns, path, delimiter):
    with open(path) as f:
        return list(csv.DictReader(f, fieldnames=columns, delimiter=delimiter))


def read_sql(columns, config, *arguments):
    if 'SELECT' == arguments[0]:
        connection = DEFAULT_CONNECTION_NAME
        query = ' '.join(arguments)
    else:
        connection = arguments[0]
        query = ' '.join(arguments[1:])
    connection = getattr(config, connection)

    # DTT expects a sequence of dicts with the column names as keys
    cursor = connection.cursor()
    cursor.execute(query)
    rows = map(lambda row: dict(zip(columns, row)), cursor.fetchall())
    cursor.close()
    return rows


def read_dt(path, dt, lastlinenumber, pre_dtts, post_dtts, config, nullsubst,
            variableprefix, connection_wrappers, pre_conditions,
            post_conditions):
    header = list(map(lambda s: s.strip(), dt[0].split(',')))
    firstlinenumber = lastlinenumber - len(dt) + 1

    # If the last line does not start with a pipe it cannot be a DT column and
    # must instead be a data source with the column names defined by the DT
    loadFrom = None
    if '|' != dt[-1].strip()[0]:
        columns = [c.split(':')[0].strip() for c in dt[1].split('|') if c]
        (reader_name, *arguments) = shlex.split(dt[-1])  # Splits as POSIX SH
        try:
            if reader_name == 'csv':
                reader_function = read_csv
            elif reader_name == 'sql':
                reader_function = read_sql
                arguments.insert(0, config)
            else:
                reader_function = getattr(config, reader_name)
            arguments.insert(0, columns)
            loadFrom = reader_function(*arguments)
            dt = dt[:-1]  # The external data source should be passed to Table
        except Exception as e:
            # Errors are caught so the test runner is not terminated
            print_reader_error(path, firstlinenumber, lastlinenumber,
                               str(reader_name), e)
            return

    # If the user has not given a connection the default is used
    connection = DEFAULT_CONNECTION_NAME
    if '@' in header[0]:
        (header[0], connection) = header[0].split('@')

    # Ensures a connection to the test data is available and creates the table
    try:
        # Only one ConnectionWrapper should be created per connection
        if connection not in connection_wrappers:
            connection_wrappers[connection] = \
                dtt.connectionwrapper(getattr(config, connection))
        table = dtt.Table(name=header[0], table='\n'.join(dt[1:]),
                          nullsubst=nullsubst, variableprefix=variableprefix,
                          loadFrom=loadFrom,
                          testconnection=connection_wrappers[connection])
    except Exception as e:
        # Errors are caught so the test runner is not terminated
        print_reader_error(path, firstlinenumber, lastlinenumber,
                           str(reader_name), e)
        return

    # Only postconditions include an assert
    if len(header) == 1 and path in pre_dtts:
        pre_conditions.append(PreCondition(path, firstlinenumber,
                                           lastlinenumber, table))
    elif len(header) == 2 and path in post_dtts:
        post_conditions.append(PostCondition(path, firstlinenumber,
                                             lastlinenumber, table,
                                             header[1].capitalize()))


def read_dtt_file(path, pre_dtts, post_dtts, config, nullsubst, variableprefix,
                  connection_wrappers, pre_conditions, post_conditions):
    linenumber = 0
    with open(path, 'r') as f:
        dt = []
        for line in f:
            line = line.strip()
            linenumber += 1
            # Lines with content are accumulated
            if line:
                dt.append(line)
            # Empty lines separate DTs in the file
            elif dt:
                read_dt(path, dt, linenumber - 1, pre_dtts, post_dtts, config,
                        nullsubst, variableprefix, connection_wrappers,
                        pre_conditions, post_conditions)
                dt = []

    # Reads the last DT if the file not end with an empty line
    if dt:
        read_dt(path, dt, linenumber - 1, pre_dtts, post_dtts, config,
                nullsubst, variableprefix, connection_wrappers,
                pre_conditions, post_conditions)


def ensure_pre_condition(pre_condition):
    # Executes the ensure method without terminating if it fails
    try:
        pre_condition.table.ensure()
    except Exception as e:
        print_reason_for_failure("(Pre)", pre_condition, str(e))


def assert_post_condition(post_condition):
    # Executes the assert method without terminating if it fails
    try:
        getattr(post_condition.table, "assert" + post_condition.assert_name)()
    except AttributeError:
        raise ValueError("Unsupported assert specified")
    except Exception as e:
        print_reason_for_failure("(Post)", post_condition, str(e))


def usage(parser, verbose):
    print("usage: " + Path(sys.argv[0]).stem + " [-" + "".join(map(
        lambda a: a.option_strings[0][1:], parser._actions)) + "]", end="\n\n")

    if verbose:
        print("Run tests specified in .dtt files.\n")
        for action in parser._actions:
            print(", ".join(action.option_strings), end="\t\t")
            if action.metavar:
                print(action.metavar, end="\t")
            else:
                print("", end='\t')
            print(action.help, end="")
            print()
    sys.exit(1)


def parse_arguments():
    parser = argparse.ArgumentParser(add_help=False)
    parser.register('action', 'extend', ExtendAction)

    # HACK: Correcting tab characters are inserted here for alignment
    parser.add_argument('-e', '--etl', action='extend', nargs='+',
                        metavar="ETL [ARGS...]",
                        help="run the command ETL with the arguments ARGS")
    parser.add_argument('-f', '--files', action='extend', nargs='+',
                        metavar="FILES...",
                        help="use only the conditions specified in FILES")
    parser.add_argument('-h', '--help', action='store_true',
                        help="\tshow this help message and exit")
    parser.add_argument('-n', '--null', action='store', metavar="STRING",
                        help="\tuse STRING to represent NULL (default: NULL)")
    parser.add_argument('-p', '--pre', action='extend', nargs='+',
                        metavar="FILES...",
                        help="use only the preconditions specified in FILES")
    parser.add_argument('-P', '--post', action='extend', nargs='+',
                        metavar="FILES...",
                        help="use only the postconditions specified in FILES")
    parser.add_argument('-r', '--recursion-off', action='store_true',
                        help="execute only the tests in cwd and not sub-folders")
    parser.add_argument('-v', '--varprefix', action='store', metavar="STRING",
                        help="\tuse STRING as prefix for variables (default: $)")

    args, failed = parser.parse_known_args()
    if args.help:
        usage(parser, True)

    if failed:
        usage(parser, False)
    return args


# Main
def main():
    args = parse_arguments()

    # Ensures that the expected config.py file is loaded
    try:
        sys.path.insert(0, os.getcwd())
        import config  # Must specify a PEP 249 connection named 'connection'
        del(sys.path[0])
    except ImportError:
        config = types.ModuleType('config')
        config.connection = sqlite3.connect(':memory:')

    # Reads only the DTT files required to execute the tests, the arguments
    # to Table is always given to ensure the defaults in dttr.py is used
    dtts = list(map(lambda p: str(p), Path(os.getcwd()).glob('*.dtt') if
                    args.recursion_off else Path(os.getcwd()).rglob('*.dtt')))
    if args.pre and args.post:
        paths = set(args.pre + args.post)
        dtts = filter(lambda path: str(path) in paths, dtts)
    pre_dtts = set(args.pre if args.pre else dtts)
    post_dtts = set(args.post if args.post else dtts)
    nullsubst = args.null if args.null else 'NULL'
    variableprefix = args.varprefix if args.varprefix else '$'
    connection_wrappers = {}
    pre_conditions = []
    post_conditions = []
    for dtt_path in dtts:
        read_dtt_file(dtt_path, pre_dtts, post_dtts, config, nullsubst,
                      variableprefix, connection_wrappers, pre_conditions,
                      post_conditions)

    # Ensures all preconditions can setup state for the tests
    for pre_condition in pre_conditions:
        ensure_pre_condition(pre_condition)

    # Executes the ETL flow to load data into the test warehouse
    if args.etl:
        os.system(' '.join(args.etl))

    # Checks that postconditions are met after the ETL flow is run
    for post_condition in post_conditions:
        assert_post_condition(post_condition)


if __name__ == '__main__':
    main()
