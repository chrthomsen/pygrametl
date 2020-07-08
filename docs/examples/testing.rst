.. _testing:

Testing
========
*pygrametl* contains multiple abstractions to simplify testing.


Drawn Table Testing
-------------------
The DTT framework is implemented in Python, but this *does not* imply that the
user necessarily must implement the ETL flow itself with pygrametl or in
Python – the ETL flow can be implemented using any programming language or
program, including GUI-based ETL tools. In this section, we describe how DTT
can be used as a library, i.e., together with user-written Python code such as
unit tests. Later, we show how DTT can be used as a stand-alone tool that
provides the same functionality without requiring the users to implement their
tests using Python code.


Drawn Tables with the ``Table`` Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Drawn Table functionality is implemented by the ``Table`` class.
When an instance is created, a name for the table must be given as well
as a string with the Drawn Table. Further, a string value representing
``NULL`` can optionally be given as well as a prefix to be used for
variables (explained later). The Drawn Table is then parsed by the
following rules: The first row (called the "header") contains
``name:type`` pairs for each column with each pair surrounded by
vertical pipes. After a type, ``(pk)`` can be specified to make a column
(part of) the primary key. ``UNIQUE`` and ``NOT NULL`` are also
supported and must be defined in the same manner as a primary key. If
multiple constraints are defined for one column, they must be separated
by a comma. Foreign keys are also supported and will be explained later.
A valid header is, e.g., ``| bid:int (pk) | browser:text | os:text |``.
If the table should hold any data, the header must be followed by a
delimiter line only with vertical pipes, spaces, and dashes
(``| -- | -- |``) and then each row follows on a line of its own. Columns
must be given in the same order as in the header and must be separated
by pipes. For string values, any surrounding spaces are trimmed away. A
Drawn Table is also a valid table in GitHub Flavored Markdown. An
example is given below.

::

    tbl = Table("browser", """
    | bid:int (pk) | browser:text | os:text |
    | ------------ | ------------ | ------- |
    | -1           | Unknown      | Unknown |
    | 1            | Firefox      | Linux   |
    | 2            | Chrome       | MacOS   |
    | 3            | Safari       | MacOS   |""")
    tbl.ensure()

Alternatively, a Drawn Table’s rows can be loaded from an external
source by providing either a path to a file containing a Drawn Table
without header or an iterable Python object yielding ``dict``\ s mapping
from column names to values to the constructor’s ``loadFrom`` parameter.
Data can thus be loaded from files, databases, etc. at the cost of the
unit test not being self-contained.

After a ``Table`` instance is created, its ``ensure()`` method can be
invoked. This will determine if a table with the same name and rows
exists in the test database and otherwise create it (or raise an error
if it contains other rows). If the user wants to create and fill the
table even if it already exists, the ``reset()`` method is provided. If
the user wants to create the table, but not insert any data into it, the
``create`` method can be used. Finally, if the user prefers to control
the creation of the table and insertion of the data specified by the
Drawn Table, the methods ``getSQLToCreate()`` and ``getSQLToInsert()``
are provided. By default, DTT uses an in-memory SQLite database to run
all tests against as it is very fast and does not require any
installation or configuration by the user. It is thus a good choice to
use for testing ETL flows during development. If running the test suite
is too time-consuming to run frequently, testing will be neglected and
problems become harder to correct as they accumulate and additional
changes are performed.
The user can, however, optionally use any other RDBMS with a PEP 249
connector (e.g., for a staging environment or continuous integration).

Users can, of course, represent multiple different tables in the
database using multiple instances of ``Table``. In such situations,
foreign keys constraints are often required. In DTT, foreign keys are
defined in the same manner as the other constraints and require that
users specify ``fk target(att)`` where ``target`` is the name of the
referenced table and ``att`` is the referenced column. An example using
foreign keys to connect ``page`` and ``domain`` can be seen below. All
constraints are enforced by the RDMBS managing the test database.

::

    page = dtt.Table("page", """
    | pid:int (pk) | url:text    | did:int (fk domain(did)) |
    | ------------ | ----------- | ------------------------ |
    | 1            | www.aau.dk/ | 1                        |
    | 2            | www.ufm.dk/ | 2                        |""")

    domain = dtt.Table("domain", """
    | did:int (pk) | domain:text |
    | ------------ | ----------- |
    | 1            | aau.dk      |
    | 2            | ufm.dk      |""")

``Table`` instances are immutable once created. Typically, the
postcondition is, however, similar to the precondition except for a few
added or updated rows. Therefore, we have made it simple to create a new
``Table`` instance from an existing one: We support the ``+`` operator
such that new rows simply can be added as in
``newtbl = tbl + | 2 | Chrome | Windows | + | 3 | Opera | Linux |``. A
new instance is also created when one of the rows is updated. This is
done by calling the ``update(index, newrow)`` method. For example, the
first row in ``tbl`` can be changed with the line
``newtbl2 = tbl.update(0, |-1|Unknown|N/A|)``. Note that a new instance
of ``Table`` is not represented in the test database unless its
``ensure()`` method is invoked. By making ``Table`` instances immutable
and creating new instances when they are modified, the user can very
easily reuse the ``Table`` instance representing the precondition for
multiple tests, and then as part of each test create a new instance with
the postcondition based on it. After a number of additions and/or
updates, it can be useful to get all modified rows. The user can then,
e.g., make a test case where the ETL flow is executed for the new rows
as shown below.

::

    def test_canInsertIntoBrowserDimensionTable(self):
        expected = tbl + "| 2 | Chrome | Windows |" \ 
                       + "| 3 | Opera  | Linux |"
        newrows = expected.changes()
        etl.executeETLFlow(newrows) 
        expected.assertEqual()

For the code above, ``expected`` defines how the user
expects the database state to become, but it is not the DTT framework
that puts the database in this state. The database is modified by the
user’s own ETL flow invoked on Line 5 and this may be implemented in
Python, another programming language, or any other program. Using these
features, DTT makes it simple to define the initial state of a database
before a test is executed and the rows the ETL flow should load.
However, for the automatic test to be of any use, it is necessary to
validate that the state of the database after the ETL flow has finished.
This is done using assertions (Line 6) which we will now describe.


Assertions
~~~~~~~~~~
DTT offers multiple assertions to check the state of a database table.
At the moment, the methods ``assertEqual(verbose=True)``,
``assertDisjoint(verbose= True)``, and ``assertSubset(verbose=True)``
are implemented in the DTT framework. When the
``assertEqual(verbose=True)`` method is called as shown in
above, DTT verifies that the table in the test database
contains the expected rows (and only those) and if not, it raises an
``AssertionError`` and provides an easy-to-read explanation of why the
test failed as shown here.

::

    AssertionError: browser's rows differ from the rows in the database.
    Expected Table:
      | bid:int (pk) | browser:text | os:text |
      | ------------ | ------------ | ------- |
      | -1           | Unknown      | Unknown |
      | 1            | Firefox      | Linux   |
      | 2            | Firefox      | MacOS   |
      | 3            | Firefox      | Windows |

    Database Table:
      | bid:int (pk) | browser:text | os:text |
      | ------------ | ------------ | ------- |
      | 2            | Firefox      | MacOS   |
      | 3            | Firefox      | Linux   |
      | 1            | Firefox      | Linux   |
      | -1           | Unknown      | Unknown |

    Violations:
      | bid:int (pk) | browser:text | os:text |
      | ------------ | ------------ | ------- |
    E | 3            | Firefox      | Windows |
      |              |              |         |
    D | 3            | Firefox      | Linux   |

In this example, the part of the user’s ETL flow loading the ``browser``
table contains a bug. The ``Table`` instance in the test specifies that
the dimension should contain a row for unknown browsers and operating
systems and three rows for Firefox on different operating systems (see
the expected state in the top of the output). However, the user’s ETL
code added ``Firefox`` on ``Linux`` a second time instead of ``Firefox``
on ``Windows`` (see the middle table in the output). To help the user
quickly identify exactly what rows do not match, DTT prints the rows
violating the assertion which for equality is the difference between the
two relations (bottom). The expected rows (i.e., those in the ``Table``
instance) are prefixed by an ``E`` and the rows in the database table
are prefixed by a ``D``. The detailed information provided by
``assertEqual(verbose=True)`` can be disabled, by setting the optional
parameter ``verbose`` to ``False``. Note that the orders of the rows are
allowed to differ between the Drawn Table and the database table without
causing the test to fail.

When ``assertDisjoint`` is called on a ``Table`` instance, it is
asserted that none of the ``Table``\ ’s rows are present in the database
table. In this way it is also possible to assert that something *is not*
in the database table, e.g., to test a filter or to check for the
absence of erroneous rows that previously fixed bugs wrongly added. When
``assertSubset`` is called, it is asserted that all the ``Table``\ ’s
rows are present in the database table which, however, may contain more
rows which the user then does not have to specify. ``assertSubset``
makes it easy to define a sample of rows that can be compared to a table
with so many rows that they cannot be effectively embedded in the test
self. For example, it can then easily be tested if the leap day
2020-02-29 exists in the ``time`` dimension.

When compared to a table in the database, a ``Table`` instance does not
have to contain all of database table’s columns. Only the state of the
included columns will then be compared. This is useful for excluding
columns for which the user does not know the state or which do not
matter in the test, like an automatically generated primary key or audit
information such as a timestamp.



Variables
~~~~~~~~~
Cases can also occur where it is important that specific cells are equal
across different database tables, but the exact values are unknown or do
not matter. A prominent example is when foreign keys are used. In DTT
this is easy to state using *variables*. A variable has a name prefixed
by $ (the prefix is user-configurable) and can be used in any cell of a
Drawn Table. The DTT framework then checks if the cells with the same
variable contain the same actual value in the database and fails the
test if not. The code snippet below shows an example of how to use
variables to test that foreign keys are assigned correctly.

::

    page = dtt.Table("page", """"
    | pid:int (pk) | url:text    | did:int (fk domain(did)) |
    | ------------ | ----------- | ------------------------ |
    | 1            | www.aau.dk/ | $1                       |
    | 2            | www.ufm.dk/ | $2                       |""")

    domain = dtt.Table("domain", """
    | did:int (pk) | domain:text |
    | ------------ | ----------- |
    | $1           | aau.dk      |
    | $2           | ufm.dk      |""")

Here the user has stated that the ``did`` for ``www.aau.dk/`` in ``page``
must match the ``did`` for ``aau.dk`` in ``domain`` and likewise for
``ufm.dk``. If variables with the same name do not have matching values,
DTT raises errors.

::

    ...
    ValueError: Ambiguous values for $1: page(0,2) is 1 and domain(0,0) is 2
    ...
    ValueError: Ambiguous values for $2: page(1,2) is 2 and domain(1,0) is 1

These error messages are excerpts from the output of a test case where
``page`` and ``domain`` had the IDs defined in two different orders. As
such, the foreign key constraints were satisfied although
``www.aau.dk/`` was referencing the domain ``ufm.dk``. This demonstrates
that variables can test parts of the ETL flow which cannot be verified
by foreign keys which just ensure that a value is present.

Another example of using variables is shown below. Here the user verifies
that in a type-2 Slowly Changing Dimension, the
timestamp set for ``validto`` matches ``validfrom`` for the new version of the
member. Thus, variables allow users to efficiently test generated values without
knowing their value.

::

    page = dtt.Table("page", """"
    | url:text    | validfrom:date | validto:date  |
    | ----------- | -------------- | ------------- |
    | www.aau.dk/ | 2019-06-01     | $1            |
    | www.aau.dk/ | $1             | NULL          |""")

It is also possible to specify that the value of a cell should not be
included in the comparison. This is done with a special variable ``$_``.
When compared to any value, ``$_`` is always considered to be equal. An
example is shown in the code below where the actual value of the
primary key of the expected new row is not taken into consideration.
``$_!`` is a stricter version of ``$_`` which disallows ``NULL``.

::

    domain = dtt.Table("domain", """
    | did:int (pk)  | domain:text |
    | ------------- | ----------- |
    | 1             | aau.dk      |
    | 2             | ufm.dk      |""")
    domain.ensure()
    etl.executeETLFLow()
    expected = domain + "| $_ | python.org |"

The methods ``ensure()`` and ``reset()`` may not be called on a Drawn
Table where any variables are used (this will raise an error). This
effectively means that variables only can be used when the postcondition
is specified. The reason is of course that DTT does not know which
concrete values to insert into the database for variables if they are
used in preconditions.



Tooling Support
~~~~~~~~~~~~~~~
A key benefit of DTT is the ability for users to effectively understand
the preconditions and postconditions of a test due to the visual
representation provided by . However, to gain the full benefit of ,
their columns should be aligned across rows as their content otherwise
becomes much more difficult to read. A very poorly formatted Drawn Table
can be seen below.

::

    | bid:int (pk) | browser:text    | os:text   |
    |-----
    | 1 | Firefox         | Linux     |
    | 2     | Firefox         | Windows     |
    | 3 | Firefox         | MacOS |
    | 4     | Chrome | Linux     |
    | 5 | Chrome | Windows     |
    | 6    | Chrome | MacOS |
    | -1 | Unknown browser | Unknown   |

It is clear from this example that poor formatting makes a Drawn Table
harder to read. However, as properly formatting each Drawn Table can be
tedious, our framework provides the script ``formattable.py`` for doing this
automatically. 
The script is designed to be interfaced with extensible text editors so
users can format a Drawn Table simply placing the cursor anywhere on a
Drawn Table and executing the script. Integrating the script with the
popular editors Emacs and Vim requires only a few lines of Elisp and
Vimscript, respectively.

An automatically formatted version of the Drawn Table from
above can be seen below.
::

    | bid:int (pk) | browser:text    | os:text |
    | ------------ | --------------- | ------- |
    | 1            | Firefox         | Linux   |
    | 2            | Firefox         | Windows |
    | 3            | Firefox         | MacOS   |
    | 4            | Chrome          | Linux   |
    | 5            | Chrome          | Windows |
    | 6            | Chrome          | MacOS   |
    | -1           | Unknown browser | Unknown |

It is clear that this version of the Drawn Table is much easier to read.

Drawn Table Testing as a Python Library
---------------------------------------
Using the constructs presented, users can efficiently define
preconditions and postconditions to test each part of their ETL flows.
This thus supports creation of tests during development, e.g., using
TDD. A full example using both DTT and Python’s ``unittest`` module is
shown here.

When using ``unittest``, a class must be defined for each set of tests.
We find it natural to group tests for a dimension into a class such that
they can share . A class using DTT to test the ETL flow for the
``browser`` dimension is defined on Line 1. It inherits from
``unittest.TestCase`` as required by ``unittest``. Two methods are then
overridden: ``setUpClass(cls)`` and ``setUp(self)``.

::

    class BrowserStateTest(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.cw = dtt.connectionwrapper()
            cls.initial = dtt.Table("browser", """
            | bid:int (pk) | browser:text | os:text |
            | ------------ | ------------ | ------- |
            | -1           | Unknown      | Unknown |
            | 1            | Firefox      | Linux   |
            | 2            | Firefox      | MacOS   |""")

        def setUp(self):
            self.initial.reset()

        def test_insertNew(self):
            expected = self.initial + "| 3 | Firefox | Windows |"
            newrows = expected.changes()
            etl.executeETLFlow(self.cw, newrows)
            expected.assertEqual()

        def test_insertExisting(self):
            row = {'bid':3, 'browser':'Firefox', 'os':'Linux'}
            etl.executeETLFlow(self.cw, [row])
            self.initial.assertEqual() 

The method ``setUpClass(cls)`` is executed before the tests (here
starting with ``test_``) in the class are executed. The method requests
a database connection from DTT on Line 4 and defines a Drawn Table with
the initial state of the dimension in Line 5. By creating them in
``setUpClass(cls)``, they are only initialized once and can be reused
for each test. To ensure the tests do not affect each other, which would
make the result depend on the execution order of the tests, the
``browser`` table in the database is reset before each test by
``setUp(self)``. Then on Line 15 and Line 21 the tests are implemented
as separate methods. ``test_insertNew(self)`` tests that a row that
currently does not exist in ``browser`` is inserted correctly, while
``test_insertExisting(self)`` ensures that an already existing row does
not become duplicated. In this example, both of these tests invoke the
user’s ETL flow by calling the user-defined method
``executeETLFlow(connection, newrows)``. This method could, e.g., spawn
a new process in which the user’s ETL tool runs. It is *not* a
requirement that the user’s ETL flow is implemented in Python despite
the tests being so.


Drawn Table Testing as a Stand-Alone Tool
-----------------------------------------
Above we demonstrated how DTT can be used as a library,
i.e., in the user’s own test code written in Python. It is, however,
also possible to use DTT without doing any programming. To enable this,
we have implemented a program with a command-line interface named
``dttr`` (for DTT Runner). Internally, ``dttr`` of course uses the
library with the functionality described in Section [sec:db]. By
implementing the functionality of DTT as a library, interfaces for
specific use-cases are easy to create. In this section, we explain how
``dttr`` can be used.

``dttr`` uses *test files*, which have the ``.dtt`` suffix, to specify
preconditions and/or postconditions. An example of a test file is given
in below. Note that a test file does not contain any
Python code. This file only contains one precondition (i.e., a Drawn
Table with a name, but without an assert above it) on Line 1–4 and one
postcondition (i.e., a Drawn Table with both a name and an assert above
it) on Line 6–12). This is, however, not a requirement as a ``.dtt``
file can contain any number of preconditions and/or postconditions.
Users are free to structure their tests as they please.

::

    browser
    | bid:int (pk) | browser:text    | os:text |
    | ------------ | --------------- | ------- |
    | -1           | Unknown browser | Unknown |

    browser, equal
    | bid:int (pk) | browser:text    | os:text |
    | ------------ | --------------- | ------- |
    | 1            | Firefox         | Linux   |
    | 2            | Firefox         | Windows |
    | 3            | Firefox         | MacOS   |
    | -1           | Unknown browser | Unknown |

The format of a test file is as follows. On the first line of a
precondition, the name of the table is given, in our example
``browser``. As ``dttr`` uses DTT internally, it uses an in-memory
SQLite database as the test database by default, but users can define
their own named PEP 249 connections in the configuration file ``config.py``.
In that case, the table name may include an ``@`` sign followed the name of the
connection to use for this table, e.g., ``browser@targetdw``. After the
table name, a Drawn Table must be specified (Lines 2–4 in the file above).
Like for any other Drawn Table, the header must
be given first, then the delimiter, and last the rows. To mark the end
of the precondition, an empty line is specified (Line 5).

For the specification of a postcondition, a table name is again given
first. The table name is followed by a comma and the name of the
assertion to use as shown in Line 6 in the file. In the
shown example, the table name is ``browser`` like for the precondition,
but they may be different. For example, the precondition could define
the initial state for ``inputdata@sourcedb`` and the postcondition could
define the expected state for ``browser@targetdw``. As already
mentioned, the name of the table to use for the postcondition is
followed by a comma and the assertion to use, i.e., ``equal`` in this
example. One can also use the other assertions in DTT: ``disjoint`` and
``subset``. Finally (Lines 7–12 in the file), the actual
Drawn Table is given in the same way as for the precondition. The Drawn
Table in the postcondition may also use variables. Note that a test does
not require both a precondition and postcondition, both are optional. It
is thus, e.g., possible to create a test file where no precondition is
set, but the postcondition still is asserted after executing the ETL
flow. Also, as stated, a ``.dtt`` file can contain any number of
preconditions and postconditions.

For tests that require more data than what is feasible to embed directly
in a Drawn Table, data in an external file or database can be added to a
Drawn Table by specifying an external data source as its last line. For
example, by adding the line ``csv browserdata.csv ,`` the contents of
the CSV file ``browserdata.csv`` is added to the Drawn Table with ``,``
used as field separator, in addition to any rows drawn as part of the
Drawn Table. By adding ``sql oltp SELECT bid, browser, os FROM browser``
as the last line all rows of the table ``browser`` from the PEP 249
connection ``oltp`` are added to the Drawn Table. This is
user-extensible through the configuration file such that the user (or
administrator) can add support for other sources of data, e.g., XML or a
NoSQL DBMS like MongoDB. This is done by creating a function in
``config.py``. If, for example, the line ``xml teacher 8`` is found in a
``.dtt`` file, ``xml('teacher', '8')`` is called (and the function
``xml`` must have been defined in ``config.py``).

``dttr`` can be invoked from the command line as shown in
here. Note that the ETL program to test and its
arguments simply are given to ``dttr`` as arguments (``–etl ...``).
Thus, any ETL program can be invoked.

::

    $ ./dttr.py --etl "python3 myetl --loaddim browser" 

When started, ``dttr`` by default looks for all ``.dtt`` test files
under the current working directory, but optional arguments allow the
user to select which files to consider. ``dttr`` then reads all relevant
test files. Then the preconditions from these files are set. This is
done by means of the DTT library’s ``ensure`` method such that each
table is created and its data is inserted if necessary. If a table with
the given name already exists and has differing content, an error will
be raised and the table will not be updated. After the preconditions
have been set, the user’s ETL flow is started. How to execute the ETL
flow is specified using the ``–etl`` flag as shown above. When
the ETL flow has finished, all postconditions are asserted and any
violation raises an error. If multiple
occurrences of the same variable have different values, an error will
also be raised, no matter if the
variables are in the same or different ``.dtt`` files. It is thus, e.g.,
possible to have a test file for the fact table and another test file
for a dimension table and still ensure that an inserted fact’s foreign
key references a specific dimension member.

