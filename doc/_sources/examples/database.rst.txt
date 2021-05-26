.. _database:

Database
========
Database access in pygrametl is done through either a :PEP:`249` connection if
CPython is used, or with a `JDBC <https://jcp.org/en/jsr/detail?id=221>`__
connection when pygrametl is running on Jython. pygrametl provides multiple
abstractions on top of these connections and direct usage of these to manipulate
the database should generally not be necessary. As an abstraction for database
rows Python's :class:`.dict` type is used, where the keys the names of the
columns in the table and the values are the data stored in that row.


Connection Wrappers
-------------------
Multiple connection wrappers are provided by the pygrametl framework to allow
:PEP:`249` connections and `JDBC <https://jcp.org/en/jsr/detail?id=221>`__
connections to be used uniformly, and to allow multiple threads and process to
use the connection safely. In addition, the connection wrappers for :PEP:`249`
connections also automatically convert from the pyformat parameter style used by
pygrametl to any of the other parameter styles defined in :PEP:`249#paramstyle`.
To simplify the use of database connections, the first connection wrapper
created is set as the default. The default connection wrapper can be used by
abstractions such as :class:`.tables.FactTable` and :class:`.tables.Dimension`
without the user having to pass the connection wrapper to them explicitly. If
another database connection should be used, for example, if data is read from one
database and written to another, a specific connection can be explicitly passed
as an argument to all pygrametl abstractions that can read to and/or write from
a database.

:class:`.ConnectionWrapper` and
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper` are the two main
connection wrappers provided by pygrametl. The interface provided by these two
classes is just an abstraction on top of database operations, and provides
methods, among others, for executing statements, iterating over returned rows,
and committing transactions. Note however that these connection wrappers cannot
be used by multiple threads or processes in parallel. To ensure that database
access is performed correctly in a parallel ETL program without burdening the
user with the task, the class :class:`.parallel.SharedConnectionWrapperClient`
is provided. This class can be created from an existing connection wrapper using
the function :func:`.parallel.shareconnectionwrapper`. Each separate process can
then be given a unique copy of the shared connection to access the database
safely in parallel. For more information about the parallel capabilities of
pygrametl see :ref:`parallel`.


Experimental Connection Wrappers
--------------------------------
pygrametl also provides two very experimental connection wrappers:
:class:`.BackgroundConnectionWrapper` and
:class:`.JDBCConnectionWrapper.BackgroundJDBCConnectionWrapper`. They are
provided as alternatives to :class:`.ConnectionWrapper` and
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper` and perform the database
operations in a separate thread instead of the same thread as the ETL program.
As they are considered experimental, they are not set as default upon creation,
and must thus manually be set as the default with the method
:meth:`setasdefault`, available on all connection wrappers, or be manually
passed around the program.

For most usage the classes :class:`.ConnectionWrapper` and
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper` will likely provide better
performance compared to the background versions. Furthermore, a connection
wrapper used in a parallel ETL program should always be wrapped using
:func:`.parallel.shareconnectionwrapper` to ensure safe parallel database
access, which itself runs the connection wrapper in a separate process or thread
depending on the implementation. As the two implementations are very similar and
provide an identical interface, either set of implementations might be removed
in a future release.
