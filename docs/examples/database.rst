.. _database:

Database
========
Database access in *pygrametl* is done through either a :PEP:`249` connection
if CPython is used, or with a JDBC connection when pygrametl is running on
Jython. pygrametl provides multiple abstractions on top of these connections
and direct usage of these to manipulate the database should generally not be
necessary. As an abstraction for database rows, Python's :class:`.dict` type is
used, where the keys are named after the attributes in the table and the values
being the values stored in that particular row.


Connection Wrappers
-------------------
Multiple connection wrappers are provided by the pygrametl framework in order
to allow :PEP:`249` connections and JDBC connections to be used uniformly, and
to allow multiple threads and process to use the connection safely. In addition
the connection wrappers for :PEP:`249` connections also automatically converts
from the pyformat parameter style used by pygrametl to any of the other
parameter styles defined in :PEP:`249#paramstyle`.  To simplify the use of the
database connection the first connection wrapper created is set as a default,
which can then be accessed directly by the database abstractions such as
:class:`.tables.FactTable` and :class:`.tables.Dimension` without the user
having to pass around the connection explicitly in the ETL program. If an
alternative database connection is used, for example if data is read from one
database and written to another, a specific connection wrapper can be passed as
argument to the abstraction in question.

The two simplest connection wrappers provided by pygrametl are
:class:`.ConnectionWrapper` and
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper`. The interface provided
by these two classes is just an abstraction on top of database operations, and
provides methods, among others, for executing statements, iterating over returned
rows and committing transactions. Note however that these connection wrappers
cannot be accessed in parallel, and creating a unique database connection
abstraction that needs access to the database in a parallel ETL flow, has a
very high resource consumption and might also lead to inconsistencies.

To ensure that database access is done correctly in a parallel ETL program
without burdening the user with the task, the class
:class:`.parallel.SharedConnectionWrapperClient` is provided. This class can be
created from an existing connection wrapper using the function
:func:`.parallel.shareconnectionwrapper`. Each separate process can then be
given a unique copy of the client in order to access the database safely. For
more information about the parallel capabilities of pygrametl see
:ref:`parallel`.


Experimental Connection Wrappers
--------------------------------
In addition to the previously documented connection wrappers, two experimental
connection wrappers are also provided by pygrametl.
:class:`.BackgroundConnectionWrapper` and
:class:`.JDBCConnectionWrapper.BackgroundJDBCConnectionWrapper`. They are
provided as alternatives to :class:`.ConnectionWrapper` and
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper` and perform the database
operations in a separate thread instead of the same thread as the ETL program.
As they are considered experimental, they are not set as default upon creation,
and must thus be set with the method :meth:`setasdefault`, available on both
classes, or manually passed around the program.

For most usage the classes :class:`.BackgroundConnectionWrapper` and
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper` will likely provide
better performance compared to the background versions. Furthermore, a
connection wrapper used in a parallel ETL program should always be wrapped
using :func:`.parallel.shareconnectionwrapper` to ensure safe parallel database
access, which itself runs the connection wrapper in a separate process or
thread depending on the implementation.  As the two implementations are very
similar and provide an identical interface, either set of implementations might
be removed in a future release.
