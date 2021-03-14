.. _jython:

Jython
======
pygrametl supports running ETL flows on Jython, an implementation of Python that
run on the JVM. Using Jython instead of CPython allows an ETL flow to be
parallelized using multiple threads instead of multiple processes. This is
because Jython does not have a global interpreter lock, which in CPython ensures
that only a single thread is running per process at a given time. For more
information about the GIL see the Python wiki `GIL
<https://wiki.python.org/moin/GlobalInterpreterLock>`__.

To make switching between CPython and Jython as simple as possible, two
abstractions are provided by pygrametl. Firstly, :mod:`.JDBCConnectionWrapper`
provides two connection wrappers for `JDBC
<https://jcp.org/en/jsr/detail?id=221>`__ connections with the same interface as
the connection wrappers for :pep:`249` connections. As the connection wrappers,
all share the same interface the user usually only has to change the connection
type (`JDBC <https://jcp.org/en/jsr/detail?id=221>`__ or :pep:`249`) and the
connection wrapper when switching between CPython and Jython. For more
information about database access in pygrametl see :ref:`database`. Secondly,
Jython currently has no support for :mod:`multiprocessing` as threads are more
lightweight than processes and multiple threads can be run in parallel. So
pygrametl includes the module :mod:`.jythonmultiprocessing` which wraps Python's
:mod:`threading` module and provides a very small part of Python's
:mod:`multiprocessing` module. Thus, pygrametl exposes the same interface for
creating parallel ETL flows no matter if a user is using CPython or Jython.

While both Jython and CPython are capable of executing the same language, the
two platforms are implemented differently, so optimizations suitable for one
platform may be less effective on the other. One aspect to be aware of when
running high-performance pygrametl-based ETL flows on Jython is memory
management. For example, Oracle's HotSpot JVM implements a generational garbage
collector that uses a much slower garbage collection strategy for the old
generations than for the young. Thus, allowing too many objects to be promoted
to the old generations can reduce the throughput of an ETL flow significantly.
Unfortunately, this can easily occur if the values controlling caching, such as
:attr:`.Decoupled.batchsize`, are set too high. Similarly, if the value for
:attr:`.Decoupled.batchsize` is set too low the overhead of transferring data
between threads increases as smaller batches are used. Many tools for profiling
programs running on the JVM exist: `JFR
<https://docs.oracle.com/javacomponents/jmc-5-4/jfr-runtime-guide/about.htm>`__
and `JConsole
<http://docs.oracle.com/javase/8/docs/technotes/guides/management/jconsole.html>`__
are bundled with the JDK, while tools such as `VisualVM
<https://visualvm.github.io/>`__ must be installed separately but often provide
additional functionality.

Setup
-----
Using pygrametl with Jython requires an extra step compared to CPython, as
Jython is less integrated with Python's package management system. Firstly,
install pygrametl from `PyPI <https://pypi.python.org/pypi/pygrametl/>`__ or by
downloading the development version from `GitHub
<https://github.com/chrthomsen/pygrametl>`__. For more information about
installing pygrametl for use with CPython see :ref:`install`.

After pygrametl has been installed, the location it has been installed to must
be added to the environment variable ``JYTHONPATH``, as Jython purposely does
not import modules from CPython by default. The default directory used by
CPython for packages depends on the operating system and whether a package is
installed locally or globally. Check the output of the ``pip install`` command
or its log for precise information about where the package has being installed.
The method for setting this variable depends on the operating system. On most
Unix-like systems, the variable can be set in ``~/.profile``, which will be
sourced on login. On Windows, environment variables can be changed through the
System setting in the Control Panel. Python's module search path can also be
extended on a per program basis by adding a path to :attr:`.sys.path` at the
start of a Python program.

Usage
-----
Jython can in most cases be used as a direct replacement for CPython unless its
C API is being used. While Jython does not implement CPython C API, it can use
libraries implemented in other JVM-based languages like Java, Scala, Clojure,
and Kotlin. To use such libraries, they must be added to the JVM classpath by
using the ``-J-cp`` command-line option. For more information about Jython's
command-line flags run the command ``jython -h``.

.. code-block:: python

    from pygrametl.tables import FactTable
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper

    # The Java classes used must be explicitly imported into the program
    import java.sql.DriverManager

    # The actual database connection is handled by a JDBC connection
    jconn = java.sql.DriverManager.getConnection(
	"jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    # As PEP 249 and JDBC connections provide different interfaces, is it
    # necessary to use a JDBCConnectionWrapper instead of a ConnectionWrapper.
    # Both provides the same interface, thus pygrametl can execute queries
    # without taking into account how the connection is implemented
    conn = JDBCConnectionWrapper(jdbcconn=jconn)

    # This instance of FactTable manages the table "facttable" in the
    # database using the default connection wrapper created above
    factTable = FactTable(
	name='testresults',
	measures=['errors'],
	keyrefs=['pageid', 'testid', 'dateid'])

The above example demonstrates how few changes are needed to change the first
example from :ref:`facttables` from using CPython to Jython. The database
connection is changed from a :pep:`249` connection to a `JDBC
<https://jcp.org/en/jsr/detail?id=221>`__ connection, and
:class:`.ConnectionWrapper` is changed to
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper`. The creation of the
:class:`.FactTable` object does not need to be changed to run on Jython, as the
connection wrappers abstract away the differences between `JDBC
<https://jcp.org/en/jsr/detail?id=221>`__ and :pep:`249`. The other Jython
module, :mod:`.jythonmultiprocessing`, is even simpler to use as pygrametl's
parallel module :mod:`.parallel` imports either it or CPython's built-in
:mod:`.multiprocessing` module depending on whether Jython or CPython is used.
