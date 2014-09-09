.. _jython:

Jython
======
*pygrametl* contains additional support for running ETL programs on Jython, the
Java Virtual Machine implementation of Python. Using Jython compared to CPython
allows for threads to be used instead of processes for performing operations in
parallel. This is caused by the lack of global interpreter lock on the JVM,
which in CPython prevents more then one thread in the same process from running
at the same time. For more information about the GIL see the Python wiki `GIL
<https://wiki.python.org/moin/GlobalInterpreterLock>`_.

To make the switch between CPython and Jython as simple as possible, two
abstractions are provided by pygrametl. Firstly, :mod:`.JDBCConnectionWrapper`
provides two wrappers for database connections following the Java Database
Connectivity standard (JDBC), and as pygrametl uses a similar wrapper for
:pep:`249` database connections, they can be used without any changes to the
program code. For more information about database accesses in pygrametl see
:ref:`database`. Secondly, as Jython currently has no support for
:mod:`multiprocessing` due to threads being more lightweight and capable of
running in parallel, another abstraction is provided by pygrametl in the form
of the module :mod:`.jythonmultiprocessing`. This module wraps :mod:`threading`
to implement a very small part of Python :mod:`multiprocessing` module, so the
same library interface can used on both Jython and CPython.

Note, that while both Jython and CPython are capable of executing the same
language, the two platforms are implemented differently, so optimisations
suitable for one platform may be less effective for the other.  One aspect to
be aware of when running high performance pygrametl flows on Jython, is memory
management.  The JVM, which Jython runs on, uses a generational garbage
collector with more expensive garbage collection strategies used for the old
generation part of the heap. Allowing too many objects to be moved to this part
of memory can reduce the throughput of an ETL flow significantly, something
which can easily occur if values controlling caching such as
:attr:`.Decoupled.batchsize`, are set too high. Similarly, a too low value
would in the case of :attr:`.Decoupled.batchsize` increase the overhead of
transferring data between threads, as smaller batches are used.  Multiple tools
for profiling JVM based programs exist: `HPROF
<http://docs.oracle.com/javase/8/docs/technotes/samples/hprof.html>`_ and
`JConsole
<http://docs.oracle.com/javase/8/docs/technotes/guides/management/jconsole.html>`_
are bundled with the JDK, while tools such as `VisualVM
<http://visualvm.java.net/>`_ must be installed separately but often provide
additional functionality.

Setup
-----
Using pygrametl with Jython requires a few extra steps compared to CPython, as
Jython is less integrated with Python's package management system, and the JVM
needs access to the necessary libraries. First, install pygrametl either
through `PyPI <https://pypi.python.org/pypi/pygrametl/>`_ or by checking out
the latest development version from `Google Code
<https://code.google.com/p/pygrametl/>`_, for more information about
installation of pygrametl for CPython see :ref:`install`.

After pygrametl has been installed, the install location must be added to the
environment variable 'JYTHONPATH', as Jython purposely does not read the
locations used by CPython as a default. The default pip install directory
depends on the operating system, and whether packages are installed locally or
globally, check the output of the pip install command or its log for precise
information about where the packages are installed.  The method for setting
this variable depends on your operating system. On most Unix-like systems , the
variable can be set in '~/.profile', which will be sourced on login. On
Windows, environment variables can be changed through the System setting in in
the Control Panel. The module path can also be set programmatically through the
method :meth:`.sys.path`

Usage
-----
Jython can in most cases be used as a replacement for Jython, with the
exception of C-Extensions which Jython replaces with the capability to use
libraries from languages targeting the JVM such as Java, Scala or Clojure.  To
accesses JVM libraries, they must be added to the JVM classpath by using the
'-J-cp' command line option. For more information about Jython's command line
flags, see `Jython CLI <http://jython.org/docs/using/cmdline.html>`_.

.. code-block:: python

    import pygrametl
    from pygrametl.tables import FactTable
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper
    
    # Java classes used must be imported into the program
    import java.sql.DriverManager
    
    # The actual database connection is handled using a JDBC connection
    jconn = java.sql.DriverManager.getConnection \
        ("jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")
    
    # As PEP 249 and JDBC connections are different must JDBCConnectionWrapper 
    # instead of ConnectionWrapper. The class has the same interface and a 
    # reference to the wrapper is also saved to allow for easy access of it
    conn = JDBCConnectionWrapper(jdbcconn=jconn)
    
    # The instance of FactTable connects to the table "facttable" in the 
    # database using the default connection wrapper we just created 
    factTable = FactTable(
        name='testresults',
        measures=['errors'],
        keyrefs=['pageid', 'testid', 'dateid'])

The above example demonstrates how few changes are needed to in order to change
the first example from :ref:`facttables` from using CPython to Jython. The
database connection is changed to use a JDBC connection, and
:class:`.ConnectionWrapper` is changed to
:class:`.JDBCConnectionWrapper.JDBCConnectionWrapper`. The creation of the fact
table does not need to be changed in any way to run on Jython, as the
connection wrappers abstract away the differences between JDBC and :pep:`249`.
The other Jython module, :mod:`.jythonmultiprocessing`, is even simpler to use
as pygrametl's parallel module :mod:`.parallel` imports either it, or CPythons
built-in :mod:`.multiprocessing` module depending on whether Jython or CPython
is used. 

