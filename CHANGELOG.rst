Version 2.7
-----------
**Note**
  This is the last version to actively support Python 2. Support for it will
  slowly be reduced as we continue to develop pygrametl.

**Added**
  ``drawntabletesting`` a new module for testing ETL flows. The module makes it
  easy to define the preconditions and postconditions for the database as part
  of each test. This is done simply by "drawing" the tables and their contents
  using strings.

  ``AccumulatingSnapshotFactTable`` a new class supporting accumulating snapshot
  fact tables where facts can be updated as a process progresses.

  ``BatchFactTable.__init__`` now optionally takes the argument ``usemultirow``.
  When this argument is ``True`` (the default is ``False``), batches are loaded
  using ``execute`` with a single ``INSERT INTO name VALUES`` statement instead
  of ``executemany()``. (GitHub issue #19).

  ``closecurrent`` method added to ``SlowlyChangingDimension`` to make it
  possible to set an end date for the most current version without adding a new
  version.

  A (read-only) property ``awaitingrows`` added to ``BatchFactTable`` and
  ``_BaseBulkloadable`` to get the number of inserted rows awaiting to be loaded
  into the database table. (GitHub issue #23)

**Changed**
  ``SlowlyChangingDimension.scdensure`` now checks if the newest version has its
  ``toatt`` set to a value different from ``maxto`` (if ``toatt`` is defined).
  This can happen from a call to ``closecurrent`` or a manual update. If it is
  the case, a new version will be added when ``scdensure`` is called even if no
  other differences are present.

  Generators in ``datasources`` don't raise ``StopIteration`` anymore as
  required by PEP 479.

  ``__author__`` and ``__maintainer__`` removed from all .py files.

  ``__version__`` removed from all .py files except ``pygrametl/__init__.py``
  The version of pygrametl is thus now available as ``pygrametl.__version__``
  and will be updated for every release.

**Fixed**
  Outdated information stating that type 1 slowly changing dimensions are not
  supported has been removed from the documentation. In addition, minor errors
  and inconsistencies have been corrected throughput the documentation. (GitHub
  issue #27)

  Wrong use of paramstyle in ``ConnectionWrapper.executemany`` fixed.

  A call to an incorrect method in ``aggregators.Avg.finish()``.

  The ``datespan()`` function now checks whether ``fromdate`` and ``todate`` are
  strings before calling ``.split()``. In addition, the function now uses
  ``dict.items()`` instead of ``dict.iteritems()`` which is not supported in
  Python 3.

Version 2.6
-----------
**Added**
  ``PandasSource`` a new class, that given a Pandas ``DataFrame`` acts as a data
  source. Each row of the ``DataFrame`` is returned as a ``dict`` that can be
  loaded into a data warehouse using ``tables``.

  ``MappingSource`` a new class, that given a data source and a dictionary of
  columns to callables, maps the callables over each element of the specified
  column before returning the row.

**Changed**
  ``SlowlyChangingDimension`` improved to make ``versionatt`` optional. (GitHub
  issue #12. Thanks to HereticSK)

  ``ConnectionWrapper.__init__`` now optionally takes the argument
  ``copyintonew``. When this argument is ``True`` (the default is ``False``), a
  new ``dict`` with parameters is created when a statement is executed. The new
  ``dict`` only holds the k/v pairs needed by the statement. This is to avoid
  ``DatabaseError: ORA-01036: illegal variable name/number`` with cx_Oracle.
  (GitHub issue #9).

  First argument to ``TypedCSVSource.__init__`` renamed from ``csvfile`` to
  ``f`` to be consistent with documentation and ``CSVSource``

**Fixed**
  ``ConnectionWrapper.execute`` does not pass the argument ``arguments`` to the
  underlying cursor's execute method if ``arguments`` is ``None``. Some drivers
  raise an ``Error`` if ``None`` is passed, some don't.

Version 2.5
-----------
**Added**
  ``TypedCSVSource`` a new class that reads a CSV file (by means of
  ``csv.DictReader``) and performs user-specified casts (or other function
  calls) on the values before returning the rows.

  Added ``definequote`` function to enable quoting of SQL identifiers in all
  tables.

  Added ``getdbfriendlystr`` function to enable conversion of values into
  strings that are accepted by an RDBMS. Boolean values become ```0`` or ``1``,
  ``None`` values can be replaced by another value.

  All Bulkloadables now accept the argument ``strconverter`` to their
  ``__init__`` methods. This should be a function that converts values into
  strings that are written to a temporary file and eventually bulkloaded. The
  default value is the new ``getdbfriendlystr``.

  ``SlowlyChangingDimension`` can now optionally be given the argument
  ``useorderby`` when instantiated. If ``True`` (the default), the SQL used by
  ``lookup`` uses ``ORDER BY`` (this is the same behaviour as before). If
  ``False``, ``ORDER BY`` is not used and the SQL used by ``lookup`` will fetch
  all versions of the member and then find the key value for the newest version
  with Python code. For some systems, this can lead to significant performance
  improvements.

**Changed**
  Generator used in ``ConnectionWrapper.fetchalltuples`` to reduce memory
  consumption. (Thanks to Alexey Kuzmenko)

  ``SlowlyChangingDimension`` can sometimes avoid deleting from the cache on
  updates, now checked in the same way as in ``CachedDimension``

  ``rowfactory`` now tries to use ``fetchmany``. (Suggested by Alexey Kuzmenko).

  ``_BaseBulkloadable`` now has the method ``insert`` while the methods
  ``_insertwithnull`` and ``_insertwithoutnull`` have been removed (and
  subclasses do thus not pick one of them at runtime). The ``insert`` method
  will always call ``strconverter`` (see above) no matter if a ``nullsubst`` has
  been specified or not.

  ``_BaseBulkloadable`` will now raise a ``TypeError`` if no ``nullsubst`` is
  specified and a ``None`` value is present. Before this change, the ``None``
  value would silently be converted into the string ``'None'``. Users must now
  give a ``nullsubst`` argument when instantiating a subclass of
  ``_BaseBulkloadable`` that should be able to handle ``None`` values.

  ``SubprocessFactTable`` has been changed similarly to ``_BaseBulkloadable``
  and does now define ``insert`` which uses ``strconverter``. Thus
  ``_insertwithnull`` and  ``_insertwithoutnull`` have been removed.

  ``getunderlyingmodule`` has been changed and now tries different possible
  module names and looks for ``'paramstyle'`` and ``'connect'``.
  ``ConnectionWrapper`` now uses ``getunderlyingmodule`` in ``__init__`` when
  trying to determine the paramstyle to use.

**Fixed**
  Using ``cachesize=0`` with ``SlowlyChangingDimension`` no longer causes
  crash.

  Problem with double use of namemappings in ``_before_update`` in
  ``CachedDimension`` and ``SlowlyChangingDimension`` fixed. (Thanks to Alexey
  Kuzmenko).

  Problem with ``rowfactory`` only returning one row fixed. (Thanks to Alexey
  Kuzmenko).

  Problem with ``JDBCConnectionWrapper.rowfactory`` returning dictionaries with
  incorrect keys fixed. (GitHub issue #5).

  Problem with ``TypeOneSlowlyChangingDimension`` caching ``None`` after an
  update if a namemapping mapped to an attribute not in the update row fixed.

  Problem in ``__init__.copy`` fixed.

  Namemapping is now used when comparing measure values in ``FactTable.ensure``
  with ``compare=True``.

Version 2.4
-----------
**Note**
  This is the last version to support versions of Python 2 older than 2.7

**Added**
  ``TypeOneSlowlyChangingDimension`` a new class that adds support for efficient
  loading and updating of a type 1 exclusive slowly changing dimension.

  ``CachedBulkLoadingDimension`` a new class that supports bulk loading a
  dimension without requiring the caching of all rows that are loaded.

  Alternative implementation of ``FIFODict`` based on an ``OrderedDict``.
  (Thanks to Alexey Kuzmenko).

  Dimension classes with finite caches can now be prefilled more efficiently
  using the ``FETCH FIRST`` SQL statement for increased performance.

  Examples on how to perform bulk loading in MySQL, Oracle Database, and
  Microsoft SQL Server. (Thanks to Alexey Kuzmenko).

**Changed**
  It is now verified that ``lookupatts`` is a subset of all attributes.

  All method calls to a superclass constructor now uses named parameters.

  Made cosmetic changes, and added additional information about how to ensure
  cache coherency between pygrametl and the database to existing docstrings.

  The entire codebase was updated to adhere more closely to PEP 8 using
  autopep8.

**Fixed**
  Using ``dependson`` no longer causes crashes due to multiple loads of a table.
  (Thanks to Alexey Kuzmenko).

  Using ``defaultidvalue`` no longer causes ``Dimension.ensure`` to fail to
  insert correctly, or make ``CachedDimension.ensure`` produce duplicates.
  (Thanks to Alexey Kuzmenko).

  Using ``SlowlyChangingDimension`` with the cache disabled no longer causes a
  crash in ``SlowlyChangingDimension.scdensure``.

  Using ``BulkDimension``, ``CachedBulkDimension`` or ``BulkFactTable`` with
  ``tempdest`` and ``usefilename`` no longer causes a crash in
  ``_BaseBulkloadable._bulkloadnow``.

Version 2.3.2
-------------
**Fixed**
  ``SnowflakedDimension`` no longer crashes due to ``levellist`` not being a
  list before the length of it is computed.

  ``FactTable`` now inserts the correct number of commas to the SQL statements
  used for inserting rows, independent of the value of ``keyrefs``.

Version 2.3.1
-------------
**Fixed**
  Using other parameter styles than ``pyformat`` no longer causes a crash in
  ``ConnectionWrapper``.

Version 2.3
-------------
**Added**
  A new quick start guide was added to the documentation.

  Added code examples for all classes in pygrametl except ``Steps``.

  pygrametl now officially supports Python 2.6.X, Python 2.7.X, Python 3, Jython
  2.5.X and Jython 2.7.X.

  ``BulkDimension`` a new class that supports bulk loading of dimension tables.

  ``_BaseBulkloadable`` with common functionality for ``BulkFactTable`` and
  ``BulkDimension``.

  ``SQLSource`` can now pass parameters to the cursor's ``execute`` function.

**Fixed**
  Importing everything from ``tables`` using a wildcard now longer causes a
  crash.

Version 2.2
-----------
**Added**
  Created a PyPI package and uploaded it to `pypi.python.org/project/pygrametl
  <https://pypi.python.org/project/pygrametl>`_.

  Added code examples for some of the classes in pygrametl.

**Changed**
  Documentation is now written in reStructuredText and compiled using Sphinx.
