Unreleased
----------
**Added**
  ``TypeOneSlowlyChangingDimension`` a new class that adds support for
  efficient loading and updating of a type 1 exclusive slowly changing
  dimension.

  ``CachedBulkLoadingDimension`` a new class that supports bulk loading a
  dimension without requiring the caching of all rows that are loaded.

  Dimension classes with finite caches can now be prefilled more efficiently
  using the "FETCH FIRST" SQL statement for increased performance.

  Examples on how to perform bulk loading in MySQL, Oracle Database, and
  Microsoft SQL Server.

**Changed**
  It is now verified that ``lookupatts`` is a subset of all attributes.

  All method calls to a superclass constructor now uses named parameters.

  Made cosmetic changes, and added additional information about how to ensure
  cache coherency between pygrametl and the database to existing docstrings.

  The entire codebase was updated to adhere more closely to PEP8 using
  autopep8.

**Fixed**
  Using ``dependson`` no longer causes crashes due to multiple loads of a
  table.

  Using ``defaultidvalue`` no longer causes ``Dimension.ensure`` to fail to
  insert correctly, or make ``CachedDimension.ensure`` produce duplicates.

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
  Using other parameter styles then ``pyformat`` no longer causes a crash in
  ``ConnectionWrapper``.

Version 2.3
-------------
**Added**
  A new quick start guide was added to the documentation.

  Added code examples for all classes in pygrametl except ``Steps``.

  Pygrametl now officially supports Python 2.6.X, Python 2.7.X, Python 3,
  Jython 2.5.X and Jython 2.7.X.

  ``SQLSource`` can now pass parameters to the cursor's ``execute`` function.

**Fixed**
  Importing everything from ``tables`` using a wildcard now longer causes a
  crash.

Version 2.2
-----------
**Added**
  Created a PyPI package and uploaded it to pypi.python.org.

  Added code examples for some of the classes in pygrametl.

**Changed**
  Documentation is now written in reStructuredText and compiled using Sphinx.
