#####################################
pygrametl - ETL Development in Python
#####################################
pygrametl is a package for creating Extract-Transform-Load (ETL) programs in Python.

The package contains several classes for filling fact tables and dimensions
(including snowflaked and slowly changing dimensions), classes for extracting
data from different sources, classes for optionally defining an ETL flow using
steps, classes for parallelizing an ETL flow, classes for testing an ETL flow,
and convenient functions for often-needed ETL functionality.

The package's modules are:

*   **datasources** for access to different data sources
*   **tables** for giving easy and abstracted access to dimension and fact tables
*   **parallel** for parallelizing an ETL flow
*   **JDBCConnectionWrapper** and **jythonmultiprocessing** for Jython support
*   **aggregators** for aggregating data
*   **steps** for defining steps in an ETL flow
*   **FIFODict** for a dict with a limited size and where elements are removed in first-in, first-out order
*   **drawntabletesting** for testing an ETL flow


pygrametl is currently being maintained at Aalborg University in Denmark by the following people:

**Current Maintainers**
    - Christian Thomsen <chr@cs.aau.dk>
    - Søren Kejser Jensen <devel@kejserjensen.dk>

**Former Maintainers**
    - Christoffer Moesgaard <cmoesgaard@gmail.com>
    - Ove Andersen <ove.andersen.oa@gmail.com>

Getting started
===============

.. toctree::
   :maxdepth: 1

   quickstart/install
   quickstart/beginner

Code Examples
=============

.. toctree::
   :maxdepth: 1

   examples/database
   examples/datasources
   examples/dimensions
   examples/facttables
   examples/bulkloading
   examples/parallel
   examples/jython
   Testing <examples/testing>

API
===

.. toctree::
   :maxdepth: 1

   api/pygrametl
   api/datasources
   api/tables
   api/parallel
   api/jdbcconnectionwrapper
   api/jythonmultiprocessing
   api/aggregators
   api/steps
   api/fifodict
   api/drawntabletesting

.. Prevents the indices from being generated in the LaTeX documentation
.. only:: html

    Indices and tables
    ==================

    * :ref:`genindex`
    * :ref:`modindex`
    * :ref:`search`
