#####################################
Welcome to pygrametl's documentation!
#####################################

.. sectionauthor:: Christoffer Moesgaard <cmoesgaard@gmail.com>
.. sectionauthor:: Søren Kejser Jensen <devel@kejserjensen.dk>

A package for creating Extract-Transform-Load (ETL) programs in Python.
 
The package contains a number of classes for filling fact tables
and dimensions (including snowflaked and slowly changing dimensions), 
classes for extracting data from different sources, classes for defining
'steps' in an ETL flow, and convenient functions for often-needed ETL
functionality.

The package's modules are:

*   **datasources** for access to different data sources
*   **tables** for giving easy and abstracted access to dimension and fact tables
*   **parallel** for parallelizing ETL operations
*   **JDBCConnectionWrapper** and jythonmultiprocessing for support of Jython
*   **aggregators** for aggregating data
*   **steps** for defining steps in an ETL flow
*   **FIFODict** for providing a dict with a limited size and where elements are removed in first-in first-out order

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

.. To prevent the indices from being generated in the LaTeX documentation
.. only:: html
  
    Indices and tables
    ==================

    * :ref:`genindex`
    * :ref:`modindex`
    * :ref:`search`

