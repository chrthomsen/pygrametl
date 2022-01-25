.. _beginner:

Beginner Guide
==============
The following is a small guide for users new to pygrametl. It shows the main
constructs provided by pygrametl and how to use them to create a simple ETL
flow for a made-up example. The example is a toy data warehouse for a chain of
book stores and is shown below as :ref:`dwexample`. The warehouse has one fact
table and three dimensions organized in a star schema. The fact table stores
facts about how many of each book is sold each day. The book dimension stores
the name and genre of each book sold, the location dimension stores the city
and region of the stores, and the time dimension stores the date of each sale.
To keep the example simple, none of the dimensions are snowflaked, nor do they
contain any slowly changing attributes. These are however supported by
pygrametl through :class:`.SnowflakedDimension`,
:class:`.TypeOneSlowlyChangingDimension`, and
:class:`.SlowlyChangingDimension`, respectively. In addition, pygrametl
provides high-level constructs for creating efficient multiprocess or
multithreaded ETL flow, depending on the implementation of Python used (see
:ref:`parallel`). pygrametl also simplifies testing by making it easy to define
preconditions and postconditions for each part of an ETL flow without relying
on external files for the input and the expected results (see :ref:`testing`).

.. note::
   When using pygrametl, we strongly recommend using named parameters when
   instantiating classes as this improves readability and prevents errors in
   the future if the API changes.

.. .* makes 'make html' uses .svg and 'make latex' uses .pdf
.. _dwexample:

.. figure:: ../_static/example.*
    :align: center
    :alt: bookstore data warehouse example

    Data Warehouse example

Input Data
----------
Most pygrametl abstractions either produce, consume, or operate on data in
rows. A row is a Python :class:`.dict` where the row's column names are the
keys and the values are the data the row contains. For more information about
the data source provided by pygrametl see :ref:`datasources`.

The most important data for the warehouse are which books have been sold. This
information can be extracted from the book stores' sales records which are
stored in the SQLite database :formatref:`sale.sqlite
<../_static/sale.sqlite>`. Of course, storing sales records from multiple book
stores is not a common use-case for SQLite. However, SQLite is used in this
beginner guide as it makes sharing the input data simple and demonstrates
pygrametl's ability to read and write from any RDBMS that provides a :pep:`249`
connection. The data is stored in the table sale as shown below:

.. code-block:: none

   | book:text             | genre:text | store:text | date:date      | sale:int |
   | --------------------- | ---------- | ---------- | -------------- | -------- |
   | Nineteen Eighty-Four  | Novel      | Aalborg    | 2005/08/05     | 50       |
   | Calvin and Hobbes One | Comic      | Aalborg    | 2005/08/05     | 25       |
   | The Silver Spoon      | Cookbook   | Aalborg    | 2005/08/14     | 5        |
   | The Silver Spoon      | Cookbook   | Odense     | 2005/09/01     | 7        |
   | ....                  |            |            |                |          |


The book titles and genres are extracted from the `CMU Book Summary Dataset
<https://www.cs.cmu.edu/~dbamman/booksummaries.html>`_. As the geographical
information stored in the sales records is limited, the location dimension must
be pre-filled with data from the CSV file :formatref:`region.csv
<../_static/region.csv>`. This file contains data about cities and regions as
shown below (tabs are added for readability):

.. code-block:: none

    city,       region
    Aalborg,    North Denmark Region
    Odense,     Region of Southern Denmark
    ....

ETL Flow
--------
The ETL flow is designed to run on CPython and use PostgreSQL as the RDBMS for
the data warehouse. The guide assumes PostgreSQL is already installed and
running. The ETL flow can easily be run on other Python implementations like
Jython. For example, to use Jython the :pep:`249` database drivers must simply
be replaced with their `JDBC <https://jcp.org/en/jsr/detail?id=221>`__
equivalents and :class:`.ConnectionWrapper` with
:class:`.JDBCConnectionWrapper`. For more information about running pygrametl
on Jython see :ref:`jython`.

We start by creating the database and tables for the data warehouse in
PostgreSQL using psql. The SQL script :formatref:`example.sql
<../_static/example.sql>` creates the dw database, the dwuser role with all
privileges, and the four tables::

    psql -f example.sql

For the ETL flow we start by importing the various functions and classes needed
in this beginner guide. The psycopg2 and slite3 database driver must be
imported so a connection to PostgreSQL and sqlite3 can be established. The main
pygrametl module is also imported so a :class:`.ConnectionWrapper` can be
created. pyggrametl's :mod:`.datasources` module is imported so the sales
records (:class:`.SQLSource`) and CSV file (:class:`.CSVSource`) can be read.
Finally, classes for interacting with the fact table (:class:`.FactTable`) and
the various dimensions (:class:`.Dimension`) are imported from :mod:`.tables`.

.. code-block:: python

    # psycopg2 is a database driver allowing CPython to access PostgreSQL
    import psycopg2

    # sqlite3 is a database driver allowing CPython to access SQLite
    import sqlite3

    # pygrametl's __init__ file provides a set of helper functions and more
    # importantly the class ConnectionWrapper for wrapping PEP 249 connections
    import pygrametl

    # pygrametl makes it simple to read external data through datasources
    from pygrametl.datasources import SQLSource, CSVSource

    # Interacting with the dimensions and the fact table is done through a set
    # of classes. A suitable object must be created for each table
    from pygrametl.tables import Dimension, FactTable

Then a connection to the database containing the sales records and the data
warehouses is needed. For CPython, these must be :pep:`249` connections. As the
data warehouse connection will be shared by multiple pygrametl abstractions, an
instance of :class:`.ConnectionWrapper` is created. The first instance created
of this class is set as the default connection for pygrametl's abstractions.
This allows pygrametl to be used without having to pass a connection to each
abstraction that needs it. A :class:`.ConnectionWrapper` is not needed for the
connection to the sales database as it is only used by the :class:`.CSVSource`,
so in that case, the :pep:`249` connection is used directly. For more
information about database connections in pygrametl see :ref:`database`.

.. code-block:: python

    # Creates a PEP 249 connection to the sales database. PARSE_DECLTYPES makes
    # sqlite3 return values with the types specified in the database's schema.
    sale_conn = sqlite3.connect("sale.sqlite",
            detect_types=sqlite3.PARSE_DECLTYPES)

    # While SQLite is used in this guide, any RDBMS that provides a PEP 249
    # driver can be used with pygrametl. For example, SQLite can be replaced
    # with PostgreSQL by simply replacing sale_conn with following two lines.
    # sale_string = "host='localhost' dbname='sale' user='user' password='pass'"
    # sale_conn = psycopg2.connect(sale_string)

    # A connection is also created to the data warehouse. The connection is
    # then given to a ConnectionWrapper so it becomes implicitly shared between
    # all the pygrametl abstractions that needs it without being passed around
    dw_string = "host='localhost' dbname='dw' user='dwuser' password='dwpass'"
    dw_conn = psycopg2.connect(dw_string)

    # Although the ConnectionWrapper is shared automatically between pygrametl
    # abstractions, it is saved in a variable so the connection can be closed
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_conn)

To get data into the ETL flow, two data sources are created. One for the
database containing the sales records, and one for the CSV file containing the
region information. For more information about the various data sources
provided by pygrametl see :ref:`datasources`.

.. code-block:: python

    # The location dimension stores the name of a location in the column city
    # instead of in the column store as done in the input data from the sales
    # database. By passing SQLSource a sequence of names matching the number of
    # columns in the table it can automatically rename the columns
    name_mapping = 'book', 'genre', 'city', 'date', 'sale'

    # Extraction of rows from a database using a PEP 249 connection and SQL
    query = "SELECT book, genre, store, date, sale FROM sale"
    sale_source = SQLSource(connection=sale_conn, query=query,
            names=name_mapping)

    # Extraction of rows from a CSV file does not require a PEP 249 connection,
    # just an open file handler. pygrametl uses Python's DictReader for CSV
    # files and assumes the header of the CSV file contains the name of each
    # column. When using CSVSource it is very important to convert the values
    # to the correct type before inserting them into a table through pygrametl
    region_file_handle = open('region.csv', 'r', 16384)
    region_source = CSVSource(f=region_file_handle, delimiter=',')

An object must then be created for each dimension and fact table in the data
warehouse. pygrametl provides many types of abstractions for dimensions and
fact tables, but in this example, we use the simplest ones. For more
information about the more advanced dimension and fact table classes, see
:ref:`dimensions` and :ref:`facttables`.

.. code-block:: python

    # An instance of Dimension is created for each dimension in the data
    # warehouse. For each dimension, the name of the database table, the
    # table's primary key, and the table's non-key columns (attributes) are
    # given. In addition, for the location dimension the subset of the
    # attributes that should be used to lookup the primary key are given. As
    # mentioned in the beginning of this guide, using named parameters is
    # strongly encouraged
    book_dimension = Dimension(
            name='book',
            key='bookid',
            attributes=['book', 'genre'])
    
    time_dimension = Dimension(
            name='time',
            key='timeid',
            attributes=['day', 'month', 'year'])
    
    location_dimension = Dimension(
            name='location',
            key='locationid',
            attributes=['city', 'region'],
            lookupatts=['city'])
    
    # A single instance of FactTable is created for the data warehouse's single
    # fact table. It is created with the name of the table, a list of columns
    # constituting the primary key of the fact table, and the list of measures
    fact_table = FactTable(
            name='facttable',
            keyrefs=['bookid', 'locationid', 'timeid'],
            measures=['sale'])

As the input dates are datetime objects and the time dimension consists of
multiple levels (day, month, and year), the datetime objects must be split into
their separate values. For this, a normal Python function is created and passed
each of the rows. As pygrametl is a Python package, data transformations can be
implemented using standard Python without any syntactic additions or
restrictions. This also means that Python's many packages can be used as part
of an ETL flow.

.. code-block:: python

    # A normal Python function is used to split the date into its parts
    def split_date(row):
        """Splits a date represented by a datetime into its three parts"""
    
        # First the datetime object is extracted from the row dictionary
        date = row['date']
        
        # Then each part is reassigned to the row dictionary. It can then be
        # accessed by the caller as the row is a reference to the dict object
        row['year'] = date.year
        row['month'] = date.month
        row['day'] = date.day
    
Finally, the data can be inserted into the data warehouse. All rows from the
CSV file are inserted into the location dimension first. This is necessary for
foreign keys to the location dimension to be computed while filling the fact
table. The other two dimensions are filled while inserting the facts as the
data needed is included in the sales records. To ensure that the data is
committed to the database and that the connection is closed correctly, the
methods :meth:`.ConnectionWrapper.commit` and :meth:`.ConnectionWrapper.close`
are executed at the end.

.. code-block:: python

    # The Location dimension is filled with data from the CSV file as the file
    # contains all the information required for both columns in the table. If
    # the dimension was filled using data from the sales database, it would be
    # necessary to update the region attribute with data from the CSV file
    # later. To insert the rows the method Dimension.insert() is used 
    [location_dimension.insert(row) for row in region_source]
    
    # The file handle to the CSV file can then be closed
    region_file_handle.close()
    
    # All the information needed for the other dimensions are stored in the
    # sales database. So with only a single iteration over the sales records
    # the ETL flow can split the date and lookup the three dimension keys
    # needed for the fact table. While retrieving the dimension keys, pygrametl
    # can automatically update the dimensions with new data if ensure() is
    # used. This method combines a lookup with a insertion so a new row is only
    # inserted into the dimension or fact table if it does not yet exist
    for row in sale_source:
    
        # The date is split into its three parts
        split_date(row)
        
        # The row is updated with the correct primary keys for each dimension, and
        # any new data are inserted into each of the dimensions at the same time
        row['bookid'] = book_dimension.ensure(row)
        row['timeid'] = time_dimension.ensure(row)
        
        # Dimension.ensure() is not used for the location dimension as it has
        # already been filled. Instead the method Dimension.lookup() is used.
        # Dimension.lookup() does not insert any data and returns None if a row
        # with the correct lookupatts is not available. This makes error
        # handling very simple to implement. In this case an error is raised if
        # a location is missing from the CSV file as recovery is not possible
        row['locationid'] = location_dimension.lookup(row)
        if not row['locationid']:
            raise ValueError("city was not present in the location dimension")
        
        # As the number of sales is already aggregated in the sales records, the
        # row can now be inserted into the data warehouse. If aggregation, or
        # other more advanced transformations are required, the full power
        # Python is available as shown with the call to split_date()
        fact_table.insert(row)
    
    # After all the data have been inserted, the connection is ordered to
    # commit and is then closed. This ensures that the data is committed to the
    # database and that the resources used by the connection are released
    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
    
    # Finally, the connection to the sales database is closed
    sale_conn.close()

This small example shows how to quickly create a very simple ETL flow with
pygrametl. A combined version with fewer comments can be seen below. However,
as stated since this is a very small and simple example, the caching and bulk
loading built into some of the more advanced dimension and fact table classes
has not been used. In anything but very small ETL flows, these should be used
to significantly increase the throughput of an ETL flow. See :ref:`dimensions`
and :ref:`facttables` for more information. The simple parallel capabilities of
pygrametl can also be used to further increase the throughput of an ETL program
(see :ref:`parallel`), and the correctness of an ETL flow should be checked
using a set of automated repeatable tests (see :ref:`testing`).


.. code-block:: python

    import psycopg2
    import sqlite3
    import pygrametl
    from pygrametl.datasources import SQLSource, CSVSource
    from pygrametl.tables import Dimension, FactTable
    
    # Opening of connections and creation of a ConnectionWrapper.
    sale_conn = sqlite3.connect("sale.sqlite",
            detect_types=sqlite3.PARSE_DECLTYPES)
    
    dw_string = "host='localhost' dbname='dw' user='dwuser' password='dwpass'"
    dw_conn = psycopg2.connect(dw_string)
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_conn)
    
    # Creation of data sources for the sales database and the CSV file,
    # containing extra information about cities and regions in Denmark
    name_mapping = 'book', 'genre', 'city', 'date', 'sale'
    query = "SELECT book, genre, store, date, sale FROM sale"
    sale_source = SQLSource(connection=sale_conn, query=query,
            names=name_mapping)
    
    region_file_handle = open('region.csv', 'r', 16384)
    region_source = CSVSource(f=region_file_handle, delimiter=',')
    
    # Creation of dimension and fact table abstractions for use in the ETL flow
    book_dimension = Dimension(
            name='book',
            key='bookid',
            attributes=['book', 'genre'])
    
    time_dimension = Dimension(
            name='time',
            key='timeid',
            attributes=['day', 'month', 'year'])
    
    location_dimension = Dimension(
            name='location',
            key='locationid',
            attributes=['city', 'region'],
            lookupatts=['city'])
    
    fact_table = FactTable(
            name='facttable',
            keyrefs=['bookid', 'locationid', 'timeid'],
            measures=['sale'])
    
    # Python function needed to split the date into its three parts
    def split_date(row):
        """Splits a date represented by a datetime into its three parts"""
    
        # Splitting of the date into parts
        date = row['date']
        row['year'] = date.year
        row['month'] = date.month
        row['day'] = date.day
    
    # The location dimension is loaded from the CSV file
    [location_dimension.insert(row) for row in region_source]
    
    # The file handle for the CSV file can then be closed
    region_file_handle.close()
    
    # Each row in the sales database is iterated through and inserted
    for row in sale_source:
    
        # Each row is passed to the date split function for splitting
        split_date(row)
    
        # Lookups are performed to find the key in each dimension for the fact
        # and if the data is not there, it is inserted from the sales row
        row['bookid'] = book_dimension.ensure(row)
        row['timeid'] = time_dimension.ensure(row)
    
        # The location dimension is pre-filled, so a missing row is an error
        row['locationid'] = location_dimension.lookup(row)
        if not row['locationid']:
            raise ValueError("city was not present in the location dimension")
    
        # The row can then be inserted into the fact table
        fact_table.insert(row)
    
    # The data warehouse connection is then ordered to commit and close
    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
    
    # Finally, the connection to the sales database is closed
    sale_conn.close()
