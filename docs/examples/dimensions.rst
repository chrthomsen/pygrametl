.. _dimensions:

Dimensions
==========
Multiple abstractions for representing dimensions in a data warehouse are
provided by pygrametl. These abstractions allow for simple modeling of both
star and snowflake schemas as well as type 1, type 2, and combined type 1 and
type 2 slowly changing dimensions. The abstractions can be used for both
sequential and parallel loading of data into dimensions. For more information
about the parallel capabilities of pygrametl see :ref:`parallel`. The following
examples use PostgreSQL as the RDBMS and psycopg2 as the database driver.

All of following classes are currently implemented in the
:mod:`.pygrametl.tables` module.

Dimension
---------
:class:`.Dimension` is the simplest abstraction pygrametl provides for
interacting with dimensions in a data warehouse. It provides an interface for
performing operations on the underlying table, such as insertions or looking up
keys, while abstracting away the database connection and queries. Using
:class:`.Dimension` is a two-step process. Firstly, an instance of
:class:`.ConnectionWrapper` must be created with a :PEP:`249` database
connection. The first :class:`.ConnectionWrapper` created is automatically set
as the default and used by :class:`.Dimension` for interacting with the
database. For more information about database connections see :ref:`database`.
Secondly, an instance of :class:`.Dimension` must be created for each dimension
in the data warehouse. To create an instance of :class:`.Dimension` the name of
the table must be specified, along with the primary key of the table, as well as
the non-key columns in the table. In addition to these required parameters, a
subset of columns to be used for looking up keys can also be specified, as well
as a function for computing the primary key, a default return value if a lookup
fails, and a function for expanding a row automatically.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import Dimension

    # Input is a list of rows which in pygrametl is modeled as dicts
    products = [
	{'name': 'Calvin and Hobbes 1', 'category': 'Comic', 'price': '10'},
	{'name': 'Calvin and Hobbes 2', 'category': 'Comic', 'price': '10'},
	{'name': 'Calvin and Hobbes 3', 'category': 'Comic', 'price': '10'},
	{'name': 'Cake and Me', 'category': 'Cookbook', 'price': '15'},
	{'name': 'French Cooking', 'category': 'Cookbook', 'price': '50'},
	{'name': 'Sushi', 'category': 'Cookbook', 'price': '30'},
	{'name': 'Nineteen Eighty-Four', 'category': 'Novel', 'price': '15'},
	{'name': 'The Lord of the Rings', 'category': 'Novel', 'price': '60'}
    ]

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The instance of Dimension connects to the table product in the
    # database using the default connection wrapper created above, the
    # argument lookupatts specifies the column which needs to match
    # when doing a lookup for the key from this dimension
    productDimension = Dimension(
	name='product',
	key='productid',
	attributes=['name', 'category', 'price'],
	lookupatts=['name'])

    # Filling a dimension is simply done by using the insert method
    for row in products:
	productDimension.insert(row)

    # Ensures that the data is committed and the connection is closed correctly
    conn.commit()
    conn.close()

In the above example, a set of rows with product information is loaded into the
product dimension, using an instance of :class:`.Dimension` created with
information about the table in the database. The list of product information can
then be inserted into the database one element at a time using the method
:meth:`.Dimension.insert()`. Then the transaction must be committed and the
connection closed to ensure that the data is written to the database.

CachedDimension
---------------
:class:`.CachedDimension` extends :class:`.Dimension` with a cache to reduce the
latency of lookups by reducing the number of round trips to the database. To
control what is cached, three additional parameters have been added to its
constructor. The parameter :attr:`.prefill` indicates that the cache should be
filled with data from the database when the object is created, while
:attr:`.cachefullrows` determines whether only the primary key and columns
defined by :attr:`.lookuparts`, or entire rows should be cached. Lastly, the
parameter :attr:`.cacheoninsert` specifies if newly inserted rows should be
cached. To ensure that the cache is kept consistent, the RDBMS is not allowed
to modify the rows in any way, a default value set by the RDBMS is an example of
a simple-to-miss violation of this.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.datasources import CSVSource
    from pygrametl.tables import CachedDimension, FactTable

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(pgconn)

    # The cached dimension is initialized with data from the product table in
    # the database, allowing for more efficient lookups of keys for the fact
    # table, at the cost of requiring it to already contain the necessary data
    productDimension = CachedDimension(
	name='product',
	key='productid',
	attributes=['name', 'category', 'price'],
	lookupatts=['name'],
	prefill=True)

    # A similar abstraction is created for the data warehouse fact table
    factTable = FactTable(
	name='facttable',
	measures=['sales'],
	keyrefs=['storeid', 'productid', 'dateid'])

    # A CSV file contains information about each product sold by a store
    sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter='\t')

    # Looking up keys from the product dimension is done using the lookup
    # method with the information read from the sales.csv file. The second
    # argument renames the column product_name from the CSV file to name
    for row in sales:

	# Using only the attributes defined as lookupatts, lookup() checks if a
	# row with matching values are present in the cache. Only if a match
	# cannot be found in the cache does lookup() check the database table.
	row['productid'] = productDimension.lookup(row, {'name': 'product_name'})
	factTable.insert(row)

    # Ensures that the data is committed and the connection is closed correctly
    conn.commit()
    conn.close()

The example shows how to utilize :class:`.CachedDimension` to automatically
improve the performance of :meth:`.CachedDimension.lookup`. The
:class:`.CachedDimension` caches the values from the product dimension locally,
allowing increased performance when looking up keys as fewer, or none if all
rows are cached, round trips are made to the database.

BulkDimension
-------------
:class:`.BulkDimension` is a dimension optimized for high throughput when
inserting rows and fast lookups. This is done by inserting rows in bulk from a
file while using an in-memory cache for lookup. To support this the RDBMS is
not allowed to modify the rows in any way, as this would make the cache and the
database table inconsistent. Another aspect of :class:`.BulkDimension` is that
:meth:`.BulkDimension.update` and :meth:`.BulkDimension.getbyvals` calls
:meth:`.BulkDimension.endload` which inserts all rows stored in the file into
the database using a user-defined bulk loading function. Thus, calling these
functions often will negate the benefit of bulk loading. The method
:meth:`.BulkDimension.getbykey` also forces :class:`.BulkDimension` to bulk load
by default but can use the cache if :attr:`.cachefullrows` is enabled at the
cost of additional memory. :meth:`.BulkDimension.lookup` and
:meth:`.BulkDimension.ensure` will always use the cache and do not invoke any
database operations as :class:`.BulkDimension` never evicts rows from its cache.
If the dimension is too large to be cached in memory, the class
:class:`.CachedBulkDimension` should be used instead as it supports bulk loading
using a finite cache. To support bulk loading from a file on disk, multiple
additional parameters have been added to :class:`.BulkDimension` constructor.
These provide control over the temporary file used to store rows, such as
specific delimiters and the number of rows to be bulk loaded. All of these
parameters have a default value except for :attr:`.bulkloader`. This parameter
must be passed a function to be called for each set of rows to be bulk loaded,
this is necessary as the exact way to perform bulk loading differs from RDBMS to
RDBMS.

.. py:function:: func(name, attributes, fieldsep, rowsep, nullval, filehandle):

    Required signature of a function bulk loading data from a file into an RDBMS
    in pygrametl. For more information about bulk loading see
    :ref:`bulkloading`.

    **Arguments:**

    - name: the name of the dimension table in the data warehouse.
    - attributes: a list containing the sequence of attributes in the dimension
      table.
    - fieldsep: the string used to separate fields in the temporary file.
    - rowsep: the string used to separate rows in the temporary file.
    - nullval: if the :class:`.BulkDimension` was passed a string to substitute
      None values with, then it will be passed, if not then None is passed.
    - filehandle: either the name of the file or the file object itself,
      depending upon on the value of :attr:`.BulkDimension.usefilename`. Using
      the filename is necessary if the bulk loading is invoked through SQL
      (instead of directly via a method on the PEP249 driver). It is also
      necessary if the bulkloader runs in another process.


.. code-block:: python

    import sqlite3
    import psycopg2
    import pygrametl
    from pygrametl.datasources import SQLSource
    from pygrametl.tables import BulkDimension

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)


    # This function bulk loads a file into PostgreSQL using psycopg2
    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
	cursor = conn.cursor()
	# psycopg2 does not accept the default value used to represent NULL
	# bv BulkDimension, which is None. Here this is ignored as we have no
	# NULL values that we wish to substitute for a more descriptive value
	cursor.copy_from(file=filehandle, table=name, sep=fieldsep,
			 columns=attributes)


    # In addition to arguments needed for a Dimension, a reference to the
    # bulk loader defined above must also be passed to BulkDimension
    productDimension = BulkDimension(
	name='product',
	key='productid',
	attributes=['name', 'category', 'price'],
	lookupatts=['name'],
	bulkloader=pgbulkloader)

    # A PEP249 connection is sufficient for an SQLSource so we do not need
    # to create a new instance of ConnectionWrapper to read from the database
    sqconn = sqlite3.connect('product_catalog.db')

    # Encapsulating a database query in an SQLSource allows it to be used as an
    # normal iterator, making it very simple to load the data into another table
    sqlSource = SQLSource(connection=sqconn, query='SELECT * FROM product')

    # Inserting data from a data source into a BulkDimension is performed just
    # like any other dimension type in pygrametl, as the interface is the same
    for row in sqlSource:
	productDimension.insert(row)

    # Ensures that the data is committed and the connections are closed correctly
    conn.commit()
    conn.close()
    sqconn.close()

The example above shows how to use :class:`.BulkDimension` to efficiently load
the contents of a local SQLite database into a data warehouse dimension. This
process is a good use case for :class:`.BulkDimension` as
:meth:`.BulkDimension.update`, :meth:`.BulkDimension.getbykey` and
:meth:`.BulkDimension.getbyval` are not used, so no additional calls to
:meth:`.BulkDimension.endload` are made. By bulk loading the rows from a file
using :meth:`copy_from` instead of inserting them one at a time, the time
required to load the dimension is significantly reduced. However, it is
important that :meth:`.ConnectionWrapper.commit()` is executed after all rows
have been inserted into :class:`.BulkDimension` as it ensures that the last set
of rows are bulk loaded by calling :meth:`.BulkDimension.endload` on all tables.
A downside of :class:`.BulkDimension` is that it caches the entire dimension in
memory. If the dimension can be bulk loaded but is too large to cache in memory
:class:`.CachedBulkDimension` should be used instead of :class:`.BulkDimension`.


CachedBulkDimension
-------------------
:class:`.CachedBulkDimension` is very similar to :class:`.BulkDimension` and is
also intended for bulk loading a dimension, so only their differences are
described here. Unlike :class:`.BulkDimension` the size of
:class:`.CachedBulkDimension` cache is limited by the parameter
:attr:`cachesize`. This allows it to be used with a dataset too large to be
cached entirely in memory. The trade-off is that
:meth:`.CachedBulkDimension.lookup` and :meth:`.CachedBulkDimension.ensure`
sometimes have to lookup keys in the database instead of always using the cache.
However, the method :meth:`.CachedBulkDimension.getbykey` also no longer needs
to call :meth:`.CachedBulkDimension.endload` if :attr:`.cachefullrows` is not
enabled. This is because :class:`.CachedBulkDimension` caches the rows currently
in the file in a separate cache. All rows in the file are cached as there is no
guarantee that the cache storing rows from the database does not evict rows
currently in the file but not yet in the database when the cache is full, So an
additional cache is needed to ensure that :meth:`.CachedBulkDimension.lookup`
and :meth:`.CachedBulkDimension.getbykey` can locate rows before they are loaded
into the database. :meth:`.CachedBulkDimension.insert` caches rows in the file
cache, and only when the rows in the file are loaded into the database are they
moved to the database row cache, in which :meth:`.CachedBulkDimension.lookup`
also stores rows if the method had to query the database for them.

Due to the use of two caches, caching in :class:`.CachedBulkDimension` is
controlled by two parameters. The parameter :attr:`.cachesize` can be set to
control the size of the cache for rows loaded into the database, while the
parameter :attr:`.bulksize` controls the number of rows stored in the file
before the dimension bulk loads. As the rows in the file are all cached in a
separate cache, the memory consumption will change in correspondence to both of
these values.

.. note::
    If rows with matching :attr:`lookupatts` are passed to :meth:`insert`
    without bulk loading occurring between the calls, only the first row will be
    loaded into the dimension. All calls to :meth:`insert` after the first one
    will just return the key of the first row as it is stored in the file cache.

.. The space in the header is intentional so the two parts can fit in the toc

TypeOneSlowlyChanging Dimension
-------------------------------
:class:`.TypeOneSlowlyChangingDimension` allows the creation of a type 1 slowly
changing dimension. The dimension is based on :class:`.CachedDimension`, albeit
with a few differences. The primary difference between the two classes besides
the additional method :meth:`.TypeOneSlowlyChangingDimension.scdensure`, is that
:class:`.TypeOneSlowlyChangingDimension` always caches rows when they are
inserted. This is done to minimize the number of round trips to the database
needed for :meth:`.TypeOneSlowlyChangingDimension.scdensure` to increase its
throughput. The user must also specify the sequence of attributes to use for
looking up keys as :attr:`.lookupatts`, and can optionally specify the sequence
of type 1 slowly changing attributes :attr:`.type1atts`. If :attr:`.type1atts`
is not given it will default to all attributes minus :attr:`.lookupatts`. The
sequences :attr:`.lookupatts` and :attr:`.type1atts` must be disjoint and an
error will be raised if they are not. As caching is used to increase to speedup
lookups, it is assumed that the database does not change or add any attribute
values to the rows. For example, a default value set by RDBMS and automatic type
coercion can break this assumption.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import TypeOneSlowlyChangingDimension

    # Input is a list of rows which in pygrametl is modeled as dicts
    products = [
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '10'},
	{'name': 'Cake and Me', 'category': 'Cookbook', 'price': '15'},
	{'name': 'French Cooking', 'category': 'Cookbook', 'price': '50'},
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '20'},
	{'name': 'Sushi', 'category': 'Cookbook', 'price': '30'},
	{'name': 'Nineteen Eighty-Four', 'category': 'Novel', 'price': '15'},
	{'name': 'The Lord of the Rings', 'category': 'Novel', 'price': '60'},
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '10'}
    ]

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # TypeOneSlowlyChangingDimension is created with price as a changing attribute
    productDimension = TypeOneSlowlyChangingDimension(
	name='product',
	key='productid',
	attributes=['name', 'category', 'price'],
	lookupatts=['name'],
	type1atts=['price'])

    # scdensure determines whether the row already exists in the database
    # and inserts a new row or updates any type1atts that have changed
    for row in products:
	productDimension.scdensure(row)

    # Ensures that the data is committed and the connections are closed correctly
    conn.commit()
    conn.close()

The above example shows a scenario where the price of a product changes over
time. The instance of :class:`.TypeOneSlowlyChangingDimension` automatically
checks if a product already exists, and if it does, updates the price if the old
and new values differ. As a type 1 slowly changing dimension does store the
history of the changes only the value of the last row to be inserted will be
stored, so the rows most be loaded in chronological order. If the history of the
changes must be stored a type 2 slowly changing dimension should be created, and
:class:`.SlowlyChangingDimension` should be used instead of
:class:`.TypeOneSlowlyChangingDimension`.


SlowlyChangingDimension
-----------------------
:class:`.SlowlyChangingDimension` allows for the creation of either a type 2
slowly changing dimension, or a combined type 1 and type 2 slowly changing
dimension. To support this functionality, multiple additional attributes have
been added to :class:`.SlowlyChangingDimension` compared to :class:`.Dimension`.
However, only the additional parameter :attr:`.versionatt` is required when
creating a :class:`.SlowlyChangingDimension`. This parameter indicates which of
the dimensions attribute stores the row's version number. The method
:meth:`.SlowlyChangingDimension.scdensure` updates the table while taking the
slowly changing aspect of the dimension into account. If the row is already
available then the primary key is returned, if the row is not available then it
is inserted into the dimension, and if an attributes have changed a new version
is created. The method :meth:`.SlowlyChangingDimension.lookup` is also changed
slightly as it returns the latest version of a row. To improve the performance
of lookups for a slowly changing dimension, caching is used, which assumes that
the database does not modify any values in the inserted rows; an assumption that
the use of default values can break.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import SlowlyChangingDimension

    # Input is a list of rows which in pygrametl is modeled as dicts
    products = [
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '20',
	 'date': '1990-10-01'},
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '10',
	 'date': '1990-12-10'},
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '20',
	 'date': '1991-02-01'},
	{'name': 'Cake and Me', 'category': 'Cookbook', 'price': '15',
	 'date': '1990-05-01'},
	{'name': 'French Cooking', 'category': 'Cookbook', 'price': '50',
	 'date': '1990-05-01'},
	{'name': 'Sushi', 'category': 'Cookbook', 'price': '30',
	 'date': '1990-05-01'},
	{'name': 'Nineteen Eighty-Four', 'category': 'Novel', 'price': '15',
	 'date': '1990-05-01'},
	{'name': 'The Lord of the Rings', 'category': 'Novel', 'price': '60',
	 'date': '1990-05-01'}
    ]

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # This slowly changing dimension is created as type 2 only. Meaning that a
    # new row only changes the validto attribute in the previous row. validto
    # is a timestamp indicating when the row is no longer valid. As additional
    # parameters, the object is initialized with information about which
    # attribute holds a timestamp for when the row's validity starts and ends.
    # The parameter fromfinder is also given, which is set to a function that
    # computes the timestamp for when the row becomes valid. In this example,
    # the function datareader from pygrametl is used which converts a timestamp
    # from a str to a datetime.date which PostgresSQL stores as Date type.
    productDimension = SlowlyChangingDimension(
	name='product',
	key='productid',
	attributes=['name', 'category', 'price', 'validfrom', 'validto',
		    'version'],
	lookupatts=['name'],
	fromatt='validfrom',
	fromfinder=pygrametl.datereader('date'),
	toatt='validto',
	versionatt='version')

    # scdensure extends the ensure methods with support for updating slowly
    # changing attributes of rows where lookupparts match. This is done by
    # increamenting the version attribute for the new row, and assigning the new
    # rows fromatt to the old rows toatt, indicating that the old row is no
    # longer valid.
    for row in products:
	productDimension.scdensure(row)

    # Ensures that the data is committed and the connections are closed correctly
    conn.commit()
    conn.close()


As the values of the product dimension, in this case, have changing prices, a
:class:`.SlowlyChangingDimension` is used to automate the changes a new row
might incur on an existing row. The product information itself is also extended
with timestamps indicating at what time a particular product had a certain
price. When creating the instance of :class:`.SlowlyChangingDimension`
information about how these timestamps should be interpreted is provided to the
constructor. In this case, is it fairly simple as the timestamp provided in the
input data is simple enough to be converted directly to :class:`.datetime.date`
object. These can then be inserted into a column of type Date. To automate this
conversion, the parameter :attr:`fromfinder` is set to the function returned by
:func:`.pygrametl.datareader` which constructs :class:`.datetime.date` objects
from the :class:`.str` in date. However, a user-defined function with the same
interface as the function generated by :func:`.pygrametl.datareader` could also
be used. When inserting the rows the method
:meth:`.SlowlyChangingDimension.scdensure` is used instead of
:meth:`.SlowlyChangingDimension.insert` as it first performs a lookup to verify
that an existing version of the row is not already present. If a row is already
present, this row is updated with the from timestamp inserted into its to time
attribute indicating when this version of the row was deemed obsolete, and an
incremented version number is added to the new row indicating that this is a
newer version of an existing row.

SnowflakedDimension
-------------------
:class:`.SnowflakedDimension` represents a snowflaked dimension as a single
object with the same interface as :class:`.Dimension`. Instantiating a
:class:`.SnowflakedDimension` is however different. Instead of requiring all
arguments to be passed to the constructor of :class:`.SnowflakedDimension`
itself, dimension objects must be created for each table in the snowflaked
dimension. These objects are then passed to :class:`.SnowflakedDimension`
constructor. The dimension objects must be given as pairs that indicate their
foreign key relationships, e.g. (a1, a2) should be passed if a1 has a foreign
key to a2. Currently, it is a requirement that a foreign key must have the same
name as the primary key it references. The only additional configuration
supported by :class:`.SnowflakedDimension` is :attr:`.expectboguskeyvalues`
which indicates that a key used as a lookup attribute in a lower level of the
hierarchy might not have a matching primary key. Support for slowly changing
dimensions of type 2 or a combined of type 1 and type 2 are provided if an
instance of :class:`.SlowlyChangingDimension` is at the root of the snowflaked
dimension. Currently, only the root dimension can be an instance of
:class:`.SlowlyChangingDimension` and the feature should be considered
experimental.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import CachedDimension, SnowflakedDimension

    # Input is a list of rows which in pygrametl is modeled as dicts
    products = [
	{'name': 'Calvin and Hobbes 1', 'category': 'Comic',
	 'type': 'Fiction', 'price': '10'},
	{'name': 'Calvin and Hobbes 2', 'category': 'Comic',
	 'type': 'Fiction', 'price': '10'},
	{'name': 'Calvin and Hobbes 3', 'category': 'Comic',
	 'type': 'Fiction', 'price': '10'},
	{'name': 'Cake and Me', 'category': 'Cookbook',
	 'type': 'Non-Fiction', 'price': '15'},
	{'name': 'French Cooking', 'category': 'Cookbook',
	 'type': 'Non-Fiction', 'price': '50'},
	{'name': 'Sushi', 'category': 'Cookbook',
	 'type': 'Non-Fiction', 'price': '30'},
	{'name': 'Nineteen Eighty-Four', 'category': 'Novel',
	 'type': 'Fiction', 'price': '15'},
	{'name': 'The Lord of the Rings', 'category': 'Novel',
	 'type': 'Fiction', 'price': '60'}
    ]

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The product dimension is in the database represented as a snowflaked
    # dimension, so a CachedDimension object is created for each table
    productTable = CachedDimension(
	name='product',
	key='productid',
	attributes=['name', 'price', 'categoryid'],
	lookupatts=['name'])

    categoryTable = CachedDimension(
	name='category',
	key='categoryid',
	attributes=['category', 'typeid'],
	lookupatts=['category'])

    typeTable = CachedDimension(
	name='type',
	key='typeid',
	attributes=['type'])

    # An instance of SnowflakedDimension is initialized with the created
    # dimensions as input. Thus allowing a snowflaked dimension to be used in
    # the same manner as a dimension consisting of a single table. The dimension's
    # tables are passed as pairs based on their foreign key relationships.
    # Meaning the arguments indicate that the productTable has a foreign key
    # relationship with the categoryTable, and the categoryTable has a foreign
    # key relationship with the typeTable. If a table has multiple foreign key
    # relationships to tables in the snowflaked dimension, a list must be passed
    # as the second part of the tuple with a dimension object for each table the
    # first argument references through its foreign keys.
    productDimension = SnowflakedDimension(references=[
	(productTable, categoryTable), (categoryTable, typeTable)])

    # SnowflakedDimension provides the same interface as the dimensions classes,
    # however, some of its methods stores keys in the row when they are computed
    for row in products:
	productDimension.insert(row)

    # Ensures that the data is committed and the connections are closed correctly
    conn.commit()
    conn.close()


In the above example, the product dimension is not represented by a single table
like in the examples shown for the other dimensions classes provided by
pygrametl. It is instead represented as a snowflake consisting of multiple
tables. To represent this in the ETL flow, a combination of
:class:`.SnowflakedDimension` and :class:`.CachedDimension` is used. As multiple
tables need to be represented, an instance of :class:`.CachedDimension` is
created for each. An instance of :class:`.SnowflakedDimension` is then created
to represent the many instances of :class:`.CachedDimension` as a single object.
Interacting with a snowflaked dimension is done through the same interface
provided by the other dimension classes in pygrametl. However, some of
:class:`.SnowflakedDimension` methods modify the provided rows as foreign key
relationship needs to be computed based on the contents of the rows the object
operates on.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import CachedDimension, SnowflakedDimension, \
	SlowlyChangingDimension

    # Input is a list of rows which in pygrametl is modeled as dicts
    products = [
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '20',
	 'date': '1990-10-01'},
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '10',
	 'date': '1990-12-10'},
	{'name': 'Calvin and Hobbes', 'category': 'Comic', 'price': '20',
	 'date': '1991-02-01'},
	{'name': 'Cake and Me', 'category': 'Cookbook', 'price': '15',
	 'date': '1990-05-01'},
	{'name': 'French Cooking', 'category': 'Cookbook', 'price': '50',
	 'date': '1990-05-01'},
	{'name': 'Sushi', 'category': 'Cookbook', 'price': '30',
	 'date': '1990-05-01'},
	{'name': 'Nineteen Eighty-Four', 'category': 'Novel', 'price': '15',
	 'date': '1990-05-01'},
	{'name': 'The Lord of the Rings', 'category': 'Novel', 'price': '60',
	 'date': '1990-05-01'}
    ]

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The dimension is snowflaked into two tables, one with categories, and the
    # other at the root with name and the slowly changing attribute price
    productTable = SlowlyChangingDimension(
	name='product',
	key='productid',
	attributes=['name', 'price', 'validfrom', 'validto', 'version',
		    'categoryid'],
	lookupatts=['name'],
	fromatt='validfrom',
	fromfinder=pygrametl.datereader('date'),
	toatt='validto',
	versionatt='version')

    categoryTable = CachedDimension(
	name='category',
	key='categoryid',
	attributes=['category'])

    productDimension = SnowflakedDimension(references=[(productTable,
							categoryTable)])

    # Using a SlowlyChangingDimension with a SnowflakedDimension is done in the
    # same manner as a normal SlowlyChangingDimension using scdensure
    for row in products:
	productDimension.scdensure(row)

    # Ensures that the data is committed and the connections are closed correctly
    conn.commit()
    conn.close()

A :class:`.SlowlyChangingDimension` and a :class:`.SnowflakedDimension` can be
combined if necessary, with the restriction that all slowly changing attributes
must be placed in the root table. To do this, the :class:`.CachedDimension`
instance connecting to the root table has to be changed to an instance of
:class:`.SlowlyChangingDimension` and the necessary attributes added to the
database table. Afterward, :meth:`.SnowflakedDimension.scdensure` can be used to
insert and lookup rows while ensuring that the slowly changing attributes are
updated correctly.
