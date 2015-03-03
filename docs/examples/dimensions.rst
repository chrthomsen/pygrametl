.. _dimensions:

Dimensions
==========
Multiple abstractions for representing dimensions in a data warehouse is
provided by *pygrametl*, this is to allow for simple modelling of both star and
snowflake schemas as well as type 2 or a combined type 1 and type 2 slowly
changing dimension. A slowly changing dimension that is only type 1 is
currently not supported. The abstractions can be used both for serial and
parallel loading of data into the dimensions. For more information about the
parallel capabilities of pygrametl see :ref:`parallel`. In the following
examples we use PostgreSQL as a database management system and psycopg2 as the
database driver.

All of these classes are currently implemented in the  
:mod:`.pygrametl.tables` module.

Dimension
---------
:class:`.Dimension` is the simplest abstraction pygrametl provides for
interaction with dimensions in a data warehouse. For each of the dimensions in
a data warehouse a instance of :class:`.Dimension` is created, which provides
an interface for performing operations on the table, such as insertions or
looking up keys, while abstracting away the database connection and queries.
Using :class:`.Dimension` is a two-step process. First, an instance of
:class:`.ConnectionWrapper` is created with a :PEP:`249` database connection
which is automatically set as the default connection and used by
:class:`.Dimension` for interacting with the database. For more information
about database connections see :ref:`database`. Second, the name of the table
must be specified, along with the primary key of the table, as well as the
columns in the table. In addition to these required parameters, a subset of
columns to be used for looking up keys can also be specified, as well as a
function for computing the primary key, a default return value if a lookup
fails, and a function for expanding a row automatically. 

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import Dimension 

    # Input is a list of "rows" which in pygrametl is modelled as dict
    products = [
        {'name' : 'Calvin and Hobbes 1', 'category' : 'Comic', 'price' : '10'},
        {'name' : 'Calvin and Hobbes 2', 'category' : 'Comic', 'price' : '10'},
        {'name' : 'Calvin and Hobbes 3', 'category' : 'Comic', 'price' : '10'},
        {'name' : 'Cake and Me', 'category' : 'Cookbook', 'price' : '15'},
        {'name' : 'French Cooking', 'category' : 'Cookbook', 'price' : '50'},
        {'name' : 'Sushi', 'category' : 'Cookbook', 'price' : '30'},
        {'name' : 'Nineteen Eighty-Four', 'category' : 'Novel', 'price' : '15'},
        {'name' : 'The Lord of the Rings', 'category' : 'Novel', 'price' : '60'}
    ]

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser' 
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used.
    # A reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The instance of Dimension connects to the table "product" in the 
    # database using the default connection wrapper we just created, the 
    # argument "lookupatts" specifies the column which needs to match
    # when doing a lookup of the key from this dimension
    productDimension = Dimension(
        name='product',
        key='productid',
        attributes=['name', 'category', 'price'],
        lookupatts=['name'])

    # Filling a dimension is simply done by using the insert method
    for row in products:
        productDimension.insert(row)
    conn.commit()
    conn.close()

In this very simple example, a set of rows with product information is loaded
into the product dimension, using an instance of :class:`.Dimension` created
with information on the table in database. The list of product information can
then be inserted into the database using the method
:meth:`.Dimension.insert()`. Afterwards the database must be committed and the
transaction closed to ensure that the data is correctly written to the
database.

CachedDimension
---------------
:class:`.CachedDimension` expands the standard dimension with a cache, allowing
for lower latency when when performing lookups as the number of round trips to
the database can be decreased. To control what is cached, three additional
parameters have been added to the initialiser method. The parameter `prefill`
indicates that the cache should be filled with data from the database on
initialisation, while `cachefullrows` determines whether only the primary key
and columns defined by `lookuparts`, or entire rows should be cached. Lastly
the parameter `cacheoninsert` specifies if newly inserted rows should be
cached. To ensure that the cache is kept consistent, no changes or additions
should be performed on the rows by the database, a default value set by the
database is an example of a simple-to-miss violation of this.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.datasources import CSVSource 
    from pygrametl.tables import CachedDimension, FactTable

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser' 
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used.
    # A reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(pgconn)

    # The cached dimension is initialised with data from the product table in
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
        keyrefs=['storeid', 'productid', 'dayid'])

    # The CSV file containing information about products sold in stores
    # Looking up keys from the product dimension is done using the lookup
    # method with the information read from the sales.csv file. The second
    # argument renames the column product_name from the CSV file to name
    for row in sales:
        
        # Looking up a key in the cached dimension checks if a row containing
        # a matching value of the attributes defined as lookupatts is present, 
        # if a match cannot be found the actual database table is checked for 
        # a match
        row['productid'] = productDimension.lookup(row, {"name":"product_name"})
        factTable.insert(row)

    # To ensure that all information is loaded and that the database connection 
    # is terminated correctly the current transaction should be committed 
    conn.commit()
    conn.close()

The example shows how to utilise :class:`.CachedDimension` to improve
performance of `lookup` when finding the value of a key for insertion into the
fact table. The :class:`.CachedDimension` caches the values from the product
dimension locally, allowing increased performance when looking up keys as
fewer, or none if all rows are cached, round trips are made to the database.

BulkDimension
-------------
:class:`.BulkDimension` is a dimension specialised for increased throughput
when performing insertions by inserting rows in bulk from a file, in addition
to quick lookups through an in-memory cache. To support, this the database must
not perform transformations in order to not create inconsistencies between the
cache and the database table. Another aspect of :class:`.BulkDimension` is that
`update`, `getbyvals`, and `getbykey` by default forces a call to `endload`
which inserts all cached values into the database using a user defined bulk
loading function, so calling these functions might often lead to a decrease in
performance. Calls of lookup and ensure will only use the cache and does not
invoke any database operations.  To support caching to disk, multiple
additional parameters have been added to the class initialiser method allowing
control of the temporary file used to store rows, such as specific delimiters
and the number of facts to be bulk loaded. All of these parameters provide a
default value except for :attr:`.bulkloader`.  This parameter must be passed a
function to be called for each batch of rows to be loaded, this is necessary as
the exact way to perform bulk loading differs from DBMS to DBMS. 

.. py:function:: func(name, attributes, fieldsep, rowsep, nullval, filehandle):

    Expected signature of a bulk loader function passed to 
    :class:`.BulkDimension`. See the API documentation for more information.

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

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used.
    # A reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # How to perform the bulk loading using psycopg2 is defined as this function 
    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        cursor = conn.cursor()
        # psycopg2 does not accept the default value used for null substitutes
        # bv BulkDimension, which is None, so we just ignore it as we have no 
        # null values that we wish to substitute for a more descriptive value
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep, 
                         columns=attributes)

    # In addition to arguments needed for a Dimension, a reference to the
    # bulk loader defined above must also be passed, so a BulkDimension 
    # can use it
    productDimension = BulkDimension(
        name='product',
        key='productid',
        attributes=['name', 'category', 'price'],
        lookupatts=['name'],
        bulkloader=pgbulkloader)

    # A PEP249 connection is sufficient for an SQLSource so we do not need 
    # to create a new instance of ConnectionWrapper to read from the database
    sqconn = sqlite3.connect("product_catalog.db")

    # Encapsulating a database query in an SQLSource allows it to be used as an
    # normal iterator, making it very simple to load the data into another table
    sqlSource = SQLSource(connection=sqconn, query="SELECT * FROM product")

    # Inserting data from a data source into a BulkDimension is performed just
    # like any other dimension type in pygrametl, as the interface is the same 
    for row in sqlSource:
        productDimension.insert(row)

    # To ensure all cached data is inserted and the transaction committed
    # both the commit and close functions should be called when done
    conn.commit()
    conn.close()

    # The commit here is strictly not necessary as no writes have been 
    # performed, but it is performed to be sure that the connection is 
    # terminated correctly
    sqconn.commit()
    sqconn.close()

This example shows how to use :class:`.BulkDimension` to effectively load the
contents of a local SQLite database into a data warehouse dimension located on
the network. This process is a good use case for :class:`.BulkDimension` as no
calls to `Update`, `getbykey` or `getbyval` are needed so the caches can be
filled before they are loaded into the data warehouse. As the data warehouse is
located on another machine many round trips to perform single insertions to it
may become a necessary bottleneck.  The severity of this problem is decreased
by the use of local cache, as much larger amounts of data is loaded for each
round trip to the database through the use of the bulk loading function, which
uses the `copy_from` method to load multiple rows while performing a insertion
for each. A downside, however, of using :class:`.BulkDimension` to cache rows
is that some data might not be inserted into the database after when the last
row is given to the :class:`.BulkDimension` object, as data is only loaded into
the database when the cache is filled. To load the contents manually, the
method :meth:`.BulkDimension.endload()` must be called, this can quickly become
non-trivial so a simpler solution is to use the method
:meth:`.ConnectionWrapper.commit()`, which calls `endload()` and `commit()` on
all tables created anywhere in the program and commits the current database
transaction on the database which the :class:`.ConnectionWrapper` is associated
with.

.. The space in the header is intentional so the two parts can fit in the toc

TypeOneSlowlyChanging Dimension
-------------------------------
:class:`.TypeOneSlowlyChangingDimension` allows the creation of a Type 1 slowly
changing dimension.  The dimension is based on :class:`.CachedDimension`,
albeit with a few differences. The primary difference between the two classes
besides the additional method, is that :class:`.TypeOneSlowlyChangingDimension`
enables caching on insert and disables caching of full rows by default,
settings that cannot be overridden. This is done in order to minimize the
amount of database communication needed for
:meth:`.TypeOneSlowlyChangingDimension.scdensure` in an effort to increase its
throughput. The class requires a sequence of attributes for lookup
:attr:`.lookupatts`, as well as a sequence of type 1 attributes
:attr:`.type1atts`, defaults to all attributes minus lookupatts, which are the
slowly changing attributes in the dimension and these two sequences of
attributes need to be disjoint. Caching is used to increase the performance of
lookups, which assumes that the database does not change or add any attribute
values that are cached. For example, a DEFAULT value in the database or
automatic type coercion can break this assumption.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import TypeOneSlowlyChangingDimension

    # Input is a list of "rows" which in pygrametl is modelled as dict
    products = [
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '10'},
        {'name' : 'Cake and Me', 'category' : 'Cookbook', 'price' : '15'},
        {'name' : 'French Cooking', 'category' : 'Cookbook', 'price' : '50'},
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '20'},
        {'name' : 'Sushi', 'category' : 'Cookbook', 'price' : '30'},
        {'name' : 'Nineteen Eighty-Four', 'category' : 'Novel', 'price' : '15'},
        {'name' : 'The Lord of the Rings', 'category' : 'Novel', 'price' : '60'}
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '10'},
    ]

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used.
    # A reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # An instance of a Type 1 slowly changing dimension is created with 'price'
    # as a slowly changing attribute.
    productDimension = TypeOneSlowlyChangingDimension (
        name='product',
        key='productid',
        attributes=['name', 'category', 'price'],
        lookupatts=['name'],
        type1atts=['price'])

    # scdensure determines whether the row already exists in the database
    # and either inserts a new row, or updates the changed attributes in the
    # existing row.
    for row in products:
        productDimension.scdensure(row)

    # To ensure all cached data is inserted and the transaction committed
    # both the commit and close function should be called when done
    conn.commit()
    conn.close()

The values of the product dimension in this case is used to illustrate a
situation where a product changes its price. Using a
:class:`.TypeOneSlowlyChangingDimension`, the rows in the database are updated
accordingly when a change happens. As opposed to a
:class:`.SlowlyChangingDimension`, a Type 1 slowly changing dimension does not
include any history or time stamps, so it is important that the rows are
introduced in chronological order.

SlowlyChangingDimension
-----------------------
:class:`.SlowlyChangingDimension` allows for the creation of either a Type 2
slowly changing dimension, or a combined Type 1 and Type 2 slowly changing
dimension. To support this functionality, multiple additional attributes have
been added to :class:`.SlowlyChangingDimension` compared to
:class:`.Dimension`, in order to control how the slowly changing dimension
should operate. However, only the additional parameter :attr:`.versionatt` is
required for the creation of a :class:`.SlowlyChangingDimension`. This
parameter indicates which of the dimensions attribute holds version number of
the :class:`.SlowlyChangingDimension`. In addition to the methods provided by
:class:`.Dimension`, the method :meth:`.SlowlyChangingDimension.scdensure` is
also available.  This method is similar to :meth:`.Dimension.ensure` in that it
performs a combined lookup and insertion. If the row is already available then
the primary key is returned, if the row is not available, then it is inserted
into the dimension. The method :meth:`.SlowlyChangingDimension.lookup` is also
changed slightly as it returns the newest version of a particular row, instead
of just the single one available, which is the case for a regular dimension. To
improve the performance of lookups for a slowly changing dimension, caching is
used, which assumes that the database does not modify any values in the
inserted rows; an assumption the use of default values can break.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import SlowlyChangingDimension 

    # Input is a list of "rows" which in pygrametl is modelled as dict
    products = [
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '20',
         'date' : '1990-10-01'},
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '10',
         'date' : '1990-12-10'},
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '20',
         'date' : '1991-02-01'},
        {'name' : 'Cake and Me', 'category' : 'Cookbook', 'price' : '15',
         'date' : '1990-05-01'},
        {'name' : 'French Cooking', 'category' : 'Cookbook', 'price' : '50',
         'date' : '1990-05-01'},
        {'name' : 'Sushi', 'category' : 'Cookbook', 'price' : '30',
         'date' : '1990-05-01'},
        {'name' : 'Nineteen Eighty-Four', 'category' : 'Novel', 'price' : '15',
         'date' : '1990-05-01'},
        {'name' : 'The Lord of the Rings', 'category' : 'Novel', 'price' : '60',
         'date' : '1990-05-01'}
    ]

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser' 
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used.
    # A reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The slowly changing dimension is created as type 2 only, as a new row is
    # inserted with a from and to timestamps for each change in the dataset
    # without changing any attributes in the existing rows, except validto 
    # which is a time stamp indicating when the row is no longer valid.
    # As additional parameters, the object is initialised with information
    # about which attribute holds a time stamp for when the row's validity 
    # starts and ends. The parameter fromfinder is also given, which is must be
    # set to the function that should be used to compute the time stamp for 
    # when the row becomes valid and given as input the name of the row which 
    # value it should use. In this example, the function datareader from 
    # pygrametl is used which converts time stamp from a string to a Python 
    # datetime.date object to simplify the conversion to the Postgres Date type.
    productDimension = SlowlyChangingDimension (
        name='product',
        key='productid',
        attributes=['name', 'category', 'price', 'validfrom', 'validto',
                    'version'],
        lookupatts=['name'],
        fromatt='validfrom',
        fromfinder=pygrametl.datereader('date'),
        toatt='validto',
        versionatt='version')

    # scdensure extends the existing ensure methods to provide support for
    # updating slowly changing attributes for rows where lookupparts match, but
    # other differences exist. This is done by increamenting the version
    # attribute for the new row, and assigning the new rows fromatt to the old
    # rows toatt, indicating that the validity of the old row has ended.
    for row in products:
        productDimension.scdensure(row)

    # To ensure all cached data is inserted and the transaction committed
    # both the commit and close function should be called when done
    conn.commit()
    conn.close()


As the values of the product dimension in this case have changing prices, a
:class:`.SlowlyChangingDimension` is used to automate the changes a new row
might incur on an existing row. The product information itself is also extended
with time stamps indicating valid time for the price of that particular
product.  When creating the instance of :class:`.SlowlyChangingDimension`
information about how these time stamps should be interpreted is provided to
the instance. In this case is it fairly simple, as the time stamp provided in
the data is simple enough to be converted directly to :class:`.datetime.date`
object which can be inserted into the Postgres database in a column of type
Date, to automate this conversion, the parameter
:attr:`.SlowlyChangingDimension.fromfinder` is set to the function
:func:`.pygrametl.datareader` which constructs the :class:`.datetime.date`
object. However for more complicated ETL flows, a user defined function could
be created to perform more complicated creations of time stamps based on the
input data. The function in such a situation should follow the same interface
as the function generated by :func:`.pygrametl.datareader`. When performing the
actual insertion of rows the method :class:`.SlowlyChangingDimension.scdensure`
is used instead of :class:`.SlowlyChangingDimension.insert` as it first
performs a lookup to verify that an existing version of the row is not already
present. If a row is already present, this row is updated with the from
timestamp inserted into its to time attribute indicating when this version of
the row was deemed obsolete, and a incremented version number is added to the
new row indicating that this is a newer version of an existing row.

SnowflakedDimension
-------------------
:class:`.SnowflakedDimension` allows for use of a data warehouse represented as
a snowflake dimension, through the same interface as :class:`.Dimension`.
Instantiation of a :class:`.SnowflakedDimension` is however different.  Instead
of requiring all arguments to be passed to the constructor of
:class:`.SnowflakedDimension` itself, a :class:`.Dimension` object should be
created for each table in the snowflaked dimension. These objects are then
passed to the initialiser of :class:`.SnowflakedDimension` in the sequence of
the order in which tables have foreign keys to the next table, e.g. (a1, a2)
should be passed if a1 has a foreign key to a2. To support this, each foreign
key must have the same name as the primary key it references. The only
additional configuration supported by :class:`.SnowflakedDimension` is
:attr:`.expectboguskeyvalues` that indicates if a key that is used as lookup
attribute in a lower level of the hierarch does not have a matching primary
key. Support for slowly changing dimensions of Type 2 or a combined Type 1 and
Type 2 is provided by using an instance of :class:`.SlowlyChangingDimension` as
the root of snowflaked dimension instead of an instance of :class:`.Dimension`.
Currently only the root dimension need to be an instance of
:class:`.SlowlyChangingDimension` to support a slowly changing snowflaked
dimension. This feature should however be considered experimental.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import Dimension, SnowflakedDimension

    # Input is a list of "rows" which in pygrametl is modelled as dict
    products = [
        {'name' : 'Calvin and Hobbes 1', 'category' : 'Comic', 'price' : '10'},
        {'name' : 'Calvin and Hobbes 2', 'category' : 'Comic', 'price' : '10'},
        {'name' : 'Calvin and Hobbes 3', 'category' : 'Comic', 'price' : '10'},
        {'name' : 'Cake and Me', 'category' : 'Cookbook', 'price' : '15'},
        {'name' : 'French Cooking', 'category' : 'Cookbook', 'price' : '50'},
        {'name' : 'Sushi', 'category' : 'Cookbook', 'price' : '30'},
        {'name' : 'Nineteen Eighty-Four', 'category' : 'Novel', 'price' : '15'},
        {'name' : 'The Lord of the Rings', 'category' : 'Novel', 'price' : '60'}
    ]

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser' 
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used.
    # A reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The product dimension is in the database represented as a Snowflaked 
    # dimension, so a dimension object is created for each table
    productTable = Dimension(
        name='product',
        key='productid',
        attributes=['name', 'categoryid'],
        lookupatts=['name'])

    categoryTable = Dimension(
        name='category',
        key='categoryid',
        attributes=['category', 'priceid'],
        lookupatts=['category'])

    priceTable = Dimension(
        name='price',
        key='priceid',
        attributes=['price'])

    # A instance of SnowflakedDimension is initialised with the
    # created dimensions as input, creating a simple interface matching a
    # single dimension, allowing a Snowflaked dimension to be used in the same
    # manner as a dimension represented in the database by a Star schema. The
    # dimensions representing tables are passed in pairs based on their foreign
    # key relations. Meaning the arguments indicate that the productTable has
    # a foreign key relation with the categoryTable, and the categoryTable has
    # a foreign key relation with the priceTable. If a table has multiple
    # foreign key relations to tables in the Snowflaked dimension, a list must
    # be passed as the second part of the tuple with a Dimension object for
    # each table the first argument references through its foreign keys.
    productDimension = SnowflakedDimension(references=[(productTable, categoryTable), 
                                            (categoryTable, priceTable)])

    # Using a SnowflakedDimension is done through the same interface as the
    # Dimension class. Some methods of the SnowflakedDimension have
    # side effects on the rows passed to the SnowflakedDimension as the foreign
    # keys are computed based on interconnection of the Snowflaked dimension
    for row in products:
        productDimension.insert(row)

    # To ensure that all cached data is inserted and the transaction committed
    # both the commit and close function should be called when done
    conn.commit()
    conn.close()


In the above example the product dimension is not represented as a star schema
like in the examples shown for the other type of dimensions provided by
pygrametl. It is instead represented as a snowflake schema where the dimension
is split into multiple tables to achieve more normalisation and reduce
redundancy in the dimension. To support this, a combination of
:class:`.SnowflakedDimension` and :class:`.Dimension` is used. As multiple
tables need to be represented, an instance of :class:`.Dimension` is created
for each. An instance of :class:`.SnowflakedDimension` is then created to
aggregate the created instances of :class:`.Dimension` and represent the
Snowflaked dimension through one interface instead of manually interacting with
each table on its own. Interacting with a Snowflaked dimension is then done
through the same interface as presented by the other dimensions provided by
pygrametl, with the caveat that some methods have side effects on the rows
provided to :class:`.SnowflakedDimension` object, as foreign key relations
needs to be computed based on the contents of the rows the object operates on.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import Dimension, SnowflakedDimension, \
        SlowlyChangingDimension

    # Input is a list of "rows" which in pygrametl is modelled as dict
    products = [
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '20',
         'date' : '1990-10-01'},
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '10',
         'date' : '1990-12-10'},
        {'name' : 'Calvin and Hobbes', 'category' : 'Comic', 'price' : '20',
         'date' : '1991-02-01'},
        {'name' : 'Cake and Me', 'category' : 'Cookbook', 'price' : '15',
         'date' : '1990-05-01'},
        {'name' : 'French Cooking', 'category' : 'Cookbook', 'price' : '50',
         'date' : '1990-05-01'},
        {'name' : 'Sushi', 'category' : 'Cookbook', 'price' : '30',
         'date' : '1990-05-01'},
        {'name' : 'Nineteen Eighty-Four', 'category' : 'Novel', 'price' : '15',
         'date' : '1990-05-01'},
        {'name' : 'The Lord of the Rings', 'category' : 'Novel', 'price' : '60',
         'date' : '1990-05-01'}
    ]

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser' 
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used,
    # a reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The dimension is snowflaked into two tables, one with categories, and the
    # other with name and price. As the price is the slowly changing attribute,
    # and pygrametl currently only supports a slowly changing dimension as the
    # root table in a snow flaked dimension
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

    categoryTable = Dimension(
        name='category',
        key='categoryid',
        attributes=['category'])

    productDimension = SnowflakedDimension(references=[(productTable, categoryTable)])

    # Using a SlowlyChangingDimension with a SnowflakedDimension is done in the
    # same manner as a normal SlowlyChangingDimension using scdensure
    for row in products:
        productDimension.scdensure(row)

    # To ensure that all cached data is inserted and the transaction committed
    # both the commit and close function should be called when done
    conn.commit()
    conn.close()

A :class:`.SlowlyChangingDimension` and a :class:`.SnowflakedDimension` can be
combined if necessary, with the restriction that all slowly changing attributes
must be placed in the root table. To do this, the :class:`.Dimension` instance
connecting to the root table has to be changed to an instance of
:class:`.SlowlyChangingDimension` and the necessary attributes added to the
database table. Afterwards :meth:`.SnowflakedDimension.scdensure` can be used
to insert and lookup rows while ensuring that the slowly changing attributes
are updated correctly.
