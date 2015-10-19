.. _datasources:

Data sources
============

*pygrametl* has support for numerous data sources. Data in pygrametl is moved
around in rows, so instead of implementing a row class, pygrametl utilizes
Python's built in dictionaries. Each of the data sources in this class, are
iterable and provide *dicts* with data values. Implementing your own data
sources in pygrametl is easy, as the only requirement is that the data source
is iterable, i.e. defining the :meth:`__iter__` method. As such, it should be
possible to do the following:

.. code-block:: python

   for row in datasource:
       ...

As a default, pygrametl has a number of built-in data types:

SQLSource
---------

The class :class:`.SQLSource` is a data source used to iterate the results of a
single SQL query. The data source accepts only a :PEP:`249` connection, and not
a :class:`.ConnectionWrapper` object. For illustrative purposes, a PostgreSQL
connection is used here, using the Psycopg package.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.datasources import SQLSource

    conn = psycopg2.connect(database="db", user="dbuser", password="dbpass")

    sql = "SELECT * FROM table;"
    resultsSource = SQLSource(connection=conn, query=sql)

    for row in resultsSource:
        print(row)

In the above example, an SQLSource is created in order to extract all rows from
a table.

A tuple of attribute names can also be supplied as a parameter, if preferable,
which will be used instead of the attribute names from the table. Naturally,
the number of supplied names must match the number of names in the query result
from the database:

.. code-block:: python

    ...

    newnames = 'ID', 'Name', 'Price'
    resultsSource = SQLSource(connection=conn, query=sql, names=newnames)

The class also makes it possible to supply an SQL expression that will be
executed before the query, through the initsql parameter. The result of the
expression will not be returned.

.. code-block:: python

    ...

    sql = "SELECT * FROM newview;"
    resultsSource = SQLSource(connection=conn, query=sql, \
        initsql="CREATE VIEW newview AS SELECT ID, Name FROM table WHERE Price > 10;")

In the previous example a new view is created, which is then used in the query.

CSVSource
---------

The class :class:`.CSVSource` is a data source returning the lines of a
delimiter-separated file, turned into dictionaries. The class is fairly simple,
and is implemented as a reference to `csv.DictReader
<http://docs.python.org/2/library/csv.html#csv.DictReader>`_ in the Python
Standard Library. An example of the usage of this class can be seen below, in
which a file containing comma-separated values is loaded:

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource

    resultsSource = CSVSource(csvfile=open('ResultsFile.csv', 'r', 16384), \
                                delimiter=',')

In the above example, a CSVSource is created from a file delimited by commas,
using a buffer size of 16384. This particular buffer size is used as it
performed better than the alternatives we evaluated it against.

MergeJoiningSource
------------------

In addition to the aforementioned data sources, pygrametl also includes a
number of ways to join and combine existing data sources.

The class :class:`.MergeJoiningSource` can be used to equijoin rows from two
data sources. The rows of the two data sources which are to be merged, must
deliver their rows in sorted order. It is also necessary to supply the common
attributes on which the join must be performed.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, MergeJoiningSource

    products = CSVSource(csvfile=open('products.csv', 'r', 16384), delimiter=',')
    sales = CSVSource(csvfile=open('sales.txt', 'r', 16384), delimiter='\t')

    data = MergeJoiningSource(src1=products, key1='productID',
                              src2=sales, key2='productID')

In the above example, the class is used to join two sources on a common
attribute *productID*.

HashJoiningSource
-----------------

The class :class:`.HashJoiningSource` functions similarly to
:class:`.MergeJoiningSource`, but performs the join using a hash map instead.
As such, it is not necessary for the two input data sources to be sorted.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, HashJoiningSource

    products = CSVSource(csvfile=open('products.csv', 'r', 16384), delimiter=',')
    sales = CSVSource(csvfile=open('sales.txt', 'r', 16384), delimiter='\t')

    data = HashJoiningSource(src1=products, key1='productID',
                              src2=sales, key2='productID')

UnionSource
-----------

It is also possible to union different data sources together in pygrametl. The
class :class:`.UnionSource` creates a union of a number of supplied data
sources. The data sources do not necessarily have to contain the same types of
rows.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, UnionSource

    salesOne = CSVSource(csvfile=open('sales1.csv', 'r', 16384), delimiter='\t')
    salesTwo = CSVSource(csvfile=open('sales2.csv', 'r', 16384), delimiter='\t')
    salesThree = CSVSource(csvfile=open('sales3.csv', 'r', 16384), delimiter=',')

    combinedSales = UnionSource(salesOne, salesTwo, salesThree)

The data sources are read in their entirety, i.e. every row is read from the
first source before rows are read from the second source.

RoundRobinSource
----------------

It can also be beneficial to interleave rows, and for this purpose
:class:`.RoundRobinSource` can be used.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, RoundRobinSource

    salesOne = CSVSource(csvfile=open('sales1.csv', 'r', 16384), delimiter='\t')
    salesTwo = CSVSource(csvfile=open('sales2.csv', 'r', 16384), delimiter='\t')
    salesThree = CSVSource(csvfile=open('sales3.csv', 'r', 16384), delimiter=',')

    combinedSales = RoundRobinSource((salesOne, salesTwo, salesThree), batchsize=500)

As can be seen in the above example, the class takes a number of data sources
along with an argument *batchsize*, corresponding to the amount of rows read
from one source before reading from the next in a round-robin fashion.

ProcessSource
-------------

The class :class:`.ProcessSource` is used for iterating a source in a separate
process.  A worker process is spawned, which iterates the source rows in
batches, which are added to a queue. The sizes of the batches and the queue are
optional parameters to the class.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, ProcessSource

    sales = CSVSource(csvfile=open('sales.csv', 'r', 16384), delimiter='\t')

    sales_process = ProcessSource(source=sales, batchsize=1000, queuesize=20)

For more examples of the parallel features of pygrametl, refer to
:doc:`parallel`.

FilteringSource
---------------

The class :class:`.FilteringSource` is used to apply a filter to a source.  As
a default, the built-in Python function `bool
<http://docs.python.org/2/library/functions.html#bool>`_ is used, which can be
used to remove empty rows. Alternatively, the user can supply a custom filter,
which should be a callable function ``f(row)``, which returns ``True`` when a
row should be passed on.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, FilteringSource

    def locationfilter(row):
        if row['location'] == 'Aalborg':
            return True
        else:
            return False

    sales = CSVSource(csvfile=open('sales.csv', 'r', 16384), delimiter='\t')

    sales_filtered = FilteringSource(source=sales, filter=locationfilter)

In the above example, a very simple filter is used, which filters out rows
where the value of the *location* attribute is not *Aalborg*.

TransformingSource
------------------

The class :class:`.TransformingSource` can be used to apply functions to the
rows of a source.  The class can be supplied with a number of callable
functions of the form ``f(row)``, which will be applied to the source in the
given order.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, TransformingSource

    def dkk_to_eur(row):
        oldprice = int(row['price'])
        row['price'] = oldprice / 7.46

    sales = CSVSource(csvfile=open('sales.csv', 'r', 16384), delimiter=',')

    sales_transformed = TransformingSource(source=sales, dkk_to_eur)

In the above example, a function is used which transforms the value of an
attribute containing currency from Danish kroner (DKK) to euros

CrossTabbingSource
------------------

The class :class:`.CrossTabbingSource` can be used to compute generate a cross
tab of a data source.  The class takes as parameters the names of the
attributes that are to appear as rows and colums in the crosstab, as well as
the name of the attribute to aggregate.  As a default, the values are
aggregated using *Sum*, but the class also accepts an alternate aggregator,
which can be found in the module :class:`pygrametl.aggregators`.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, CrossTabbingSource, TransformingSource
    from pygrametl.aggregators import Sum

    def price_to_integer(row):
        row['price'] = int(row['price'])

    sales = CSVSource(csvfile=open('sales.csv', 'r', 16384), delimiter=',')
    sales_transformed = TransformingSource(source=sales, price_to_integer)

    crossTab = CrossTabbingSource(source=sales_transformed, rowvaluesatt='product',\
                 colvaluesatt='location', values='price', aggregator=Sum())

In the above example, a crosstab is made from a table containing sales data, in
order to view the total amount of sales of specific products across different
locations. `TransformingSource` is used here as well, to convert the prices
from strings to integers, to allow for summation.

DynamicForEachSource
--------------------

The class :class:`.DynamicForEachSource` is a source that for each provided
source, creates a new source that will be iterated by this source.  The user
must also provide a function that when called with a single argument, produces
a new iterable source.

.. code-block:: python

    import pygrametl
    import glob
    from pygrametl.datasources import CSVSource, DynamicForEachSource

    # Opens a file and creates a CSVSource
    def createCSVSource(filename):
        return CSVSource(csvfile=open(filename, 'r', 16384), delimiter=',')

    # Extract all .csv file names from the folder 'files'
    files = glob.glob('files/*.csv')

    sources = DynamicForEachSource(seq=files, callee=createCSVSource)

In the above example, the class is used to create a number of *CSVSources* for
each of a number of .csv files in a directory. `DynamicForEachSource` stores
the input list in a safe multiprocessing queue, and as such the
`DynamicForEachSource` instance can be given to several :class:`.ProcessSource`
instances.

For more examples of the parallel features of pygrametl, refer to
:doc:`parallel`.
