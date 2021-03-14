.. _datasources:

Data Sources
============
pygrametl supports numerous data sources, which are iterable classes that
produce rows. A row is a Python :class:`.dict` where the keys are the names of
the columns in the table where the row is from, and the values are the data
stored in that row. Users can easily implement new data sources by implementing
a version of the :meth:`__iter__` method that returns :class:`.dict`. As data
source are iterable, they can, e.g., be used in a loop as shown below:

.. code-block:: python

   for row in datasource:
       ...

While users can define their own data sources, pygrametl includes a number of
commonly used data sources:

SQLSource
---------
:class:`.SQLSource` is a data source used to iterate over the results of a
single SQL query. The data source's constructor must be passed a :PEP:`249`
connection and not a :class:`.ConnectionWrapper`. As an example, a PostgreSQL
connection created using the psycopg2 package is used below:

.. code-block:: python

    import psycopg2
    from pygrametl.datasources import SQLSource

    conn = psycopg2.connect(database='db', user='dbuser', password='dbpass')
    sqlSource = SQLSource(connection=conn, query='SELECT * FROM table')

In the above example, an :class:`.SQLSource` is created in order to extract all
rows from the table named table.

A tuple of strings can also optionally be supplied as the parameter :attr:`.names`, to
automatically rename the elements in the query results. Naturally, the number of
supplied names must match the number of elements in the result:

.. code-block:: python

    import psycopg2
    from pygrametl.datasources import SQLSource

    conn = psycopg2.connect(database='db', user='dbuser', password='dbpass')
    sqlSource = SQLSource(connection=conn, query='SELECT * FROM table',
			  names=('id', 'name', 'price'))

:class:`.SQLSource` also makes it possible to supply an SQL expression that will
be executed before the query, through the :attr:`.initsql` parameter. The result
of the expression will not be returned. In the example below a new view is
created and then used in the query:

.. code-block:: python

    import psycopg2
    from pygrametl.datasources import SQLSource

    conn = psycopg2.connect(database='db', user='dbuser', password='dbpass')
    sqlSource = SQLSource(connection=conn, query='SELECT * FROM view',
	initsql='CREATE VIEW view AS SELECT id, name FROM table WHERE price > 10')

CSVSource
---------
:class:`.CSVSource` is a data source that returns a row for each line in a
character-separated file. It is an alias for Python's `csv.DictReader
<http://docs.python.org/3/library/csv.html#csv.DictReader>`__ as it already is
iterable and returns :class:`.dict`. An example of how to use
:class:`.CSVSource` to read a file containing comma-separated values is shown
below:

.. code-block:: python

    from pygrametl.datasources import CSVSource

    # ResultsFile.csv contains: name,age,score
    csvSource = CSVSource(f=open('ResultsFile.csv', 'r', 16384), delimiter=',')

In the above example, a :class:`.CSVSource` is initialized with a file handler
that uses a buffer size of 16384, This particular buffer size is used as it
performed better than the alternatives we evaluated it against.

TypedCSVSource
--------------
:class:`.TypedCSVSource` extends :class:`.CSVSource` with typecasting by
wrapping `csv.DictReader
<http://docs.python.org/3/library/csv.html#csv.DictReader>`__ instead of simply
being an alias.

.. code-block:: python

    from pygrametl.datasources import TypedCSVSource

    # ResultsFile.csv contains: name,age,score
    typedCSVSource = TypedCSVSource(f=open('ResultsFile.csv', 'r', 16384),
				    casts={'age': int, 'score': float},
				    delimiter=',')

In the above example, a :class:`.TypedCSVSource` is initialized with a file
handler that uses a buffer size of 16384. This particular buffer size is used as
it performed better than the alternatives we evaluated it against. A dictionary
is also passed which provides information about what type each column should be
cast to. A cast is not performed for the name column as :class:`.TypedCSVSource`
uses :class:`.str` as the default.

PandasSource
-------------
:class:`.PandasSource` wraps a Pandas DataFrame so it can be used as a data
source. The class reuses existing functionality provided by `DataFrame
<https://pandas.pydata.org/pandas-docs/stable/reference/frame.html>`__. An
example of how to use this class can be seen below. In this example data is
loaded from a spreadsheet, then transformed using a Pandas DataFrame, and last
converted to an iterable that produce :class:`.dict` for use with pygrametl:

.. code-block:: python

    import pandas
    from pygrametl.datasources import PandasSource

    df = pandas.read_excel('Revenue.xls')
    df['price'] = df['price'].apply(lambda p: float(p) / 7.46)
    pandasSource = PandasSource(df)

In the above example, a Pandas DataFrame is created from a spreadsheet
containing revenue from some form of sales. Afterwards the data of the price
column is transformed using one of the higher-order functions built into the
Pandas package. Last, so the data can be loaded into a data warehouse using
pygrametl, a :class:`.PandasSource` is created with the DataFrame as an
argument, making the rows of the DataFrame accessible through a data source.

MergeJoiningSource
------------------
In addition to the above data sources which reads data from external sources,
pygrametl also includes a number of data sources that take other data sources as
input to transform and/or combine them.

:class:`.MergeJoiningSource` can be used to equijoin the rows from two data
sources. The rows of the two data sources must be delivered in sorted order. The
shared attributes on which the rows are to be joined must also be given.

.. code-block:: python

    from pygrametl.datasources import CSVSource, MergeJoiningSource

    products = CSVSource(f=open('products.csv', 'r', 16384), delimiter=',')
    sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter='\t')
    mergeJoiningSource = MergeJoiningSource(src1=products, key1='productid',
					    src2=sales, key2='productid')

In the above example, a :class:`.MergeJoiningSource` is used to join two data
sources on their shared attribute productid.

HashJoiningSource
-----------------
:class:`.HashJoiningSource` functions similarly to :class:`.MergeJoiningSource`,
but it performs the join using a hash map. Thus the two input data sources need
not produce their rows in sorted order.

.. code-block:: python

    from pygrametl.datasources import CSVSource, HashJoiningSource

    products = CSVSource(f=open('products.csv', 'r', 16384), delimiter=',')
    sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter='\t')
    hashJoiningSource = HashJoiningSource(src1=products, key1='productid',
					  src2=sales, key2='productid')

UnionSource
-----------
The class :class:`.UnionSource` creates a union of a number of the supplied data
sources. :class:`.UnionSource` does not require that the input data sources all
produce rows containing the same attributes, which also means that an
:class:`.UnionSource` does not guarantee that all of the rows it produces
contain the same attributes.

.. code-block:: python

    from pygrametl.datasources import CSVSource, UnionSource

    salesOne = CSVSource(f=open('sales1.csv', 'r', 16384), delimiter='\t')
    salesTwo = CSVSource(f=open('sales2.csv', 'r', 16384), delimiter='\t')
    salesThree = CSVSource(f=open('sales3.csv', 'r', 16384), delimiter='\t')

    combinedSales = UnionSource(salesOne, salesTwo, salesThree)

Each data source are exhausted before the next data source is read. This means
that all rows are read from the first data source before any rows are read from
the second data source, and so on.

RoundRobinSource
----------------
It can also be beneficial to interleave rows, and for this purpose,
:class:`.RoundRobinSource` can be used.

.. code-block:: python

    from pygrametl.datasources import CSVSource, RoundRobinSource

    salesOne = CSVSource(f=open('sales1.csv', 'r', 16384), delimiter='\t')
    salesTwo = CSVSource(f=open('sales2.csv', 'r', 16384), delimiter='\t')
    salesThree = CSVSource(f=open('sales3.csv', 'r', 16384), delimiter='\t')

    combinedSales = RoundRobinSource((salesOne, salesTwo, salesThree),
				     batchsize=500)

In the above example, :class:`.RoundRobinSource` is given a number of data
sources, and the argument :attr:`.batchsize`, which are the number of rows to be
read from one data source before reading from the next in a round-robin fashion.

ProcessSource
-------------
:class:`.ProcessSource` is used for iterating over a data source using a
separate worker process or thread. The worker reads data from the input data
source and creates batches of rows. When a batch is complete, it is added to a
queue so it can be consumed by another process or thread. If the queue is full
the worker blocks until an element is removed from the queue. The sizes of the
batches and the queue are optional parameters, but tuning them can often improve
throughput. For more examples of the parallel features provided by pygrametl see
:doc:`parallel`.

.. code-block:: python

    from pygrametl.datasources import CSVSource, ProcessSource

    sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter='\t')
    processSource = ProcessSource(source=sales, batchsize=1000, queuesize=20)

FilteringSource
---------------
:class:`.FilteringSource` is used to apply a filter to a data source. By
default, the built-in Python function `bool
<http://docs.python.org/3/library/functions.html#bool>`__ is used, which can be
used to remove empty rows. Alternatively, the user can supply a custom filter
function, which should be a callable function :attr:`f(row)`, which returns
:attr:`True` when a row should be passed on. In the example below, rows are
removed if the value of their location attribute is not Aalborg.

.. code-block:: python

    from pygrametl.datasources import CSVSource, FilteringSource


    def locationfilter(row):
	row['location'] == 'Aalborg'


    sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter='\t')
    salesFiltered = FilteringSource(source=sales, filter=locationfilter)

MappingSource
-------------
:class:`.MappingSource` can be used to apply functions to the columns of a data
source. It can be given a dictionary that where the keys are the columns and the
values are callable functions of the form :attr:`f(val)`. The functions will be
applied to the attributes in an undefined order. In the example below, a
function is used to cast all values for the attribute price to integers while
rows are being read from a CSV file.

.. code-block:: python

    from pygrametl.datasources import CSVSource, MappingSource

    sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter=',')
    salesMapped = MappingSource(source=sales, callables={'price': int})

TransformingSource
------------------
:class:`.TransformingSource` can be used to apply functions to the rows of a
data source. The class can be supplied with a number of callable functions of
the form :attr:`f(row)`, which will be applied to the source in the given order.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, TransformingSource


    def dkk_to_eur(row):
	price_as_a_number = int(row['price'])
	row['dkk'] = price_as_a_number
	row['eur'] = price_as_a_number / 7.43


    sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter=',')
    salesTransformed = TransformingSource(sales, dkk_to_eur)

In the above example, the price is converted from a string to an integer and
stored in the row as two currencies.

CrossTabbingSource
------------------
:class:`.CrossTabbingSource` can be used to compute the cross tab of a data
source. The class takes as parameters the names of the attributes that are to
appear as rows and columns in the crosstab, as well as the name of the attribute
to aggregate. By default, the values are aggregated using
:class:`.pygrametl.aggregators.Sum`, but the class also accepts an alternate
aggregator from the module :class:`pygrametl.aggregators`.

.. code-block:: python

     from pygrametl.datasources import CSVSource, CrossTabbingSource, \
	 TransformingSource
     from pygrametl.aggregators import Avg


     def dkk_to_eur(row):
	 price_as_a_number = int(row['price'])
	 row['dkk'] = price_as_a_number
	 row['eur'] = price_as_a_number / 7.43


     sales = CSVSource(f=open('sales.csv', 'r', 16384), delimiter=',')
     salesTransformed = TransformingSource(sales, dkk_to_eur)

     crossTab = CrossTabbingSource(source=salesTransformed, rowvaluesatt='product',
				   colvaluesatt='location', values='eur',
				   aggregator=Avg())

In the above example, a crosstab is made from a table containing sales data in
order to view the average price of products across different locations.
:class:`.TransformingSource` is used to parse and convert the price from DKK to EUR.

DynamicForEachSource
--------------------
:class:`.DynamicForEachSource` is a data source that for each data source
provided as input, creates a new data source that will be iterated by the
:class:`.DynamicForEachSource` data source. To create the new data sources the
user must provide a function that when called with a single argument, return a
new data source. In the example below, :class:`.DynamicForEachSource` is used to
create a :class:`.CSVSource` for each of the CSV files in a directory. The
:class:`.DynamicForEachSource` stores the input list in a safe multiprocessing
queue, and as such the :class:`.DynamicForEachSource` instance can be given to
several :class:`.ProcessSource`. For information about pygrametl's parallel
features see :doc:`parallel`.

.. code-block:: python

    import glob
    from pygrametl.datasources import CSVSource, DynamicForEachSource


    def createCSVSource(filename):
	return CSVSource(f=open(filename, 'r', 16384), delimiter=',')


    salesFiles = glob.glob('sales/*.csv')
    combinedSales = DynamicForEachSource(seq=salesFiles, callee=createCSVSource)
