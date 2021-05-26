.. _parallel:

Parallel
========
pygrametl provides multiple abstractions to simplify the creation of parallel
ETL flow, to take advantage of modern multi-core and multi-processor systems.
Firstly, any :mod:`.datasources` can be read in a separate process using
:class:`.ProcessSource`. Further parallelism can be archived by decoupling
tables from the main process and allowing these decoupled tables to communicate
with each other without interrupting the main process. Tables can also be
partitioned so operations on large tables can be performed by multiple
processes. Both decoupled tables and partitioning tables can be found in the
:mod:`.tables` module. To support database connections from multiple decoupled
tables any :class:`.ConnectionWrapper` and :class:`.JDBCConnectionWrapper` must
be wrapped by the function :func:`.shareconnectionwrapper` before being used by
multiple decoupled tables.

pygrametl also provides abstractions for running functions in parallel. The
decorator :func:`.splitpoint` can be used to annotate functions that should run
in separate processes. This supplements the decoupled tables, as many
transformations are done in a set of functions before they are inserted into a
database table. Splitpoints can be synchronized using the function
:func:`.endsplits()`. The function :func:`.createflow` can be used to create a
sequence of functions that run in separate processes. In a flow a row is first
given to the first function, then the second, and so forth. This also means the
passed row must be modified as the functions return values are ignored.

Due to CPython's `GIL <https://wiki.python.org/moin/GlobalInterpreterLock>`__,
Jython should be used to run ETL flows that use pygrametl parallel constructs.
This is because Jython allows threads to be used for parallel processing, while
it is necessary to use processes in CPython. Thus the term process is used to
denote a process or a thread, depending on the Python implementation in
question. For more information on using pygrametl on Jython, see :ref:`jython`.


ProcessSource
-------------
:class:`.ProcessSource` is a data source that allows other data sources to be
iterated through in a separate process. A data source in pygrametl is a set of
abstraction that provides access to multiple types of data through a normal
Python iterator. For more information about data sources see :ref:`datasources`.

.. code-block:: python

    from pygrametl.tables import FactTable, CachedDimension
    from pygrametl.datasources import CSVSource, ProcessSource, \
	    TransformingSource
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper

    # JDBC and Jython are used as threads usually provide better performance
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection(
	"jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")
    conn = JDBCConnectionWrapper(jdbcconn=jconn)

    factTable = FactTable(
	name='facttable',
	measures=['sale'],
	keyrefs=['storeid', 'productid', 'dateid'])

    productTable = CachedDimension(
	    name='product',
	    key='productid',
	    attributes=['name', 'price'],
	    lookupatts=['name'])


    # A set of computational expensive functions are needed to transform the
    # facts before they can be inserted into the fact table. Each function must
    # be defined as func(row) so a TransformationSource can combine them before
    # they are passed to ProcessSource and run in another thread
    def convertReals(row):
	# Converting a string encoding of a float to an integer must be done in
	# two steps, first it must be converted to a float and then to an integer
	row['sale'] = int(float(row['sale']))


    def trimProductname(row):
	row['name'] = row['name'].strip()


    # In the transformation we use three data sources to retrieve rows from
    # sales.csv, first CSVSource to read the csv file, then
    # TransformationSource to transform the rows, and lastly ProcessSource to
    # do both the reading and transformation in another thread
    sales = CSVSource(f=open('sales.csv'), delimiter=',')
    transSales = TransformingSource(sales, convertReals, trimProductname)
    salesProcess = ProcessSource(transSales)

    # While the list of sales are being read and transformed by the spawned
    # thread, the main thread is occupied with pre-loading the product dimension
    # with data from product.csv
    products = CSVSource(f=open('product.csv'), delimiter=',')
    for row in products:
	productTable.insert(row)

    # After the ProcessSource have read rows from the data source provided, they
    # can be accessed through ProcessSource iterator like any other data source
    for row in salesProcess:
	row['productid'] = productTable.lookup(row)
	factTable.insert(row)
    conn.commit()
    conn.close()

In the above example, we use a :class:`.ProcessSource` to transform a set of rows
from sales.csv while we fill the product dimension with data. As the use of a
:class:`.ProcessSource` adds additional overhead to the iterator, seeing as rows
must be transferred in batches from another process, other computations should
be performed in between the creation and use of the data source to allow for
data to be read, transformed, and transferred.

Decoupled Tables
----------------
A decoupled table in pygrametl is a proxy for an instance of another table class
defined in the :mod:`.tables` module. Currently, two different classes exist for
decoupled tables, :class:`.DecoupledDimension` and :class:`.DecoupledFactTable`.
The two classes behave nearly identically with one implementing the interface of
a dimension and the other the interface of a fact table. When a method is called
on one of the two classes, a message is sent to the actual table object, and if
the method has a return value an instance of the class :class:`.FutureResult` is
returned. This instance is a handle to the actual result when it becomes
available. To get the actual result, the instance can be given directly to a
method accepting a row which would force the method to block until a value is
ready, or the entire decoupled can be consumed by another decoupled table. When
a decoupled table is consumed by another decoupled table, the values are
extracted from an instance of :class:`.FutureResult` by the table that needs it
without blocking the caller of methods on that table. It should however be noted
that any rows passed to an instance of :class:`.DecoupledFactTable` or
:class:`.DecoupledDimension` should only contain the attributes directly needed
by the table, as having additional key/value pairs in the :class:`.dict` can
make pygrametl insert the row before the actual values are ready, leading to
instances of the class :class:`.FutureResult` being incorrectly passed to the
database instead.

.. code-block:: python

    from pygrametl.datasources import CSVSource
    from pygrametl.tables import FactTable, CachedDimension,\
	 DecoupledDimension, DecoupledFactTable
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper
    from pygrametl.parallel import shareconnectionwrapper

    # The data is read from a csv file
    inputdata = CSVSource(f=open('sales.csv', 'r'), delimiter=',')

    # JDBC and Jython are used as threads usually provide better performance
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection(
	"jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    # The connection wrapper is itself wrapped in a SharedConnectionClient,
    # so it can be shared by multiple decoupled tables in a safe manner
    conn = JDBCConnectionWrapper(jdbcconn=jconn)
    shrdconn = shareconnectionwrapper(targetconnection=conn)

    # The product dimension is decoupled and runs in a separate thread allowing
    # it to be accessed by other decoupled tables without using the main thread
    productDimension = DecoupledDimension(
	CachedDimension(
	    name='product',
	    key='productid',
	    attributes=['name', 'price'],
	    lookupatts=['name'],
	    # The SharedConnectionWrapperClient must be copied for each
	    # decoupled table that use it correct interaction with the database
	    targetconnection=shrdconn.copy(),
	    prefill=True)
	)

    # The fact table is also decoupled in order to consume the values returned
    # from the methods called on the product dimension without blocking the main
    # thread while waiting for the database. Thus allowing the main thread to
    # perform other operations needed before a full fact is ready
    factTable = DecoupledFactTable(
	FactTable(
	    name='facttable',
	    measures=['sale'],
	    keyrefs=['storeid', 'productid', 'dateid'],
	    targetconnection=shrdconn.copy()),
	returnvalues=False,
	consumes=[productDimension]
	)

    # Inserting facts into the database can be done in the same manner as in a
    # sequential ETL flow, extraction of data from the product dimension is
    # done automatically by pygrametl
    for row in inputdata:
	# A new row is created for each fact, as having values not present in a
	# decoupled table that consumes another dimension, can make pygrametl
	# miscalculate when the actual results are ready, making the framework
	# pass a FutureResult to the database which usually raises an error
	fact = {}
	fact['storeid'] = row['storeid']
	fact['productid'] = productDimension.ensure(row)
	fact['dateid'] = row['dateid']
	fact['sale'] = row['sale']
	# Other CPU intensive transformations should be performed to take
	# advantage of the decoupled dimensions automatically exchanging data
	factTable.insert(fact)
    shrdconn.commit()
    shrdconn.close()

The above example shows a very simple use of decoupled tables in pygrametl, for
real-world application, tuning of queues and buffers should be done to match the
underlying hardware to maximize the performance of the parallel ETL flow.
Although the example uses an instance of :class:`.Dimension` and
:class:`.FactTable` for simplicity, it is supported for all types of dimensions
and fact tables, except :class:`.SubprocessFactTable` on CPython as it already
runs in its own process. Decoupling of tables requiring a large amount of
processing when their methods are called, like a :class:`.SnowflakedDimension`,
can help increase performance due to not blocking the main process while waiting
on the database performing the joins.

If any user-defined function needs to access the database and be synchronized
with the decoupled tables, it must be passed to :func:`.shareconnectionwrapper`.
An example of such a function is the bulk loader used for pygrametl's
:class:`.BulkFactTable`.

.. code-block:: python

    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper
    from pygrametl.parallel import shareconnectionwrapper

    # JDBC and Jython is used as threads usually provides better performance
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection(
	"jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")


    # A user-defined function that can bulk load data into PostgreSQL over JDBC
    def bulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
	global jconn
	copymgr = jconn.getCopyAPI()
	sql = "COPY %s(%s) FROM STDIN WITH DELIMITER '%s'" % \
	      (name, ', '.join(attributes), fieldsep)
	copymgr.copyIn(sql, filehandle)


    # The connection wrapper is itself wrapped in a SharedConnectionClient so it
    # can be shared by multiple decoupled tables in a safe manner. The function
    # bulkloader is given to shareconnectionwrapper so the shared connection
    # wrapper can ensure that the bulk loading function is synchronized with
    # the decoupled tables using the shared connection wrapper
    conn = JDBCConnectionWrapper(jdbcconn=jconn)
    scw = shareconnectionwrapper(targetconnection=conn, userfuncs=[bulkloader])

Partitioning Tables
-------------------
If a particular dimension or fact table requires more processing than the other
tables, it can be beneficial to partition it into multiple partitions. Thus
allowing operations to be conducted on one table in parallel to reduce the time
needed to process that particular table. pygrametl supports partitioning of
tables through multiple features. Firstly, the classes
:class:`.DimensionPartitioner` and :class:`.FactTablePartitioner` automates the
partitioning of rows for multiple decoupled dimensions or fact tables. How to do
the partitioning is determined by a partitioning function with the signature
:func:`func(dict)`. If no function is passed, then a default partitioning
function is used as documented in the API. Secondly, to ensure that unique
surrogate keys are assigned to all rows in a partitioned table, a shared
sequence factory can be created using the function
:func:`.getsharedsequencefactory`. Each parallel process is then given a unique
set of numbers to use as surrogate keys, ensuring that all surrogate keys are
unique despite being assigned by separate processes.

.. code-block:: python

    from pygrametl.datasources import CSVSource
    from pygrametl.tables import FactTable, CachedDimension, \
	DecoupledDimension, DecoupledFactTable, DimensionPartitioner
    from pygrametl.parallel import shareconnectionwrapper, \
	getsharedsequencefactory
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper

    sales = CSVSource(f=open('sales.csv', 'r'), delimiter=',')

    # JDBC and Jython are used as threads usually provide better performance
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection(
	"jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    # The connection wrapper is itself wrapped in a SharedConnectionClient,
    # so it can be shared by multiple decoupled tables in a safe manner
    conn = JDBCConnectionWrapper(jdbcconn=jconn)
    shrdconn = shareconnectionwrapper(targetconnection=conn)

    # A sharedsequencefactory is created which provides values starting at zero.
    # It gives each table a sequence of numbers to use as surrogate keys. The
    # size of the sequence can be increased through a second argument if the
    # sharedsequencefactory becomes a bottleneck in the ETL flow
    idfactory = getsharedsequencefactory(0)

    # The product dimension must use the sharedsequencefactory to ensure that
    # the two processes do not assign overlapping surrogate key to the rows
    productDimensionOne = DecoupledDimension(
	CachedDimension(
	    name='product',
	    key='productid',
	    attributes=['name', 'price'],
	    lookupatts=['name'],
	    idfinder=idfactory(),
	    targetconnection=shrdconn.copy(),
	    prefill=True)
	)

    productDimensionTwo = DecoupledDimension(
	CachedDimension(
	    name='product',
	    key='productid',
	    attributes=['name', 'price'],
	    lookupatts=['name'],
	    idfinder=idfactory(),
	    targetconnection=shrdconn.copy(),
	    prefill=True)
	)

    # The partitioning of data is automated by the DimensionPartitioner using
    # a hash on the name of product. A FactTablePartitioner is also provided
    productDimension = DimensionPartitioner(
	parts=[productDimensionOne, productDimensionTwo],
	partitioner=lambda row: hash(row['name']))

    # Only partitioned tables needs to use the sharedsequencefactory, normal tables
    # can without any problems use the default self-incrementing surrogate key
    factTable = DecoupledFactTable(
	    FactTable(
		name='facttable',
		measures=['sale'],
		keyrefs=['storeid', 'productid', 'dateid'],
		targetconnection=shrdconn.copy()),
	    returnvalues=False,
	    # When consuming a partitioned dimension each part should be
	    # consumed separately, a simple way to do so is using the parts
	    # method which returns all parts managed by the partitioner
	    consumes=productDimension.parts
	    )

    # A partitioned table can be used in the same way as any other pygrametl
    # table since the framework takes care of the partitioning behind the scenes
    for row in sales:
	# A new row is created for each fact, as having values not present in a
	# decoupled table that consumes another dimension, can make pygrametl
	# miscalculate when the actual results are ready, making the framework
	# pass a FutureResult to the database which usually raises an error
	fact = {}
	fact['storeid'] = row['storeid']
	fact['dateid'] = row['dateid']
	fact['productid'] = productDimension.ensure(row)
	fact['sale'] = row['sale']
	# Other CPU intensive transformations should be performed to take
	# advantage of the decoupled dimensions automatically exchanging data
	factTable.insert(fact)
    shrdconn.commit()
    shrdconn.close()

The above example shows how to partition the data of the product dimension to
multiple decoupled tables. This allows operations on the dimension to be
performed by two different processes. The rows are partitioned using hash
partitioning on the attribute :attr:`name`. A shared sequence factory is used to
provide surrogate keys for the product dimension, as using a self-incrementing
key would assign the same value to multiple rows. This is not needed for the
fact table as only one table handles all operations on the fact table in the
database, so a simple self-incrementing key is fine.

Splitpoints
-----------
As CPU intensive operations are often performed in user-defined functions, the
decorator :func:`.splitpoint` is provided. This decorator functions in much the
same way as decoupled classes do for tables, as a number of processes are
spawned to run the function. The number of processes to spawn can be passed to
the decorator, allowing more processes to be created for functions with a longer
run time. The first time a function with a decorator is called, a process is
created to handle the call. This is done until the number of created processes
matches the argument given to the decorator. Then, if a process is not available,
the call and its arguments are added to a :class:`.queue` shared by the process
created for the splitpoint. If a split function calls another function that
requires synchronization it can be annotated with a new splitpoint with one as
the argument, specifying that only one process is allowed to call this function
at a time. To ensure all annotated functions are finished, the function
:func:`.endsplits` must be called, which joins all processes created by split
points up to that point.

.. code-block:: python

    from pygrametl.tables import FactTable
    from pygrametl.datasources import CSVSource
    from pygrametl.parallel import splitpoint, endsplits
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper

    sales = CSVSource(f=open('sales.csv', 'r'), delimiter=',')

    # JDBC and Jython are used as threads usually provide better performance
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection(
	"jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    conn = JDBCConnectionWrapper(jdbcconn=jconn)

    factTable = FactTable(
	name='facttable',
	measures=['sale'],
	keyrefs=['storeid', 'productid', 'dateid']
	)


    # Five threads are created to run this function, so five rows can be
    # transformed at the same time. If no threads are available, the row
    # is added to a queue and transformed when a thread becomes idle
    @splitpoint(instances=5)
    def performExpensiveTransformations(row):
	# Do some (expensive) transformations...

	# As multiple threads perform the operation inside this function. a second
	# function must be created to synchronize inserting rows into the database
	insertRowIntoData(row)


    # The function is annotated with an argument-free splitpoint, so its argument
    # becomes one, thereby specifying that this function should run in one thread
    @splitpoint
    def insertRowIntoData(row):
	factTable.insert(row)


    # The CSV file is read by the main thread, then each row is transformed by
    # one of five threads, before being added to the database by a sixth thread
    for row in sales:
	performExpensiveTransformations(row)

    # To ensure that all splitpoint annotated functions are finished before
    # the ETL flow is terminated, the function endsplits must be called as it
    # joins all the threads created by splitpoints up to this point
    endsplits()
    conn.commit()
    conn.close()

The above example shows how to use splitpoints. Here, a very computationally
expensive function is annotated with a :attr:`splitpoint` which is given the
argument five, allowing five processes to run the function at the same time. The
second :attr:`splitpoint` without an argument ensures that only one process is
allowed to execute that function at a time, so even though it is called from
:func:`.performExpensiveTransformation` only one process can insert rows into
the fact table at the same time. Should the operations on the fact table become
a bottleneck, it could be partitioned using :class:`.FactTablePartitioner`. To
ensure that all splitpoints have finished execution, the function
:func:`.endsplits` is executed, which joins all splitpoints, before the database
connection is closed.

As splitpoint annotated functions run in separate processes, any values they
return are not available to the process calling them. To work around this
restriction a queue can be passed as an argument to :attr:`splitpoint` in which
the split function's returned values will be added.

.. code-block:: python

    from pygrametl.datasources import CSVSource
    from pygrametl.parallel import splitpoint, endsplits
    from pygrametl.jythonmultiprocessing import Queue

    queue = Queue()
    sales = CSVSource(f=open('sales.csv', 'r'), delimiter=',')


    # A queue is passed to the decorator, which uses it to store return values
    @splitpoint(instances=5, output=queue)
    def expensiveReturningOperation(row):

	# Some special value, in this case None, is used to indicate that no
	# more data will be given to the queue and that processing can continue
	if row is None:
	    return None

	# Returned values are automatically added to the queue for other to use
	return row


    # Each row in the sales.csv is extracted and passed to the function
    for row in sales:
	expensiveReturningOperation(row)

    # A simple sentinel value can be used to indicate that all rows have been
    # processed and that the loop using the results below can break
    expensiveReturningOperation(None)

    # A infinite loop is used to process the returned values as the number of
    # returned rows are unknown, so a sentinel value and a break is used instead
    while True:
	# Extracts the processed row returned by the annotated function, a
	# simple sentinel value is used to indicate when the processing is done
	elem = queue.get()
	if elem is None:
	    break

	# Use the returned elements after the sentinel check to prevent errors
	# ......

    # To ensure that all splitpoint annotated functions are finished before
    # the ETL flow is terminated, the function endsplits must be called as it
    # joins all the process created by splitpoints up to this point
    endsplits()


Flows
-----
Another way to parallelize transformations is to use flows. In pygrametl, a flow
is a sequence of functions with the same interface, each running in its own
separate process, and where each function calls the next function in the
sequence. A flow can be created from multiple different functions using the
:func:`.createflow` function. After a flow is created it can be called just like
any other function. Internally, the arguments are passed from the first function
to the last. While the arguments are passed to the functions, any returned
values are ignored. Unlike :func:`.splitpoint`, arguments are passed in batches
and not as single values to reduce the overhead of synchronization.

.. code-block:: python

    from pygrametl.tables import Dimension
    from pygrametl.datasources import CSVSource
    from pygrametl.parallel import splitpoint, endsplits, createflow
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper

    # JDBC and Jython are used as threads usually provide better performance
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection(
	"jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    conn = JDBCConnectionWrapper(jdbcconn=jconn)

    products = CSVSource(f=open('product.csv', 'r'), delimiter=',')

    productDimension = Dimension(
	    name='product',
	    key='productid',
	    attributes=['name', 'price'],
	    lookupatts=['name'])


    # Two functions are defined to transform the information in product.csv
    def normaliseProductNames(row):
	# Expensive operations should be performed in a flow, this example is
	# simple, so the performance gain is negated by the synchronization
	row['name'].lower()


    def convertPriceToThousands(row):
	# Expensive operations should be performed in a flow, this example is
	# simple, so the performance gain is negated by the synchronization
	row['price'] = int(row['price']) / 1000


    # A flow is created from the two functions defined above, this flow can then
    # be called just like any other functions despite being parallelized
    flow = createflow(normaliseProductNames, convertPriceToThousands)


    # The data is read from product.csv in a splitpoint so the main process
    # does not have to both read the input data and load it into the table
    @splitpoint
    def producer():
	for row in products:
	    flow(row)

	# The flow should be closed when there is no more data available,
	# this means no more data is accepted but the computations will finish
	flow.close()


    # The producer is called and the separate process starts to read the input
    producer()

    # The simplest way to extract rows from a flow is just to iterate over it,
    # however additional functions to get the results as a list are available
    for row in flow:
	productDimension.insert(row)
    endsplits()
    conn.commit()

A flow is used in the above example to combine multiple functions, each
transforming the rows from product.csv. By creating a flow with these functions,
a process is created for each to increase the ETL flows throughput. The overhead
of transferring data between the functions is reduced through batching. Rows
are provided to the flow in function :func:`producer`, which runs in a separate
process using a splitpoint so the main process can load the transformed rows
into the database by iterating over the flow.
