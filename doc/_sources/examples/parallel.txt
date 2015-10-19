.. _parallel:

Parallel
========
*pygrametl* contains multiple abstractions to simplify the creation of parallel
ETL flow, in order to take advantage of modern multi-core and multi-processor
systems. Firstly, any :mod:`.datasources` can be read in a separate process
using :class:`.ProcessSource`. Further parallelism can be archived by
decoupling tables from the main process of execution, and allowing these
decoupled tables to communicate with each other without interrupting the main
process. Tables can also be partitioned so operations on large tables can be
performed by multiple processes. Both decoupled tables and partitioning tables
can be found in the :mod:`.tables` module. To support database connections from
multiple decoupled tables the :class:`.ConnectionWrapper`, or
:class:`.JDBCConnectionWrapper` must be used, wrapped by the function
:func:`.shareconnectionwrapper` to prevent inconsistencies when used by
multiple decoupled tables.

pygrametl also provides abstractions for running functions in parallel.  The
decorator :func:`.splitpoint` can be used to annotate functions which should
run in a separate process. This supplements the decoupled tables, as many
transformation are done in a set of functions before they are inserted into a
database table. Splitpoints can be synchronised using the function
:func:`.endsplits()`. The function :func:`createflow` can be used to create a
sequence of functions that run in a separate process. In a flow a row is given
to first function, then the second, and so forth. This also means changes to
the passed row is done using side effects while the return values are ignored.

Due to CPython's `GIL <https://wiki.python.org/moin/GlobalInterpreterLock>`_,
Jython should be used rather than CPython, as it allows threads to be used
instead of processes for performing operations in parallel. The term process is
used to denote both a process and a thread, depending on the Python
implementation in question. For more information on using pygrametl on
Jython, see :ref:`jython`.


ProcessSource
-------------
:class:`.ProcessSource` is a special data source allowing other data sources to
be iterated through in a separate process. A data source in pygrametl is a
set of abstraction allowing access to multiple types of data through a normal
Python iterator, for more information about data sources in general see
:ref:`datasources`.

.. code-block:: python

    import pygrametl
    from pygrametl.tables import FactTable, CachedDimension
    from pygrametl.datasources import CSVSource, ProcessSource, \
            TransformingSource
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper

    # JDBC and Jython is used as threads can allow for better performance
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection \
        ("jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")
    conn = JDBCConnectionWrapper(jdbcconn=jconn)

    factTable = FactTable(
        name='facttable',
        measures=['sales'],
        keyrefs=['storeid', 'productid', 'dateid'])

    productTable = CachedDimension(
            name='product',
            key='productid',
            attributes=['productname', 'price'],
            lookupatts=['productname'])

    # A set of "computational expensive" functions are needed to
    # transform the facts before they can be inserted into the fact table.
    # Each function must be defined as func(row) for them to be bundled as a
    # TransformationSource and performed in a separate process through the
    # data source ProcessSource
    def convertReals(row):
        # Converting a string encoding of a float to a integer must be done in
        # two steps, first it must be converted to a float and then to a integer
        row['sales'] = int(float(row['sales']))

    def trimProductname(row):
        row['productname'] = row['productname'].strip()

    # In the transformation we use three data sources to retrieve rows from
    # sales.csv, first CSVSource to read the csv file, then
    # TransformationSource to transform the rows, and lastly ProcessSource to
    # do both the reading and transformation in a separate threads 
    sales = CSVSource(csvfile=open('sales.csv'), delimiter=',')
    transSales = TransformingSource(sales, convertReals, trimProductname) 
    salesProcess = ProcessSource(transSales)

    # While the list of sales are being read and transformed by the spawned
    # process, the main process is occupied with pre loading the products
    # dimension with data from the products csv file
    products = CSVSource(csvfile=open('product.csv'), delimiter=',')
    for row in products:
        productTable.insert(row)

    # After the ProcessSource have read rows from the data source provided can
    # they be accessed through a iterator as any other data source
    for row in salesProcess:
        row['productid'] = productTable.lookup(row)
        factTable.insert(row) 
    conn.commit()
    conn.close()

Here we use a :class:`.ProcessSource` to transform a set of rows from the
*sales* csv file while we fill the *products* dimension with data. As the use
of a :class:`.ProcessSource` adds additional overhead to the iterator, seeing
as rows must be transferred in batches from another process, other computations
should be performed in between the creation and use of the source to allow for
data to be read, transformed and transferred.

Decoupled Tables
----------------
A decoupled table in pygrametl is a proxy for an instance of another table
class defined in the :mod:`.tables` module. Currently two different classes
exist for decoupled tables, :class:`.DecoupledDimension` and
:class:`.DecoupledFactTable`. The two classes behave nearly identically with
one implementing the interface of a dimension and the other the interface of a
fact table. When a method is called on one of the two classes, a message is
sent to the actual table object, and if the method has a return value an
instance of the class :class:`.FutureResult` is returned. This instance is a
handle to the actual result when it becomes available. In order to get the
actual result, the instance can be given directly to a method accepting a row
which would force the method to block until a value is ready or, alternatively,
the entire decoupled can be consumed by another decoupled table. When a
decoupled table is consumed by another decoupled table, the values are
extracted from an instance of :class:`.FutureResult` by the table that needs it
without blocking the caller of methods on that table. It should however be
noted that any rows passed to an instance of :class:`.DecoupledFactTable` or
:class:`.DecoupledDimension` should only contain the attributes directly needed
by the table, as having additional key/value pairs in the :class:`.dict` can
make pygrametl insert the row before the actual values are ready, leading to
instances of the class :class:`.FutureResult` being passed to the database instead,
which in nearly every case is undesirable.

.. code-block:: python

    from pygrametl.datasources import CSVSource
    from pygrametl.tables import FactTable, CachedDimension,\
         DecoupledDimension, DecoupledFactTable
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper
    from pygrametl.parallel import shareconnectionwrapper

    # The data is read from a csv file
    inputdata = CSVSource(csvfile=open('sales.csv', 'r'), delimiter=',')

    # JDBC and Jython is used as threads allows for better performance 
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection \
        ("jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")
         
    # The connection wrapper is itself wrapped in a SharedConnectionClient,
    # allowing for it to be used by multiple decoupled tables safely
    conn = JDBCConnectionWrapper(jdbcconn=jconn)
    shrdconn = shareconnectionwrapper(connection=conn)

    # The product dimension is decoupled and runs in a separate process 
    # (CPython) or thread (Jython), allowing it to be accessed by other 
    # decoupled tables without any use of the main process
    productDimension = DecoupledDimension(
        CachedDimension(
            name='product',
            key='productid',
            attributes=['productname', 'price'],
            lookupatts=['productname'],
            # The SharedConnectionWrapperClient must be copied for each 
            # decoupled table that use it correct interaction with the database 
            targetconnection=shrdconn.copy(),
            prefill=True)
        )

    # The fact table is also decoupled in order to consume the values returned
    # from the methods called on the product dimension without blocking the main
    # process while waiting for the database, allowing the main  process to 
    # perform other operations needed before a full fact is ready
    factTable = DecoupledFactTable(
        FactTable(
            name='fact',
            measures=['sale'],
            keyrefs=['storeid', 'productid', 'dateid'],
            targetconnection=shrdconn.copy()),
        returnvalues=False,
        consumes=[productDimension]
        )

    # Inserting facts into the database can be done in the same manner as in a
    # sequential ETL flow, extraction of data from the product dimension is 
    # done automatically by pygrametl.
    for row in inputdata:
        # A new 'row' is created for each fact, as having values not present in a
        # decoupled table that consumes another dimension, can make pygrametl 
        # miscalculate when actuals results are ready, making the framework 
        # pass a FutureResult object to the database driver instead of the actual
        # values, leading to exceptions
        fact = {}
        fact['storeid'] = row['storeid']
        fact['productid'] = productDimension.lookup(row)
        fact['dateid'] = row['dateid']
        fact['sale'] = row['sale']
        # Other CPU intensive transformations should be performed to take
        # advantage of the decoupled dimensions automatically exchanging data
        factTable.insert(fact)
    shrdconn.commit()
    shrdconn.close()

The above example show a very simple use of decoupled tables in pygrametl,
for real world application, tuning of queues and buffers should be done to
match the underlying hardware in order to maximize the performance of the
parallel ETL flow.  Although the example uses an instance of
:class:`.Dimension` and :class:`.FactTable` for simplicity, it is supported for
all types of dimensions and fact tables, except :class:`.SubprocessFactTable`
on CPython as it already runs in its own process. Decoupling of tables
requiring large amount of processing when their methods are called, like a
:class:`.SnowflakedDimension`, can help increase performance due to not
blocking the main process while waiting on the database performing the joins.

If any user-defined functions needs to access the database and be synchronised
with the decoupled tables, it must be passed to
:func:`.shareconnectionwrapper`.  An example of such a function is the bulk
loader used for pygrametl's :class:`.BulkFactTable`.

.. code-block:: python

    import pygrametl
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper
    from pygrametl.parallel import shareconnectionwrapper

    # JDBC and Jython is used as threads allows for better performance 
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection \
        ("jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    # A user defined function that specifies how to perform bulk loading for a
    # specific database management system such as Postgresql or Oracle
    def bulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        #DBMS specific bulk loading code here...
         
    # The connection wrapper is itself wrapped in a SharedConnectionClient,
    # allowing for it to be used by multiple decoupled tables safely. The
    #function "bulkloader" is given to "shareconnectionwrapper" allowing the
    # shared connection wrapper to ensure that the bulk loading functions is 
    # synchronised with the decoupled tables using the shared connection wrapper
    conn = JDBCConnectionWrapper(jdbcconn=jconn)
    scw = shareconnectionwrapper(targetconnection=conn, userfuncs=[bulkloader])

Partitioning Tables
-------------------
If a particular dimension of the fact table requires more processing then the
other tables, it can be beneficial to partition it into multiple parts,
allowing operations to be conducted on one table in parallel reducing the time
needed to process that particular table. pygrametl supports partitioning of
tables through multiple features. First, the classes
:class:`.DimensionPartitioner` and :class:`.FactTablePartitioner` automates the
partitioning of rows into multiple decoupled dimensions or fact tables. How to
do the partitioning is determined by a partitioning function with the signature
`func(dict)`. If no function is passed, then a default partitioning function is
used as documented in the API. Second, to ensure that unique surrogate keys are
assigned to all rows in a partitioned table, a shared sequence factory can be
created through the :func:`.getsharedsequencefactory`. Each parallel process is
then given a sequence of unique numbers to use as surrogate keys, ensuring that
all surrogate keys are unique despite being assigned by separate processes.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource, ProcessSource
    from pygrametl.tables import FactTable, CachedDimension, \
        DecoupledDimension, DecoupledFactTable, DimensionPartitioner
    from pygrametl.parallel import shareconnectionwrapper, \
        getsharedsequencefactory
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper 

    sales = CSVSource(csvfile=open('sales.csv', 'r'), delimiter=',')
        
    # JDBC and Jython is used as threads allows for better performance 
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection \
        ("jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    # The connection wrapper is itself wrapped in a SharedConnectionClient,
    # allowing for it to be used by multiple decoupled tables safely
    conn = JDBCConnectionWrapper(jdbcconn=jconn)
    shrdconn = shareconnectionwrapper(targetconnection=conn)

    # A sharedsequencefactory is created which creates values starting a zero,
    # each table is given a sequence of number to use, the size of the
    # sequence can increased trough a second argument if the 
    # sharedsequencefactory becomes a bottleneck in the ETL flow
    idfactory = getsharedsequencefactory(0)

    # The product dimension must use the sharedsequencefactory to ensure that
    # the two processes do not assign overlapping surrogate key, if the creation
    # of a surrogate key for the dimension is needed
    productDimensionOne = DecoupledDimension(
        CachedDimension(
            name='product',
            key='productid',
            attributes=['productname', 'price'],
            lookupatts=['productname'],
            idfinder=idfactory(),
            targetconnection=shrdconn.copy(),
            prefill=True)
        )

    productDimensionTwo = DecoupledDimension(
        CachedDimension(
            name='product',
            key='productid',
            attributes=['produtname', 'price'],
            lookupatts=['productname'],
            idfinder=idfactory(),
            targetconnection=shrdconn.copy(),
            prefill=True)
        )

    # The partitioning of data is automated by the DimensionPartitioner, using
    # a hash on the name of product. A corresponding class for partitioning a
    # fact table into multiple tables is also a available
    productDimension = DimensionPartitioner(parts=[productDimensionOne, 
        productDimensionTwo], partitioner = lambda row: hash(row['productname']))

    # Only partitioned tables needs to use the sharedsequencefactory, normal
    # tables can without any problems use the default incrementing surrogate key
    factTable = DecoupledFactTable(
            FactTable(
                name='fact',
                measures=['sale'],
                keyrefs=['storeid', 'productid', 'dateid'],
                targetconnection=shrdconn.copy()),
            returnvalues=False,
            # When consuming a partitioned dimension should each part be 
            # consumed separately, a simple way to do so is using the parts 
            # method which returns all parts handled by the partitioned 
            # dimension or fact table
            consumes=productDimension.parts
            )

    # Using a partitioned table is done in the same way as any other pygrametl
    # table, as the frameworks takes care of the partitioning behind the scenes
    for row in sales:
        # A new 'row' is created for each fact, as having values not present in a
        # decoupled table that consumes another dimension, can make pygrametl 
        # miscalculate when actuals results are ready, making the framework 
        # pass a FutureResult object to the database driver instead of the actual
        # values, leading to exceptions
        fact = {}
        fact['storeid'] = row['storeid']
        fact['dateid'] = row['dateid']
        fact['productid'] = productDimension.lookup(row)
        fact['sale'] = row['sale']
        # Other CPU intensive transformations should be performed to take
        # advantage of the decoupled dimensions automatically exchanging data
        factTable.insert(fact)
    shrdconn.commit()
    shrdconn.close()

The above example shows how to partition data of the product dimension over
multiple decoupled tables. This allows operations on the dimension to be
processed by two different processes. The rows are partitioned using hash
partitioning on the column `name` in the product dimension. A shared sequence
factory is used to provide surrogate keys for the product dimension, as using a
self-incrementing integer would assign the same value to multiple rows. This is
not needed for the fact table as only one table handles all operations on the
fact table in the database, so a simple auto incrementing integer is fine.

Splitpoints 
-----------
As CPU-intensive operations are often performed in user defined functions, the
decorator :func:`.splitpoint` is provided. This decorator functions in much the
same way as decoupled classes does for tables, as a number of processes are
spawned to run the function. The first time a functions with a decorator is
called, a process is created to handle the call. This is done until the number
of created process match the argument given to the decorator. If no process is
available, the call and its arguments are added to a :class:`.queue` and sent
to a process when one is idle. The number of processes to spawn can be passed
to the decorator, allowing more processes to be created for functions with a
longer running time. If a split function calls another function that requires
synchronisation it can be annotated with a new split point with *one* as
argument, specifying that only one process is allowed to call this function at
a time. To ensure all annotated functions are finished, the function
:func:`.endsplits` must be called, which joins all processes created by split
points up to that point.

.. code-block:: python

    import pygrametl
    from pygrametl.tables import FactTable
    from pygrametl.datasources import CSVSource
    from pygrametl.parallel import splitpoint, endsplits
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper 
        
    sales = CSVSource(csvfile=file('sales.csv', 'r'), delimiter=',')

    # JDBC and Jython is used as threads allows for better performance 
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection \
        ("jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    conn = JDBCConnectionWrapper(jdbcconn=jconn)

    factTable = FactTable(
        name='fact',
        measures=['sale'],
        keyrefs=['storeid', 'productid', 'dateid']
        )

    # Five processes are created to run this function, so five rows can be 
    # transformed at the same time, if not threads are available is the row 
    # added to a queue ensuring it will transformed when a process is available
    @splitpoint(instances=5)
    def performExpensiveTransformations(row):
        # Do some (expensive) transformations...

        # As multiple processes performs the operation inside this function must
        # a second function be created for the insertion into the database to
        # reduce the number of parallel processes accessing the database at the
        # same time
        insertRowIntoData(row)

    # The function is annotated with a argument free split point, no argument is
    # passed as the default is one, thereby specifying that only one process are
    # allowed to call this function at the same time
    @splitpoint
    def insertRowIntoData(row):
        factTable.insert(row)

    # The CSV file is read by the main process, while each row is transformed by
    # one of five process before being inserted to the database by sixth process
    for row in sales: 
        performExpensiveTransformations(row)

    # To ensure that a all split point annotated function are finished before
    # the ETL program terminated, must the function endsplits be called as it
    # joins all the process created by split points up to this point
    endsplits()
    conn.commit()
    conn.close()

An example use of split points are shown above. Here, a very computationally
expensive function is annotated with a splitpoint given the argument 5,
allowing five processes to run the function at the same time. The second
splitpoint without argument ensures that only one process is allowed to execute
that function at the same time, so even though it is called from
:func:`.performExpensiveTransformation` only one process can insert rows into
the fact table at the same time. Should the table operations become a
bottleneck it could be partitioned over multiple table classes. To ensure that
all split points have finished execution, the function :func:`.endsplits` is
executed, which joins all split points, before the database transaction is
committed.

As splitpoint annotated functions run in a separate processes, returned values
are not available to the calling process. To work around this restriction a
queue can be passed to the function which is then used as storage for returned
values automatically by pygrametl.

.. code-block:: python

    import pygrametl
    from pygrametl.datasources import CSVSource
    from pygrametl.parallel import splitpoint, endsplits
    from pygrametl.jythonmultiprocessing import Queue

    queue = Queue()
    sales = CSVSource(csvfile=file('sales.csv', 'r'), delimiter=',')

    # A queue is passed to the split point, which uses it to store return values
    @splitpoint(instances=5, output=queue)
    def expensiveReturningOperation(row):

        # Some special value, in this case None, is used to indicate that no
        # more data will be given to the queue and that processing can continue
        if row == None:
            return None

        # Returned values are automatically added to the queue for other to use
        return row

    # Each row in the sales csv file is extracted and passed to the function
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
        if elem == None:
            break

        # Use the returned elements after the sentinel check to prevent errors
        # ......

    # To ensure that a all split point annotated function are finished before
    # the ETL program terminated, must the function endsplits be called as it
    # joins all the process created by split points up to this point
    endsplits()
 

Flows
-----
Another way to use different processes in parallel is to use flows. A flow in
pygrametl consists of multiple functions that can be called with the same
interface, which is grouped together with each function running in its own
separate process, and with each function called in sequence. A flow can be
created from multiple different functions that, however, must be callable
through the same interface, using the :func:`.createflow` function. After a
flow is created it can be called just like any other function. Internally, the
arguments are passed from the first function to the last. As the arguments are
passed from one function to another, the side effects on each row are available
to the next function, white returned values on the other hand are ignored.
Unlike :func:`.splitpoint`, the arguments are passed in batches instead of
single values leading to less locking and synchronisation between the
processes.

.. code-block:: python

    import pygrametl
    from pygrametl.tables import Dimension 
    from pygrametl.datasources import CSVSource 
    from pygrametl.parallel import splitpoint, endsplits, createflow
    from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper 
        
    # JDBC and Jython is used as threads allows for better performance 
    import java.sql.DriverManager
    jconn = java.sql.DriverManager.getConnection \
        ("jdbc:postgresql://localhost/dw?user=dwuser&password=dwpass")

    conn = JDBCConnectionWrapper(jdbcconn=jconn)

    products = CSVSource(csvfile=file('product.csv', 'r'), delimiter=',')

    productDimension = Dimension(
            name='product',
            key='productid',
            attributes=['productname', 'price'],
            lookupatts=['productname'])

    # A couple of functions is defined to extract and transform the information
    # in the csv file each taking a row which is changed before being passed on
    def normaliseProductNames(row):
        # Expensive operations should be performed in a flow, this example is 
        # simple, so the performance gain is negated by overhead
        row['productname'].lower()

    def convertPriceToThousands(row):
        # Expensive operations should be performed in a flow, this example is 
        # simple, so the performance gain is negated by overhead
        row['price'] = int(row['price']) / 1000

    # A flow is created from the functions defined above, this flow can then be
    # called just like any other function, while two processes run the functions
    # underneath and take care of passing the arguments in batch between them
    flow = createflow(normaliseProductNames, convertPriceToThousands)

    # The data is read form the csv file in a split point so that the main
    # process does not have to both read the input data and insert it in the DB
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
    # however additional functions to extract the results as a list is available 
    for row in flow:
        productDimension.insert(row)
    endsplits()
    conn.commit()

A flow is used in the above example to combine multiple functions, each
contributing to the transformation on rows from the input csv file. Combining
the functions into a flow, creates a new process for each function in order to
increase throughput, while bundling data transfers to decrease the number of
times data needs to be moved from one process to the next. Calling the flow is
done in the function :func:`producer`, which runs in a separate process using a
splitpoint so the main process can insert rows into the database. It is done
just like a normal function call with the row as argument, as both functions in
the flow has an interface accepting one argument, the row.
