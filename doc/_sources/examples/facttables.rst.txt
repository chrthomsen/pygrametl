.. _facttables:

Fact Tables
===========
pygrametl provides multiple classes for representing fact tables. These classes
enable facts to be loaded one at a time, as batches stored in memory, or in
bulk from a file on disk. Support for loading facts with missing information and
then updating them later is also supported. For information about how to load
facts in parallel see :ref:`parallel`. In the following examples, we use
PostgreSQL as the RDBMS and psycopg2 as the database driver.

All of the following classes are currently implemented in the
:mod:`.pygrametl.tables` module.

FactTable
---------
The most basic class for representing a fact table is :class:`.FactTable`.
Before creating a :class:`.FactTable` object, an appropriate table must be
created in the database, and a :pep:`249` connection to the database must be
created and wrapped by the class :class:`.ConnectionWrapper`. For more
information about how database connections are used in pygrametl see
:ref:`database`. :class:`.FactTable` constructor must be given the table's
:attr:`name`, the attributes used as :attr:`measures` in the fact table, and the
attributes referencing dimensions (:attr:`keyrefs`). Be aware that
:class:`.FactTable` performs an insert in the database whenever the
:meth:`.FactTable.insert` method is called, which can very quickly become a
bottleneck.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import FactTable

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # This instance of FactTable connects to the table facttable in the
    # database using the default connection wrapper created above
    factTable = FactTable(
	name='facttable',
	measures=['price'],
	keyrefs=['storeid', 'productid', 'dateid'])

The above example shows the three step process needed to connect an instance of
:class:`.FactTable` to an existing database table. Firstly, a :PEP:`249`
connection to the database is created. Then an instance of
:class:`.ConnectionWrapper` is created to provide a uniform interface to all
types of database connections supported by pygrametl. The instance of
:class:`.ConnectionWrapper` is also set as the default database connection to
use for this ETL flow. Lastly, a :class:`.FactTable` is created as a
representation of the actual database table.

Operations on the fact table are done using three methods:
:meth:`.FactTable.insert` inserts new facts directly into the fact table when
they are passed to the method. :meth:`.FactTable.lookup` returns a fact if the
database contains one with the given combination of keys referencing the
dimensions. :meth:`.FactTable.ensure` combines :meth:`.FactTable.lookup` and
:meth:`.FactTable.insert` by ensuring that a fact does not exist before
inserting it. An example of each function and the automatic name mapping can be
seen below, where the fact table from the last example is reused.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import FactTable

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # This instance of FactTable connects to the table facttable in the
    # database using the default connection wrapper created above
    factTable = FactTable(
	name='facttable',
	measures=['price'],
	keyrefs=['storeid', 'productid', 'dateid'])

    # A list of facts ready to inserted into the fact table
    facts = [{'storeid': 1, 'productid': 13, 'dateid': 4, 'price': 50},
	     {'storeid': 2, 'productid':  7, 'dateid': 4, 'price': 75},
	     {'storeid': 1, 'productid':  7, 'dateid': 4, 'price': 50},
	     {'storeid': 3, 'productid':  9, 'dateid': 4, 'price': 25}]

    # The facts can be inserted using the insert method
    for row in facts:
	factTable.insert(row)
    conn.commit()

    # Lookup returns the keys and measures given only the keys
    row = factTable.lookup({'storeid': 1, 'productid': 13, 'dateid': 4})

    # Ensure should be used when loading facts that might already be loaded
    newFacts = [{'storeid': 2, 'itemid':  7, 'dateid': 4, 'price': 75},
		{'storeid': 1, 'itemid':  7, 'dateid': 4, 'price': 50},
		{'storeid': 1, 'itemid':  2, 'dateid': 7, 'price': 150},
		{'storeid': 3, 'itemid':  3, 'dateid': 6, 'price': 100}]

    for row in newFacts:
	# The second argument forces ensure to not only match the keys for facts
	# to be considered equal, but also checks if the measures are the same
	# for facts with the same key, and if not raises a ValueError. The third
	# argument renames itemid to productid using a name mapping
	factTable.ensure(row, True, {'productid': 'itemid'})
    conn.commit()
    conn.close()

BatchFactTable
--------------
:class:`.BatchFactTable` loads facts into the fact table in batches instead of
one at a time like :class:`.FactTable`. Thus reducing the number of round trips
to the database which improves the performance of the ETL flow. The size of each
batch is determined by the :attr:`batchsize` parameter added to the class's
constructor. :class:`.BatchFactTable` loads each batch using either the
:meth:`executemany` method specified in :pep:`249` or a single SQL ``INSERT INTO
facttable VALUES(...)`` statement depending on the value passed to
:attr:`usemultirow` in the classes constructor. The
:meth:`.ConnectionWrapper.commit` method must be called after all facts have
been inserted into the fact table to both ensure that the last batch is loaded
into the database from memory and that the transaction is committed.

.. note:: Both :meth:`.BatchFactTable.lookup` and :meth:`.BatchFactTable.ensure`
	  force the current batch of facts to be an inserted. This is to keep
	  them consistent with all of facts inserted into the fact table. Thus
	  using these methods can reduce the benefit of batching insertions.

BulkFactTable
-------------
:class:`.BulkFactTable` also inserts facts in batches but writes the facts to a
temporary file instead of keeping them in memory. Thus the size of a batch is
limited by the size of the disk instead of the amount of memory available.
However, this prevents :meth:`BulkFactTable.lookup` and
:meth:`BulkFactTable.ensure` from being implemented efficiently, so these
methods are not available. Like for :class:`.BatchFactTable`, the method
:meth:`.ConnectionWrapper.commit` must be called to ensure that the last batch
of facts is loaded into the database. Multiple additional parameters have been
added to the class's constructor to provide control over the temporary file used
to store facts, such as what delimiters to use and the number of facts to be
bulk loaded in each batch. All of these parameters have a default value except
for :attr:`.bulkloader`. This parameter must be passed a function that will be
called for each batch of facts to be loaded. This is necessary as the exact way
to perform bulk loading differs from RDBMS to RDBMS.

.. py:function:: func(name, attributes, fieldsep, rowsep, nullval, filehandle):

    Required signature of a function bulk loading data from a file into an RDBMS
    in pygrametl. For more information about bulk loading see
    :ref:`bulkloading`.

    **Arguments:**

    - name: the name of the fact table in the data warehouse.
    - attributes: a list containing both the sequence of attributes constituting
      the primary key of the fact table, as well as the measures.
    - fieldsep: the string used to separate fields in the temporary file.
    - rowsep: the string used to separate rows in the temporary file.
    - nullval: if the :class:`.BulkFactTable` was passed a string to substitute
      None values with, then it will be passed, if not then None is passed.
    - filehandle: either the name of the file or the file object itself,
      depending upon the value of :attr:`.BulkFactTable.usefilename`. Using
      the filename is necessary if the bulk loading is invoked through SQL
      (instead of directly via a method on the PEP249 driver). It is also
      necessary if the bulkloader runs in another process.


In the following example, a :class:`.BulkFactTable` is used to bulk load facts
into a data warehouse using function :func:`pgbulkloader`. For information about
how to bulk loading data into other RDBMSs see :ref:`bulkloading`.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import BulkFactTable

    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    facts = [{'storeid': 1, 'productid': 13, 'dateid': 4, 'price': 50},
	     {'storeid': 2, 'productid':  7, 'dateid': 4, 'price': 75},
	     {'storeid': 1, 'productid':  7, 'dateid': 4, 'price': 50},
	     {'storeid': 3, 'productid':  9, 'dateid': 4, 'price': 25}]


    # This function bulk loads a file into PostgreSQL using psycopg2
    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
	cursor = conn.cursor()
	# psycopg2 does not accept the default value used to represent NULL
	# by BulkDimension, which is None. Here this is ignored as we have no
	# NULL values that we wish to substitute for a more descriptive value
	cursor.copy_from(file=filehandle, table=name, sep=fieldsep,
			 columns=attributes)


    # The bulk loading function must be passed to BulkFactTable's constructor
    factTable = BulkFactTable(
	name='facttable',
	measures=['price'],
	keyrefs=['storeid', 'productid', 'dateid'],
	bulkloader=pgbulkloader)

    # commit() and close() must be called to ensure that all facts have been
    # inserted into the database and that the connection is closed correctly
    #  afterward
    for row in facts:
	factTable.insert(row)
    conn.commit()
    conn.close()

AccumulatingSnapshotFactTable
-----------------------------
:class:`.AccumulatingSnapshotFactTable` represents a fact table where facts are
updated as a process evolves. Typically different date references (OrderDate,
PaymentDate, ShipDate, DeliveryDate, etc.) are set when they become known.
Measures (e.g., measuring the lag between the different dates) are also often
set as they become available. Like for :class:`.FactTable`, the class
:class:`.AccumulatingSnapshotFactTable` performs an insert in the database
whenever the :meth:`.AccumulatingSnapshotFactTable.insert` method is called. The
following example illustrates how to create the class:

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import AccumulatingSnapshotFactTable

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # This ConnectionWrapper will be set as a default and is then implicitly
    # used, but it is stored in conn so transactions can be committed and the
    # connection closed
    conn = pygrametl.ConnectionWrapper(connection=pgconn)


    # A factexpander can be used to modify a row only if it has been updated, note
    # that we only ignore namemapping for brevity, production code should use it
    def computelag(row, namemapping, updated):
	if 'shipmentdateid' in updated:
	    row['shipmentlag'] = row['shipmentdateid'] - row['paymentdateid']
	if 'deliverydateid' in updated:
	    row['deliverylag'] = row['deliverydate'] - row['shipmentdateid']


    # This instance of AccumulatingSnapshotFactTable connects to the table
    # orderprocessing in the database using the connection created above
    asft = AccumulatingSnapshotFactTable(
	name='orderprocessing',
	keyrefs=['orderid', 'customerid', 'productid'],
	otherrefs=['paymentdateid', 'shipmentdateid', 'deliverydateid'],
	measures=['price', 'shipmentlag', 'deliverylag'],
	factexpander=computelag)

Firstly a :PEP:`249` connection is created to perform the actual database
operations, then an instance of the :class:`.ConnectionWrapper` is created as a
uniform wrapper around the :PEP:`249` connection which is set as the default database
connection for this ETL flow. Then a user-defined function to compute lag measures
is defined. Lastly, an :class:`.AccumulatingSnapshotFactTable` is created.

As stated :meth:`.AccumulatingSnapshotFactTable.insert` inserts new facts
directly into the fact table when they are passed to the method.
:meth:`.AccumulatingSnapshotFactTable.lookup` checks if the database contains a
fact with the given combination of keys referencing the dimensions. These
methods behave in the same way as in :class:`.FactTable`. The method
:meth:`.AccumulatingSnapshotFactTable.update`, will based on the :attr:`keyrefs`,
find the fact and update it if there are any differences in :attr:`otherrefs`
and :attr:`measures`. The method :meth:`.AccumulatingSnapshotFactTable.ensure`
checks if the row it is given, already exists in the database table. If it does
not exist, it is immediately inserted. If it exists, the method will see if some
of the values for :attr:`otherrefs` or :attr:`measures` have been updated in the
passed row. If so, it will update the row in the database. Before that, it will,
however, run the :func:`factexpander` if one was given to
:meth:`.AccumulatingSnapshotFactTable.__init__` when the object was created.
Note that the generated SQL for lookups and updates will use the :attr:`keyrefs`
in the ``WHERE`` clause and an index on them should be considered. An example of
how to use the class can be seen below:

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import AccumulatingSnapshotFactTable

    # The actual database connection is handled by a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
			      password='dwpass'""")

    # A factexpander can be used to modify a row only if it has been updated, note
    # that we only ignore namemapping for brevity, production code should use it
    conn = pygrametl.ConnectionWrapper(connection=pgconn)


    # A factexpander can be used to modify a row only if it has been updated, note
    # that we only ignore namemapping for brevity, production code should use it
    def computelag(row, namemapping, updated):
	if 'shipmentdateid' in updated:
	    row['shipmentlag'] = row['shipmentdateid'] - row['paymentdateid']
	if 'deliverydateid' in updated:
	    row['deliverylag'] = row['deliverydate'] - row['shipmentdateid']


    # This instance of AccumulatingSnapshotFactTable connects to the table
    # orderprocessing in the database using the connection created above
    asft = AccumulatingSnapshotFactTable(
	name='orderprocessing',
	keyrefs=['orderid', 'customerid', 'productid'],
	otherrefs=['paymentdateid', 'shipmentdateid', 'deliverydateid'],
	measures=['price', 'shipmentlag', 'deliverylag'],
	factexpander=computelag)

    # A list of facts that are ready to inserted into the fact table
    facts = [{'orderid': 1, 'customerid': 1, 'productid': 1, 'price': 10},
	     {'orderid': 2, 'customerid': 2, 'productid': 2, 'price': 20},
	     {'orderid': 3, 'customerid': 3, 'productid': 3, 'price': 30}]

    # The facts can be inserted using the ensure method. (If we had used the
    # insert method instead, we should have made sure the facts above had a
    # value for each attribute in the fact table. When using ensure, missing
    # attributes will be set to None before an insertion.)
    for row in facts:
	asft.ensure(row)

    # Now assume that the the orders get paid and shipped
    facts[0]['paymentdateid'] = 12
    facts[0]['shipmentdateid'] = 14
    facts[2]['paymentdateid'] = 11

    # Update the accumulating fact table in the DW
    for row in facts:
	asft.ensure(row)  # will call computelag and do the needed updates

    conn.commit()
    conn.close()
