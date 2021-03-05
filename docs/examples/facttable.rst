.. _facttables:

Fact Tables
===========
*pygrametl* provides multiple classes for representing fact tables, in order
to support both serial and parallel loading of facts into the table. For more
information about the parallel capabilities of pygrametl see :ref:`parallel`.
In the following examples we use PostgreSQL and psycopg2 as the database driver.

All of these classes are currently implemented in the
:mod:`.pygrametl.tables` module.

FactTable
---------
The most basic class for performing operations on the fact table is the class
:class:`.FactTable`. However, this class perform an insert in the database
whenever the :meth:`.FactTable.insert` method is called, which can very quickly
become a bottleneck. Using the :class:`.FactTable` class is very simple and
only requires that an appropriate table has been created in the database
beforehand, and that a connection to the database has been created using an
instance of the class :class:`.ConnectionWrapper`.

For more information about database connections see :ref:`database`

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import FactTable

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used,
    # a reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The instance of FactTable connects to the table "facttable" in the
    # database using the default connection wrapper we just created
    factTable = FactTable(
        name='facttable',
        measures=['price'],
        keyrefs=['storeid', 'productid', 'dateid'])

The above example shows the three step process needed to connect an instance of
:class:`.FactTable` to an existing database table. First a :PEP:`249`
connection is created which performs the actual database operations, then an
instance of the :class:`.ConnectionWrapper` is created as a uniform wrapper
around the PEP connection which is set as the default database connection for
pygrametl.  Lastly a :class:`.FactTable` is created as a representation of the
actual database table.

Operations on the fact table are done using three methods, each taking as their
primary arguments two :class:`dict`. The first is used as a row where the keys
match the column names of the table, while the second is a set of name mappings
used to rename keys in a row if they do not match the database table.

:meth:`.FactTable.insert` inserts new facts directly into the fact table when
they are passed to the method. :meth:`.FactTable.lookup` checks if the database
contains a fact with the given combination of keys referencing the dimensions.
The last method :meth:`.FactTable.ensure` combines :meth:`.FactTable.lookup`
and :meth:`.FactTable.insert` by first ensuring that a fact does not already
exist in the table before inserting it. An example of each function and the
automatic name mapping can be seen below, where the fact table from the last
example is reused.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import FactTable

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used,
    # a reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)

    # The instance of FactTable connects to the table "facttable" in the
    # database using the default connection wrapper we just created
    factTable = FactTable(
        name='facttable',
        measures=['price'],
        keyrefs=['storeid', 'productid', 'dateid'])

    # A list of facts are ready to inserted into the fact table
    facts = [{'storeid': 1, 'productid': 13, 'dateid': 4, 'price': 50},
             {'storeid': 2, 'productid':  7, 'dateid': 4, 'price': 75},
             {'storeid': 1, 'productid':  7, 'dateid': 4, 'price': 50},
             {'storeid': 3, 'productid':  9, 'dateid': 4, 'price': 25}]

    # The facts can be inserted using the insert method, before committing to DB
    for row in facts:
        factTable.insert(row)
    conn.commit()

    # Lookup retunes all both keys and measures given only the keys
    factTable.lookup({'storeid': 1, 'productid': 13, 'dateid': 4})

    # If a set of facts contain facts already existing in the database can the
    # ensure method be used instead of calling lookup and insert manually, we
    # also rename 'itemid' to 'productid' using the name mapping feature
    newFacts = [{'storeid': 2, 'itemid':  7, 'dateid': 4, 'price': 75},
                {'storeid': 1, 'itemid':  7, 'dateid': 4, 'price': 50},
                {'storeid': 1, 'itemid':  2, 'dateid': 7, 'price': 150},
                {'storeid': 3, 'itemid':  3, 'dateid': 6, 'price': 100}]

    for row in newFacts:
        # The second argument forces FactTable.ensure to not only match the keys
        # for facts to be considered equal, but also checks if the measures are
        # the same for facts with the same key, and if not raises a ValueError
        factTable.ensure(row, True, {'productid': 'itemid'})
    conn.commit()
    conn.close()

BatchFactTable
--------------
:class:`.BatchFactTable` is a specialised version of :class:`.FactTable` which
inserts facts into the fact table in batches, instead of one at the time,
thereby reducing the number of statements executed against the database,
improving the overall performance of the ETL application. The size of each
batch inserted into the database is determined by an additional argument to the
class initialiser method. The :meth:`.ConnectionWrapper.commit` must be called
after all facts have been inserted into the fact table to both ensure that the
last batch is loaded into the database from memory, and that the transaction is
committed.

.. note:: To keep :meth:`.BatchFactTable.lookup` and
        :meth:`.BatchFactTable.ensure` consistent with all facts inserted into
        the fact table, both methods force an insertion of facts. Use of
        these method can therefore reduce the benefit of batching insertions.

BulkFactTable
-------------
:class:`.BulkFactTable` also performs insertion in batches but writes facts to
a temporary file instead of keeping them in memory, allowing for batched
insertions to be limited by disk space instead of memory. This however prevents
database lookups to be performed consistently without reading the temporary
file store on disk as well, so the methods :meth:`.BulkFactTable.lookup` and
:meth:`.BulkFactTable.ensure` are not available. Like the other table classes,
the method :meth:`.ConnectionWrapper.commit` must be called to ensure that the
remaining set of facts are inserted into the fact table, and that the
transaction is committed. Multiple additional parameters are added to the class
initialiser method allowing control over the temporary file used to store
facts, such as specific delimiters and the number of facts to be bulk loaded. All
of these parameters provide a default value except for :attr:`.bulkloader`.
This parameter must be passed a function to be called for each batch of facts
to be loaded, this is necessary as the exact way to perform bulk loading
differs from DBMS to DBMS.

.. py:function:: func(name, attributes, fieldsep, rowsep, nullval, filehandle):

    Expected signature of a bulk loader function passed to
    :class:`.BulkFactTable`, the exact value passed to the function by
    :class:`.BulkFactTable` depends upon the parameters passed when the was
    object instantiated. For more information about bulkloading see
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
      depending upon on the value of :attr:`.BulkFactTable.usefilename`. Using
      the filename is necessary if the bulk loading is invoked through SQL
      (instead of directly via a method on the PEP249 driver). It is also
      necessary if the bulkloader runs in another process.


In the following example we use a :class:`.BulkFactTable` to load facts into a
data warehouse, with the bulk loading itself done by our own bulk loading
function. For information about how to perform bulk loading using other DBMS
than PostresSQL see the documentation for that particular DBMS and the database
driver used.

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


    # How to perform the bulk loading using psycopg2 is defined as this function
    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        cursor = conn.cursor()
        # psycopg2 does not accept the default value used for null substitutes
        # bv BulkFactTable, which is None, so we just ignore it as we have no
        # null values that we wish to substitute for a more descriptive value
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep,
                         columns=attributes)


    # The bulk loading function must be passed to the BulkFactTable on creation
    factTable = BulkFactTable(
        name='facttable',
        measures=['price'],
        keyrefs=['storeid', 'productid', 'dateid'],
        bulkloader=pgbulkloader)

    # After all the facts are inserted must commit() be called to ensure that
    # the temporary file is empty and all facts inserted into the database and
    # the last transaction is committed
    for row in facts:
        factTable.insert(row)
    conn.commit()
    conn.close()

AccumulatingSnapshotFactTable
-----------------------------
The class :class:`.AccumulatingSnapshotFactTable` supports accumulating
snapshot fact tables. Fact rows in accumulating fact tables are updated as a
process evolves. Typically different date references (OrderData, PaymentDate,
ShipDate, DeliveryDate, etc.) are set when they become known. Measures (e.g.,
measuring the lag between the different dates) are also often set as they
become available.

Like for :class:`.FactTable`, the class :class:`.AccumulatingSnapshotFactTable`
performs an insert in the database whenever the :meth:`.FactTable.insert`
method is called.

The following illustrates how to create use the class.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import AccumulatingSnapshotFactTable

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used,
    # a reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)


    # A factexpander can be used to modify a row only if it has been updated, note
    # that we only ignore namemapping for brevity, production code should use it.
    def computelag(row, namemapping, updated):
        if 'shipmentdateid' in updated:
            row['shipmentlag'] = row['shipmentdateid'] - row['paymentdateid']
        if 'deliverydateid' in updated:
            row['deliverylag'] = row['deliverydate'] - row['shipmentdateid']


    # The instance of AccumulatingSnapshotFactTable connects to the table
    # "orderprocessing" in the database using the default connection wrapper
    # we created above
    asft = AccumulatingSnapshotFactTable(
        name='orderprocessing',
        keyrefs=['orderid', 'customerid', 'productid'],
        otherrefs=['paymentdateid', 'shipmentdateid', 'deliverydateid'],
        measures=['price', 'shipmentlag', 'deliverylag'],
        factexpander=computelag)

First a :PEP:`249` connection is created to perform the actual database
operations, then an instance of the :class:`.ConnectionWrapper` is created as
a uniform wrapper around the PEP connection which is set as the default
database connection for pygrametl.  Then a user-defined function to compute lag
measures is defined. Lastly an :class:`.AccumulatingSnapshotFactTable` is
created.

:meth:`.AccumulatingSnapshotFactTable.insert` inserts new facts directly into
the fact table when they are passed to the method. :meth:`.FactTable.lookup`
checks if the database contains a fact with the given combination of keys
referencing the dimensions.  These methods behave in the same way as in
:class:`.FactTable`. The method :meth:`.AccumulatingSnapshotFactTable.update`
will based on the keyrefs find the fact and detect any differences in
otherrefs and measures and then do any necessary updates to the row in the
database. The method :meth:`.AccumulatingSnapshotFactTable.ensure` will see if
the row it is given, already exists in the database table. If it does not
exist there, it is immediately inserted. If it exsits, the method will see if
some of the values for otherrefs or measures have been updated in the passed
row. If so, it will update the row in the database. Before that it will,
however, run the factexpander if such one was given to
:meth:`.AccumulatingSnapshotFactTable.__init__` when the object was created.
Note that the generated SQL for lookups and updates will use the keyrefs
in the WHERE condition and an index on them should be considered.

An example of how to use the class can be seen below.

.. code-block:: python

    import psycopg2
    import pygrametl
    from pygrametl.tables import AccumulatingSnapshotFactTable

    # The actual database connection is handled using a PEP 249 connection
    pgconn = psycopg2.connect("""host='localhost' dbname='dw' user='dwuser'
                              password='dwpass'""")

    # This ConnectionWrapper will be set as default and is then implicitly used,
    # a reference to the wrapper is saved to allow for easy access of it later
    conn = pygrametl.ConnectionWrapper(connection=pgconn)


    # A factexpander can be used to modify a row only if it has been updated, note
    # that we only ignore namemapping for brevity, production code should use it.
    def computelag(row, namemapping, updated):
        if 'shipmentdateid' in updated:
            row['shipmentlag'] = row['shipmentdateid'] - row['paymentdateid']
        if 'deliverydateid' in updated:
            row['deliverylag'] = row['deliverydate'] - row['shipmentdateid']


    # The instance of AccumulatingSnapshotFactTable connects to the table
    # "orderprocessing" in the database using the default connection wrapper
    # we created above
    asft = AccumulatingSnapshotFactTable(
        name='orderprocessing',
        keyrefs=['orderid', 'customerid', 'productid'],
        otherrefs=['paymentdateid', 'shipmentdateid', 'deliverydateid'],
        measures=['price', 'shipmentlag', 'deliverylag'],
        factexpander=computelag)

    # A list of facts are ready to inserted into the fact table
    facts = [{'orderid': 1, 'customerid': 1, 'productid': 1, 'price': 10},
             {'orderid': 2, 'customerid': 2, 'productid': 2, 'price': 20},
	     {'orderid': 3, 'customerid': 3, 'productid': 3, 'price': 30}]

    # The facts can be inserted using the ensure method. (If we had used the
    # insert method instead, we should have made sure the the facts above
    # had a value for each attribute in the fact table. When using ensure,
    # "missing" attributes will be set to None before an insertion.)
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
