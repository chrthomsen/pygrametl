.. _bulkloading:

Bulk Loading
============
Performing bulk loading of rows instead of single insertions can dramatically
increase the performance of an ETL program. Bulk loading works by loading data
from a temporary file into the database. The actual process of bulk loading
however differs between each database vendor. Because of this, a user defined
function must be created defining how to perform the bulk loading on the
particular database. The following is a short list of example functions showing
how simple bulk loading can be performed with some of the more widely used
databases.

Currently three classes in pygrametl use bulk loading, :class:`.BulkDimension`,
:class:`.BulkFactTable`, and :class:`.CachedBulkDimension`, meaning that they
require the specification of a loader function. All three classes accept a
function with the following signature to be passed as a parameter to their
constructors.

.. py:function:: func(name, attributes, fieldsep, rowsep, nullval, filehandle):

    Expected signature of a bulk loader function by either classes.

    :param name: The name of the table in the data warehouse.
    :param attributes: A list containing the sequence of attributes in the table.
    :param fieldsep: The string used to separate fields in the temporary file.
    :param rowsep: The string used to separate rows in the temporary file.
    :param nullval: If the class was passed a string to substitute None values with,
      then it will be passed, if not then None is passed.
    :param filehandle: Either the name of the file or the file object itself,
      depending upon on the value of member :attr:`.usefilename` on the class.

PostgreSQL
----------
For PostgreSQL we can use the `copy_from
<http://initd.org/psycopg/docs/cursor.html#cursor.copy_from>`_ function from
Psycopg

.. code-block:: python

    #Psycopg
    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global connection
        cursor = connection.cursor()
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=nullval,
                             columns=attributes)

For PostgreSQL using Jython, we can use the `copyIn
<https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html>`_
function from the JDBC CopyManager

.. code-block:: python

    #JDBC
    def pgcopybulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global pgconnection
        copymgr = pgconnection.getCopyAPI()
        sql = "COPY %s(%s) FROM STDIN WITH DELIMITER '%s'" % \
              (name, ', '.join(attributes), fieldsep)
        copymgr.copyIn(sql, filehandle)

MySQL
-----
For MySQL we can use the `LOAD DATA INFILE
<http://dev.mysql.com/doc/refman/5.7/en/load-data.html>`_ functionality
included in the language supported by MySQL.

.. code-block:: python

    #MySQLdb
    def mysqlbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global connection
        cursor = connection.cursor()
        sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s' LINES TERMINATED BY '%s' (%s);" % \
                (filehandle, name, fieldsep, rowsep, ', '.join(attributes))
        cursor.execute(sql)

Oracle
------
Oracle supports two methods for bulkloading from text files, SQL Loader and
External Tables. The following example uses SQL Loader as it does not require
the creation of an additional table, which becomes problematic to do in a
program due to the need for specifying data types.

SQL Loader is part of Oracle's client package which must be installed, also
SQL Loader requires all configuration and data files to have specific suffixes,
so a file must be created with the suffix .dat and passed to any bulkloading
table as :attr:`.tempdest`.

.. code-block:: python

    with tempfile.NamedTemporaryFile(suffix=".dat") as dat_handle:
        BulkDimension(
            ...
            tempdest=dat_handle)


The example below shows the bulkloading function, where the .ctl file is
constructed based on the arguments given, and SQL Loader is executed using the
generated .ctl file.

.. code-block:: python

    #cx_Oracle or JDBC
    def oraclebulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):

        # The configuration file used by SQL Loader must have the suffix .ctf
        with tempfile.NamedTemporaryFile(suffix=".ctl") as ctl_handle:

            # The attributes to be loaded must be qouted using double quotes
            unqouted_atts = str(tuple(attributes)).replace("'", "")
            ctl_contents = """
                LOAD DATA INFILE '%s' "str %r"
                APPEND INTO TABLE %s
                FIELDS TERMINATED BY %r
                %s
                """ % (filehandle.name, rowsep, name, fieldsep, unqouted_atts)

            # Strips the multi line string of unnecessary indention, and ensures
            # the contents are written to the file by flushing it
            ctl_contents = textwrap.dedent(ctl_handle).lstrip()
            ctl_handle.write(ctl_contents)
            ctl_handle.flush()

            # Bulk loads the data using Oracle DB's SQL Loader. As a new
            # connection is created, the same username, passowrd etc. must be given
            os.system("sqlldr username/password@ip:port/sid control=" +
                    str(ctl_handle.name))


Microsoft SQL Server
--------------------
For Microsoft SQL Server we can use the `BULK INSERT
<https://msdn.microsoft.com/en-us/library/ms188365.aspx>`_ functionality
included in Transact-SQL.

.. code-block:: python

    #pymssql
    def sqlserverbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global connection
        cursor = connection.cursor()
        sql = "BULK INSERT %s FROM '%s' WITH (FIELDTERMINATOR = '%s', ROWTERMINATOR = '%s')" % \
                (name, filehandle, fieldsep, rowsep,)
        cursor.execute(sql)
