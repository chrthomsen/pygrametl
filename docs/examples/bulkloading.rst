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

There are a number of things to be aware of when using pygrametl with SQL
Server. If the file used for bulk loading is located on a machine running
Windows, the file must be copied before bulk loading, as the locks placed on
the file by the OS and pygrametl, prevents SQL Server from opening it directly.
Copying the file can be done e.g. using `shutil.copyfile
<https://docs.python.org/2/library/shutil.html#shutil.copyfile>`_.

By default, BULK INSERT ignores column names and the number and order of
columns must also match the table you are inserting into. This can be overcome
by adding a `format file
<https://msdn.microsoft.com/en-us/library/ms178129.aspx>`_.  In this case we
create a `non-XML format file
<https://msdn.microsoft.com/en-us/library/ms191479.aspx>`_.

A simple example of bulk loading in SQL Server along with the creation of a
format file, is seen below:

.. code-block:: python

    def sqlserverbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global msconn
        cursor = msconn.cursor()

        # Copy the tempdest
        shutil.copyfile(filehandle, r'd:\dw\tmpfilecopy')

        # Create format file
        fmt = open(r'd:\dw\format.fmt', 'w+')
        # 12.0 corresponds to the version of the bcp utility being used by SQL Server.
        # For more information, see the above link on non-XML format files.
        fmt.write("12.0\r\n%d\r\n" % len(attributes))
        count = 0
        sep = "\\t"
        for a in attributes:
            count += 1
            if count == len(attributes): sep = "\\n"
            # For information regarding the format values, 
            # see the above link on non-XML format files.
            fmt.write('%d SQLCHAR 0 8000 "%s" %d %s "Latin1_General_100_CI_AS_SC"\r\n' % (count, sep, count, a))
        fmt.close()

        sql = "BULK INSERT %s FROM '%s' WITH (FORMATFILE = '%s', FIELDTERMINATOR = '%s', ROWTERMINATOR = '%s')" % \
                (name, r'd:\dw\tmpfilecopy', r'd:\dw\format.fmt', fieldsep, rowsep,)
        cursor.execute(sql)
