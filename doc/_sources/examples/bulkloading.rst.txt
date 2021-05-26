.. _bulkloading:

Bulk Loading
============
Bulk loading rows instead of inserting them one at a time can dramatically
increase the throughput of an ETL program. Bulk loading works by loading data
from a temporary file into the database. The actual process of bulk loading is
unfortunately different for each RDBMS. Because of this, a user-defined function
must be created that uses the functionality provided by a particular RDBMS to
bulk load the data from a file. The following is a list of example functions
showing how bulk loading can be performed for some of the more commonly used
RDBMSs.

Currently, three classes in pygrametl use bulk loading: :class:`.BulkDimension`,
:class:`.CachedBulkDimension`, and :class:`.BulkFactTable`. Thus a function that
can bulk load data from a file into the specific RDBMS used for the data
warehouse, must be passed to each of these classes constructors. The function
must have the following signature:

.. py:function:: func(name, attributes, fieldsep, rowsep, nullval, filehandle):

    Required signature of a function bulk loading data from a file into an RDBMS
    in pygrametl.

    :param name: The name of the table in the data warehouse.
    :param attributes: A list containing the sequence of attributes in the table.
    :param fieldsep: The string used to separate fields in the temporary file.
    :param rowsep: The string used to separate rows in the temporary file.
    :param nullval: If the class was passed a string to substitute None values with,
      then it will be passed, if not then None is passed.
    :param filehandle: Either the name of the file or the file object itself,
      depending upon the value of member :attr:`.usefilename` on the class.

PostgreSQL
----------
For PostgreSQL the `copy_from
<http://initd.org/psycopg/docs/cursor.html#cursor.copy_from>`__ method from
psycopg2 can be used:

.. code-block:: python

    # psycopg2
    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
	global connection
	cursor = connection.cursor()
	cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=nullval,
			     columns=attributes)

If Jython is used the `copyIn
<https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html#copyIn-java.lang.String->`__
method in JDBC's :class:`CopyManager` class can be used:

.. code-block:: python

    # JDBC
    def pgcopybulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
	global pgconnection
	copymgr = pgconnection.getCopyAPI()
	sql = "COPY %s(%s) FROM STDIN WITH DELIMITER '%s'" % \
	      (name, ', '.join(attributes), fieldsep)
	copymgr.copyIn(sql, filehandle)

MySQL
-----
For MySQL the `LOAD DATA INFILE
<http://dev.mysql.com/doc/refman/5.7/en/load-data.html>`__ functionality
provided by MySQL SQL dialect can be used.

.. code-block:: python

    # MySQLdb
    def mysqlbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
	global connection
	cursor = connection.cursor()
	sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s' LINES TERMINATED BY '%s' (%s);" % \
		(filehandle, name, fieldsep, rowsep, ', '.join(attributes))
	cursor.execute(sql)

Oracle
------
Oracle supports two methods for bulk loading from text files, SQL Loader and
External Tables. The following example uses SQL Loader as it does not require
the creation of an additional table, which is problematic to do in a bulk
loading function as the data types of each column must be specified.

SQL Loader is part of Oracle's client package. SQL Loader requires all
configuration and data files to have specific suffixes, so a file must be
created with the suffix .dat and passed to any bulk loading table as
:attr:`.tempdest`.

.. code-block:: python

    with tempfile.NamedTemporaryFile(suffix=".dat") as dat_handle:
	BulkDimension(
	    ...
	    tempdest=dat_handle)


The bulk loading function shown below constructs a control file with the .ctl
suffix using the functions arguments. The SQL Loader is then executed (sqlldr
must in the system path) and passed the constructed .ctl file.

.. code-block:: python

    # cx_Oracle or JDBC
    def oraclebulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):

	# The configuration file used by SQL Loader must use the suffix .ctf
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
	    # that the contents are written to the file by flushing it
	    ctl_contents = textwrap.dedent(ctl_handle).lstrip()
	    ctl_handle.write(ctl_contents)
	    ctl_handle.flush()

	    # Bulk loads the data using Oracle's SQL Loader. As a new connection
	    # is created, the same username, passowrd, etc. must be given again
	    os.system("sqlldr username/password@ip:port/sid control=" +
		    str(ctl_handle.name))


Microsoft SQL Server
--------------------
For Microsoft SQL Server the `BULK INSERT
<https://msdn.microsoft.com/en-us/library/ms188365.aspx>`__ functionality
provided by Transact-SQL can be used.

There are a number of things to be aware of when using pygrametl with SQL
Server. If the file used for bulk loading is located on a machine running
Windows, the file must be copied before bulk loading, as the locks placed on the
file by the OS and pygrametl, prevents SQL Server from opening it directly.
Copying the file can be done e.g. using `shutil.copyfile
<https://docs.python.org/3/library/shutil.html#shutil.copyfile>`__.

By default, BULK INSERT ignores column names, so the number and order of columns
must match the table you are inserting into. This can be overcome by adding a
`format file <https://msdn.microsoft.com/en-us/library/ms178129.aspx>`__. In
this case, we create a `non-XML format file
<https://msdn.microsoft.com/en-us/library/ms191479.aspx>`__.

A simple example of bulk loading in SQL Server along with the creation of a
format file can be seen below:

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
