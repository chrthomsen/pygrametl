pygrametl
=========
|badge1| |badge2|

.. |badge1| image:: https://github.com/chrthomsen/pygrametl/actions/workflows/python-unittest-on-pr-and-push.yml/badge.svg
   :target: https://github.com/chrthomsen/pygrametl/actions

.. |badge2| image:: https://img.shields.io/pypi/dm/pygrametl?style=flat&label=Downloads
   :target: https://pypi.org/project/pygrametl/

`pygrametl <http://pygrametl.org>`_ (pronounced py-gram-e-t-l) is a Python framework that provides functionality commonly used when developing Extract-Transform-Load (ETL) programs. It is fully open-source and released under a 2-clause BSD license. As shown in the figure below, an ETL program that uses pygrametl is a standard Python program that imports pygrametl and uses the abstractions it provides. To provide developers with complete control over the data warehouse's schema, pygrametl assumes that all of the dimension tables and fact tables used in the ETL program have already been created using SQL.

.. image:: https://pygrametl.org/assets/etl-with-pygrametl.svg

Defining the data warehouse's schema using SQL and implementing the ETL program itself using standard Python turns out to be very efficient and effective, even when compared to drawing the program in a graphical user interface like Apache Hop or Pentaho Data Integration. pygrametl supports CPython and Jython so both existing Python code that uses native extensions models and PEP 249 connectors and JVM-based code that uses JDBC drivers can be used in the ETL program.

When using pygrametl, the developer creates an object for each data source, dimension and fact table and operate on rows in the form of standard Python ``dict``\s. Thus, (s)he can easily read rows from a data source using a loop like ``for row in datasource:``, transform the rows using arbitrary Python code like ``row["price"] *= 1.25``, and then add new dimension members to a dimension and facts to a fact table using ``dimension.insert(row)`` and ``facttable.insert(row)``, respectively. This is a very simple example, but pygrametl also supports much more complicated scenarios. For example, it is possible to create a single object for an entire snowflaked dimension. It is then possible to add a new dimension member with a single method call by using ``snowflake.insert(row)``. This will automatically perform all of the necessary lookups and insertions in the tables participating in the snowflaked dimension. pygrametl also supports multiple types of slowly changing dimensions. Again, the programmer only has to invoke a single method: ``slowlychanging.scdensure(row)``. This will perform the needed updates of both type 1 (i.e., overwrites) and type 2 (i.e., adding new versions).

pygrametl was first made publicly available in 2009. Since then, we have continuously made improvements and added new features. Version 2.8 was released in September 2023. Today, pygrametl is used in production systems in different sectors such as healthcare, finance, and transport.

Installation
------------
pygrametl can be installed from `PyPI <https://pypi.org/project/pygrametl/>`_ with the following command:

:code:`$ pip install pygrametl`

The current development version of pygrametl is available on `GitHub <https://github.com/chrthomsen/pygrametl>`_:

:code:`$ git clone https://github.com/chrthomsen/pygrametl.git`

For more information about installation see the `Install Guide <http://pygrametl.org/doc/quickstart/install.html>`_.

Documentation
-------------
The documentation is available in `HTML <http://pygrametl.org/doc/index.html>`_ and as a `PDF <http://pygrametl.org/doc/pygrametl.pdf>`_. There are also `installation <http://pygrametl.org/doc/quickstart/install.html>`_ and `beginner <http://pygrametl.org/doc/quickstart/beginner.html>`_ guides available.

In addition to the documentation, multiple papers have been published about pygrametl. The papers are listed `here <http://pygrametl.org/#documentation>`_ and provide a more detailed description of the foundational ideas behind pygrametl but is obviously not keep up to date with changes and improvements implemented in the framework, for such see the documentation. If you use pygrametl in academia, please cite the relevant paper(s).

Community
---------
To keep the development of pygrametl open for external participation, we have public mailing lists and use Github. Feel free to ask questions and provide all kinds of feedback:

- `pygrametl-user <https://groups.google.com/forum/#!forum/pygrametl-user>`_ - For any questions about how to deploy and utilize pygrametl for ETL.
- `pygrametl-dev <https://groups.google.com/forum/#!forum/pygrametl-dev>`_ - For - questions and discussion about the development of pygrametl.
- `Github <https://github.com/chrthomsen/pygrametl>`_ - Bugs and patches should be submitted to Github as issues and pull requests.

When asking a question or reporting a possible bug in pygrametl, please first verify that the problem still occurs with the latest version of pygrametl. If the problem persists after updating please include the following information, preferably with detailed version information, when reporting the problem:

- Operating System.
- Python Implementation.
- Relational Database Management System.
- Python Database Connector.
- A short description of the problem with a minimal code example that reproduces the problem.

We encourage the use of Github and the mailing lists. For discussions not suitable for a public mailing list, you can, however, send us a private `email <mailto:pygrametl@cs.aau.dk>`_.

Maintainers
-----------
pygrametl is maintained at `Aalborg University <http://www.cs.aau.dk/>`_ by `Christian Thomsen <https://github.com/chrthomsen>`_ and `Søren Kejser Jensen <https://github.com/skejserjensen>`_.
