pygrametl
=========
`pygrametl <http://pygrametl.com>`_ (pronounced py-gram-e-t-l) is a Python framework that provides commonly used functionality for the development of Extract-Transform-Load (ETL) processes. It is open-source and released under a 2-clause BSD license.

When using pygrametl, the developer codes the ETL process in Python code. This turns out to be very efficient, also when compared to drawing the process in a graphical user interface (GUI) like Pentaho Data Integration. It supports both CPython and Jython so existing Java code and JDBC drivers can be used in the ETL program.

Concretely, the developer creates an object for each dimension and fact table. (S)he can then easily add new members by :code:`dimension.insert(row)` where :code:`row` is a :code:`dict` holding the values to insert. This is a very simple example, but pygrametl also supports much more complicated scenarios. For example, it is possible to create a single object for a snowflaked dimension. It is then still possible to add a new dimension member with a single method call as in :code:`snowflake.insert(row)`. This will automatically do the necessary lookups and insertions in the tables participating in the snowflake.

pygrametl also supports slowly changing dimensions. Again, the programmer only has to invoke a single method: :code:`scdim.scdensure(row)`. This will perform the needed updates of both type 1 (i.e., overwrites) and type 2 (i.e., adding new versions).

pygrametl was first made publicly available in 2009. Since then, we have made multiple improvements and added new features. Version 2.6 was released in December 2018. Today, pygrametl is used in production systems in different sectors such as healthcare, finance, and transport.

Installation
------------
pygrametl can be installed from `PyPI <https://pypi.org/project/pygrametl/>`_ with the following command:

:code:`$ pip install pygrametl`

The current development version of pygrametl is available on `GitHub <https://github.com/chrthomsen/pygrametl>`_:

:code:`$ git clone https://github.com/chrthomsen/pygrametl.git`

For more information about installation see the `Install Guide <http://pygrametl.com/doc/quickstart/install.html>`_.

Documentation
-------------
The documentation is available in `HTML <http://pygrametl.com/doc/index.html>`_ and as a `PDF <http://pygrametl.com/doc/pygrametl.pdf>`_. There are also `installation <http://pygrametl.com/doc/quickstart/install.html>`_ and `beginner <http://pygrametl.com/doc/quickstart/beginner.html>`_ guides available.

In addition to the documentation, multiple papers have been published about pygrametl. The papers are listed `here <http://pygrametl.com/#documentation>`_ and provide a more detailed description of the foundational ideas behind pygrametl but is obviously not keep up to date with changes and improvements implemented in the framework, for such see the documentation. If you use pygrametl in academia, please cite the relevant paper(s).

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
