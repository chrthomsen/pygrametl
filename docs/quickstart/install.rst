.. _install:

Install Guide
=============
Installing *pygrametl* is fairly simple, mainly due to the library having no
mandatory dependencies. This short guide contains all the information needed to
get the framework up and running on CPython, pygrametl also runs on top of
the JVM based Python implementation, Jython. For more information about running
pygrametl on Jython see :ref:`jython`.

:Authors:
    | Christoffer Moesgaard <cmoesgaard@gmail.com> 
    | Søren Kejser Jensen <devel@kejserjensen.dk>

Installing a Python Implementation
----------------------------------
pygrametl requires an implementation of the Python programming language to run.
Currently, pygrametl supports the following implementations:

* `Jython <http://www.jython.org/>`_, version 2.5.3 or above
* `Python 2 <http://www.python.org/>`_, version 2.6 or above
* Python 3. However, it should be noted that Python 3 support is currently in
  beta. 

After either implementations have been installed and added to the system's
path, they can be run from either the command prompt in Windows or the shell in
Unix-like systems. This should launch the Python interpreter in interactive
mode allowing commands to be executed directly on the command line. ::

    Python 2.7.6 (default, Feb 26 2014, 12:07:17) 
    [GCC 4.8.2 20140206 (prerelease)] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>>

Installing Pygrametl 
--------------------
pygrametl can either be installed from `PyPI
<https://pypi.python.org/pypi/pygrametl/>`_ using a PyPI package manager such
as Pip, or by manually checking out the latest development version from the
official `Google Code repository <https://code.google.com/p/pygrametl/>`_.
Installing it from PyPI is currently the simplest way to install pygrametl as
the process is automated by the package manager. Bug fixes and new experimental
features are, however, of course available first at the `Google Code repository
<https://code.google.com/p/pygrametl/>`_.

Install from PyPI 
#################
Installing pygrametl from PyPI can as be done either to a globally
available directory or locally to the user's home directory. Installing
pygrametl globally will on most systems require root or administrator
privileges with the advantage that the framework will be available for all
users of that system, installing it in the user's home directory will 
only make it available to that particular user, but the installation can be
performed without additional privileges. Either of the two types of
installation can be performed using one of the two following commands. ::

    # Command for installing pygrametl global package diretory 
    $ pip install pygrametl

    # Command for installing pygrametl user package direcotry
    $ pip install pygrametl --user
    

Install from Google Code
########################
In order to get the latest development version the source code can be
downloaded from the official `Google Code repository
<https://code.google.com/p/pygrametl/>`_. The project currently uses Subversion
as version control system, so the source can be checked out using the following
command. ::

    # Checks out the pygrametl source code from Goolge Code 
    $ svn checkout http://pygrametl.googlecode.com/svn/trunk/ pygrametl

In order for Python to import the modules, it be added to ``sys.path``
either directly in the source code of your Python programs, or by setting
``PYTHONPATH`` if CPython is used, or ``JYTHONPATH`` if Jython is used.
More information about how `Python modules
<http://docs.python.org/2/tutorial/modules.html#the-module-search-path>`_ and
`Jython Modules
<http://www.jython.org/jythonbook/en/1.0/ModulesPackages.html#module-search-path-and-loading>`_
are located can be found in the two links provided.

Verifying installation
----------------------
A simple way to verify that pygrametl has been installed correctly and is
accessible to the Python interpreter, is to start the interpreter in
interactive mode from the command line and run the command ``import
pygrametl``. ::

    Python 2.7.6 (default, Feb 26 2014, 12:07:17) 
    [GCC 4.8.2 20140206 (prerelease)] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import pygrametl
    >>> 

If this fails with the message ``ImportError: No module named pygrametl`` then
verify that the install location of the package is included in either the
environment variable, ``PYTHONPATH`` if CPython is used, or the environment
variable ``JYTHONPATH`` if Jython is used. By including the location of
pygrametl in these variables, it is available to all instances of that Python
implementation just like any built-in Python library. As an alternative, the
path to pygrametl can be set on a program to program basis, by adding the path
of pygrametl to ``sys.path``, before importing the package in your code.

.. code-block:: python

    # The path to the to the pygramelt library is added to the path used by the
    # by the Python interpreter when libraries are being imported, this must be
    # done in all program using a module not included in the global Python path 
    sys.path.append('/path/to/pygrametl')

    # After the folder have been added to Pythons path can the pygrametl
    # package and all its modules be  imported just like any other module
    import pygrametl

