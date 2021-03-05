.. _install:

Install Guide
=============
Installing *pygrametl* is fairly simple, mainly due to the library having no
mandatory dependencies. This short guide contains all the information needed to
get the framework up and running on CPython, pygrametl also runs on top of
the JVM based Python implementation, Jython. For more information about running
pygrametl on Jython see :ref:`jython`.

Installing a Python Implementation
----------------------------------
Pygrametl requires an implementation of the Python programming language to run.
Currently, pygrametl supports the following implementations:

* `Jython <http://www.jython.org/>`_, version 2.7 or above
* `Python 2 <http://www.python.org/>`_, version 2.7 or above
* `Python 3 <http://www.python.org/>`_, version 3.4 or above

.. warning::
    As Python 2 is no longer being `maintained
    <https://www.python.org/doc/sunset-python-2/>`_ support for it will slowly
    be reduced as we continue to develop pygrametl. Currently :mod:`.dttr` is
    the only pygrametl module that requires Python 3 (version 3.4 or above).

After either implementations have been installed and added to the system's
path, they can be run from either the command prompt in Windows or the shell in
Unix-like systems. This should launch the Python interpreter in interactive
mode allowing commands to be executed directly on the command line. ::

    Python 3.9.2 (default, Feb 20 2021, 18:40:11)
    [GCC 10.2.0] on linux
    Type "help", "copyright", "credits" or "license" for more information.
    >>>


Installing Pygrametl
--------------------
Pygrametl can either be installed from `PyPI
<https://pypi.python.org/pypi/pygrametl/>`_ using a package manager such as
`Pip <https://pip.pypa.io/>`_ or `Conda <http://conda.pydata.org/>`_, or by
manually checking out the latest development version from the official `Github
repository <https://github.com/chrthomsen/pygrametl>`_.  Installing pygrametl
from PyPI is currently the simplest way to install pygrametl as the process is
automated by the package manager. Bug fixes and new experimental features are,
however, of course available first at the Github repository.

Install from PyPI With Pip
##########################
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

Install from PyPI With Conda
############################
Conda is an alternative package manager for Python, bundled with the
`Anaconda <https://store.continuum.io/cshop/anaconda/>`_ distribution of Python
created by `Continuum Analytics <http://www.continuum.io/>`_. Pygrametl does
not currently provide a package for Conda, as it uses a different package
format then Pip. It is however trivial to download, convert and install the
package from PyPI using Conda with a only few commands. ::

    # Create a template for the Conda package using the PyPI package
    $ conda skeleton pypi pygrametl

    # Build the Conda package
    $ conda build

    # Install the Coda package
    $ conda install --use-local pygrametl

After the installation process is completed, the folder containing the package
template can be deleted, as it is only used for building the package.

Install from Github
###################
In order to get the latest development version the source code can be
downloaded from the official `Github repository
<https://github.com/chrthomsen/pygrametl>`_. The project currently uses Git as
version control system, so the repository can be cloned using the following
command. ::

    # Clone the pygrametl repository from Github
    $ git clone https://github.com/chrthomsen/pygrametl.git

In order for Python to import the modules, it be added to :attr:`.sys.path`
either directly in the source code of your Python programs, or by setting
``PYTHONPATH`` if CPython is used, or ``JYTHONPATH`` if Jython is used.  More
information about how `Python modules
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

    Python 3.9.2 (default, Feb 20 2021, 18:40:11)
    [GCC 10.2.0] on linux
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
of pygrametl to :attr:`.sys.path`, before importing the package in your code.

.. code-block:: python

    # The path to the to the pygramelt library is added to the path used by the
    # by the Python interpreter when libraries are being imported, this must be
    # done in all program using a module not included in the global Python path
    import sys
    sys.path.append('/path/to/pygrametl')

    # After the folder have been added to Pythons path can the pygrametl
    # package and all its modules be  imported just like any other module
    import pygrametl
