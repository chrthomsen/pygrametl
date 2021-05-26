.. _install:

Install Guide
=============
Installing pygrametl is fairly simple, mainly due to the package having no
mandatory dependencies. This guide contains all the information needed to
install and use the package with CPython. pygrametl also supports the JVM-based
Python implementation Jython. For more information about using pygrametl with
Jython see :ref:`jython`.

Installing a Python Implementation
----------------------------------
pygrametl requires an implementation of the Python programming language to run.
Currently, pygrametl officially supports the following implementations (other
implementations like `PyPy <https://www.pypy.org/>`__ and `IronPython
<https://ironpython.net/>`__ might also work):

* `Jython <http://www.jython.org/>`__, version 2.7 or above
* `Python 2 <http://www.python.org/>`__, version 2.7 or above
* `Python 3 <http://www.python.org/>`__, version 3.4 or above

.. warning::
    As Python 2 is no longer being `maintained
    <https://www.python.org/doc/sunset-python-2/>`_ support for it will slowly
    be reduced as we continue to develop pygrametl. Currently, :mod:`.dttr` is
    the only pygrametl module that requires Python 3 (version 3.4 or above).

After a Python implementation has been installed and added to the system's
path, it can be run from either the command prompt in Windows or the shell in
Unix-like systems. This should launch the Python interpreter in interactive
mode, allowing commands to be executed directly on the command line. ::

    Python 3.9.2 (default, Feb 20 2021, 18:40:11)
    [GCC 10.2.0] on linux
    Type "help", "copyright", "credits" or "license" for more information.
    >>>


Installing pygrametl
--------------------
pygrametl can either be installed from `PyPI
<https://pypi.python.org/pypi/pygrametl/>`__ using a package manager, such as
`pip <https://pip.pypa.io/>`__ or `conda <http://conda.pydata.org/>`__, or by
manually checking out the latest development version from the official `GitHub
repository <https://github.com/chrthomsen/pygrametl>`__. Installing pygrametl
from `PyPI <https://pypi.python.org/pypi/pygrametl/>`__ is currently the
simplest way to install pygrametl as the process is automated by the package
manager. Bug fixes and new experimental features are, however, of course,
available first in the `GitHub repository
<https://github.com/chrthomsen/pygrametl>`__.

Install from PyPI with pip
##########################
pip can install pygrametl to the Python implementation's global package
directory, or to the user's local package directory which is usually located in
the user's home directory. Installing pygrametl globally will often require root
or administrator privileges with the advantage that the package will be
available to all users of that system. Installing it locally will only make it
available to the current user, but the installation can be performed without
additional privileges. The two types of installation can be performed using one
of the following commands: ::

    # Install pygrametl to the global package directory
    $ pip install pygrametl

    # Install pygrametl to the user's local package directory
    $ pip install pygrametl --user

Install from PyPI with conda
############################
conda is an alternative package manager for Python. It is bundled with the
`Anaconda <https://www.anaconda.com/products/individual>`__ CPython distribution
from `Anaconda, Inc <https://www.anaconda.com/>`__. There is no official
pygrametl conda package as it uses a different package format than pip. It is
however trivial to download, convert, and install the PyPI package using conda
with only a few commands. ::

    # Create a template for the conda package using the PyPI package
    $ conda skeleton pypi pygrametl

    # Build the conda package
    $ conda build pygrametl/meta.yaml

    # Install the conda package
    $ conda install --use-local pygrametl

Afterward, the folder containing the package template can be deleted as it is
only used for building the package.

Install from GitHub
###################
The latest development version of pygrametl can be downloaded from the official
`GitHub repository <https://github.com/chrthomsen/pygrametl>`__. The project
currently uses Git for version control, so the repository can be cloned using
the following command. ::

    # Clone the pygrametl repository from GitHub
    $ git clone https://github.com/chrthomsen/pygrametl.git

Before Python can import the modules, the pygrametl package must be added to
:attr:`.sys.path`. This can be done manually in your Python programs, by setting
``PYTHONPATH`` if CPython is used, or by setting ``JYTHONPATH`` if Jython is
used. More information about how `CPython
<http://docs.python.org/3/tutorial/modules.html#the-module-search-path>`__ and
`Jython
<https://jython.readthedocs.io/en/latest/ModulesPackages/#module-search-path-compilation-and-loading>`__
locate modules can be found in the two links provided.

Verifying Installation
----------------------
A simple way to verify that pygrametl has been installed correctly and is
accessible to the Python interpreter is to start the interpreter in
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
implementation just like any built-in Python package. As an alternative, the
path to pygrametl can be set on a program to program basis, by adding the path
of pygrametl to :attr:`.sys.path`, before importing the package in your code.

.. code-block:: python

    # The path to the pygrametl package is added to the path used by the Python
    # interpreter when modules are being imported, this must be done in all
    # program using a module not included in the default Python path
    import sys
    sys.path.append('/path/to/pygrametl')

    # After the folder is added to Python's path can pygrametl be imported
    import pygrametl
