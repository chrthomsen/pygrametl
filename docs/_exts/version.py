"""Extracts the version number from all source files of the pygrametl package,
   the largest version number is then computed from list of extracted numbers
   and used as the version number for the Pypi package.
"""

# Copyright (c) 2014, Aalborg University (chr@cs.aau.dk)
# All rights reserved.

# Redistribution and use in source anqd binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# - Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
import sys
import glob
import rtdmockup
from distutils.version import StrictVersion

# The location of the package source is different depending on if the script is 
# called by either conf.py or setup.py, so we adjust the path to suit both
if os.path.exists('./conf.py'):
    package_path = '../'
elif os.path.exists('./setup.py'):
    package_path = './'
else:
    # The script cannot operate without accesses to source files
    raise IOError('could not determine correct path of the pygrametl folder')

# The Java elements in pygrametl must be mocked, using the module rtdmockup 
pygrametl_path = package_path + 'pygrametl/'
sys.path.insert(0, os.path.abspath(package_path))
sys.path.insert(0, os.path.abspath(pygrametl_path))
rtdmockup.mockModules(['pygrametl.jythonsupport', 'java', 'java.sql'])

# Extracts the highest version number of the pygrametl python files
def get_package_version():
    # The minimum version number is used for the initial value
    version_number = StrictVersion("0.0") 

    python_files = glob.glob(pygrametl_path + '*.py')
    for python_file in python_files:
        # The path of each module is computed without suffix and version extract
        module_name = os.path.basename(python_file)[:-3]
        version = __import__(module_name).__version__

        # A alpha / beta version without a number is the first alpha / beta
        if version.endswith(('a', 'b')):
            strict_version = StrictVersion(version + '1')
        else:
            strict_version = StrictVersion(version)
        
        # If a higher version number is found then that it is used
        if strict_version > version_number:
            version_number = strict_version

    return str(version_number)
