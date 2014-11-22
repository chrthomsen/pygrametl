# Adds the location of the version extraction script also used by sphinx
import os
import sys
sys.path.insert(0, os.path.abspath('docs/_exts'))

# The beginning of the main setup function
from version import get_package_version
from distutils.core import setup

setup(
    name='pygrametl',
    version=get_package_version(),
    author='Aalborg University',
    author_email='chr@cs.aau.dk',
    packages=['pygrametl'],
    package_data={'pygrametl': ['jythonsupport/Value.class', 'jythonsupport/Value.java']},
    url='http://pygrametl.org/',
    license='BSD',
    description='ETL programming in Python',
    long_description=open('README.txt').read(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Java',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries :: Application Frameworks'
    ],
)
