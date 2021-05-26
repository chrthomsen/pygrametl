from setuptools import find_packages, setup

import os
import sys

import pygrametl

setup(
    name='pygrametl',
    version=pygrametl.__version__,
    author='Aalborg University',
    author_email='pygrametl@cs.aau.dk',
    packages=find_packages(),
    package_data={
        'pygrametl': [
            'jythonsupport/Value.class',
            'jythonsupport/Value.java']},
    url='http://pygrametl.org/',
    license='BSD',
    description='ETL programming in Python',
    long_description=open('README.rst').read(),
    long_description_content_type="text/x-rst",
    classifiers=[
                'Development Status :: 5 - Production/Stable',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: BSD License',
                'Programming Language :: Java',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2.7',
                'Programming Language :: Python :: 3',
                'Topic :: Database',
                'Topic :: Database :: Front-Ends',
                'Topic :: Software Development',
                'Topic :: Software Development :: Libraries :: Python Modules',
                'Topic :: Software Development :: Libraries :: Application '
                'Frameworks'],
    entry_points={
        'console_scripts': [
            'dttr = pygrametl.drawntabletesting.dttr:main']
   }
)
