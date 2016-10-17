#!/usr/bin/env python

import os
import os.path as op
import shutil

from setuptools import setup
from setuptools import Extension
from setuptools import Command


# Custom 'clean' command
class Clean(Command):

    user_options = []
    
    def initialize_options(self): pass
    def finalize_options(  self): pass 
    
    def run(self):

        shutil.rmtree('build',                 ignore_errors=True)
        shutil.rmtree('dist',                  ignore_errors=True)
        shutil.rmtree('indexed_gzip.egg-info', ignore_errors=True)
        shutil.rmtree('.eggs',                 ignore_errors=True)

        files = [
            'indexed_gzip.c',
            'setup.pyc',
            op.join('tests', 'ctest_zran.c'),
            op.join('tests', 'ctest_indexed_gzip.c'),
            op.join('tests', '__init__.pyc'),
            op.join('tests', 'conftest.pyc'),
            op.join('tests', 'test_zran.pyc'),
            op.join('tests', 'test_indexed_gzip.pyc')]

        for f in files:
            try:            os.remove(f)
            except OSError: pass


# If cython is present, we'll compile
# the pyx files from scratch. Otherwise,
# we'll compile the pre-generated c
# files (which are assumed to be present).

# We only need numpy to
# compile the test modules
have_cython = True
have_numpy  = True


try:    from Cython.Build import cythonize
except: have_cython = False

try:    import numpy as np
except: have_numpy = False


# If numpy is present, we need
# to include the headers
include_dirs = ['.']
if have_numpy:
    include_dirs.append(np.get_include())


# Compile from cython files if 
# possible, or compile from c.
if have_cython: pyx_extension = 'pyx'
else:           pyx_extension = 'c'
    

# The indexed_gzip module
igzip_ext = Extension(
    'indexed_gzip',
    ['indexed_gzip.{}'.format(pyx_extension), 'zran.c'],
    libraries=['z'],
    extra_compile_args=['-Wno-unused-function'])

# Optional test modules
test_exts = [
    Extension(
        'tests.ctest_zran',
        ['tests/ctest_zran.{}'.format(pyx_extension), 'zran.c'],
        libraries=['z'],
        include_dirs=include_dirs,
        extra_compile_args=['-Wno-unused-function']),
    Extension(
        'tests.ctest_indexed_gzip',
        ['tests/ctest_indexed_gzip.{}'.format(pyx_extension)],
        libraries=['z'],
        include_dirs=include_dirs,
        extra_compile_args=['-Wno-unused-function'])]


# If we have numpy, we can compile the tests
if have_numpy: extensions = [igzip_ext] + test_exts
else:          extensions = [igzip_ext]


# Cythonize if we can
if have_cython:
    extensions = cythonize(extensions)


setup(
    name='indexed_gzip',
    version='0.3.1',
    author='Paul McCarthy',
    author_email='pauldmccarthy@gmail.com',
    description='Fast random access of gzip files in Python',
    url='https://github.com/pauldmccarthy/indexed_gzip',
    license='zlib',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: zlib/libpng License',
        'Programming Language :: C',
        'Programming Language :: Cython',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Archiving :: Compression',
    ],

    cmdclass={'clean' : Clean},
    
    ext_modules=extensions,

    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'numpy'],
    test_suite='tests',
)
