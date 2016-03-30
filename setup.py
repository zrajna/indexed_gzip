#!/usr/bin/env python

from setuptools    import setup
from setuptools    import Extension
from Cython.Build  import cythonize


# TODO
# if Cython present:
#     build indexed_gzip.pyx
#
# else:
#     build pre-generated indexed_gzip.c
#

setup(
    ext_modules=cythonize([
        Extension('indexed_gzip',
                  ['indexed_gzip.pyx', 'zran.c'],
                  libraries=['z'],
                  extra_compile_args=['-Wno-unused-function']),
        
        Extension('tests.ctest_zran',
                  ['tests/ctest_zran.pyx', 'zran.c'],
                  libraries=['z'],
                  include_dirs=['.'],
                  extra_compile_args=['-Wno-unused-function'])
    ]),
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite='tests',
)
