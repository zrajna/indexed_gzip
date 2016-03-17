#!/usr/bin/env python

from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

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
                  libraries=['z'])
    ])
)
