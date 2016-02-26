#!/usr/bin/env python

from distutils.core import setup, Extension
setup(ext_modules=[Extension("indexed_gzip", ["zran.c", "indexed_gzip.c"])])
