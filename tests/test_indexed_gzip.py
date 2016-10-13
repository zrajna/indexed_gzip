#!/usr/bin/env python
#
# test_indexed_gzip.py - Python wrapper around ctest_indexed_gzip.pyx.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#


import pytest

import numpy as np

from . import ctest_indexed_gzip


pytestmark = pytest.mark.indexed_gzip_test


def test_open_close(             testfile, nelems, seed):         ctest_indexed_gzip.test_open_close(             testfile, nelems, seed)
def test_open_close_ctxmanager(  testfile, nelems, seed):         ctest_indexed_gzip.test_open_close_ctxmanager(  testfile, nelems, seed)
def test_create_from_open_handle(testfile, nelems, seed):         ctest_indexed_gzip.test_create_from_open_handle(testfile, nelems, seed)
def test_read_all(               testfile, nelems, use_mmap):     ctest_indexed_gzip.test_read_all(               testfile, nelems, use_mmap)
def test_seek_and_read(          testfile, nelems, niters, seed): ctest_indexed_gzip.test_seek_and_read(          testfile, nelems, niters, seed)
