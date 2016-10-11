#!/usr/bin/env python
#
# test_indexed_gzip.py - Python wrapper around ctest_indexed_gzip.pyx.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#


import pytest
pytestmark = pytest.mark.indexed_gzip_test

from . import ctest_indexed_gzip


def test_open_close(             testfile, nelems):         ctest_indexed_gzip.test_open_close(             testfile, nelems)
def test_open_close_ctxmanager(  testfile, nelems):         ctest_indexed_gzip.test_open_close_ctxmanager(  testfile, nelems)
def test_create_from_open_handle(testfile, nelems):         ctest_indexed_gzip.test_create_from_open_handle(testfile, nelems)
def test_read_all(               testfile, nelems):         ctest_indexed_gzip.test_read_all(               testfile, nelems)
def test_seek_and_read(          testfile, nelems, niters): ctest_indexed_gzip.test_seek_and_read(          testfile, nelems, niters)
